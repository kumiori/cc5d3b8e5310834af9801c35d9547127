from __future__ import annotations

import json
from datetime import date, datetime, time, timezone
from typing import Any, Dict, List, Tuple

import altair as alt
import pandas as pd
import streamlit as st

from infra.app_context import get_authenticator, get_notion_repo
from infra.app_state import ensure_session_context, ensure_session_state, remember_access
from services.notion_value_utils import parse_json_text, parse_number
from services.session_catalog import list_sessions_for_ui
from services.sumup_client import SumUpClient
from ui import apply_theme, set_page, sidebar_auth_controls, sidebar_technical_debug


PAGE_ID = "11_Treasury"
SUCCESS_STATUSES = {"SUCCESSFUL", "PAID", "PAID_OUT"}
PENDING_STATUSES = {"PENDING", "PENDING_PAYMENT", "CHECKOUT_CREATED", "PENDING_CHECKOUT"}


def _extract_history_items(payload: Any) -> List[Dict[str, Any]]:
    """Normalize SumUp history payloads into a flat transaction list."""
    if isinstance(payload, dict):
        if isinstance(payload.get("items"), list):
            return [item for item in payload["items"] if isinstance(item, dict)]
        if isinstance(payload.get("transactions"), list):
            return [item for item in payload["transactions"] if isinstance(item, dict)]
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    return []


def _load_responses_for_session(repo: Any, session_id: str) -> List[Dict[str, Any]]:
    """Load normalized response rows for one session from the responses database."""
    responses_db_id = str(getattr(repo, "responses_db_id", "") or "")
    if not responses_db_id:
        return []
    from infra.notion_repo import _execute_with_retry, _resolve_data_source_id, get_database_schema
    from services.notion_value_utils import (
        find_exact_prop,
        read_number,
        read_relation_first,
        read_rich_text,
        read_title,
    )

    ds_id = _resolve_data_source_id(repo.client, responses_db_id)
    if not ds_id:
        return []
    schema = get_database_schema(repo.client, responses_db_id)
    session_prop = find_exact_prop(schema, ["session"], "relation")
    player_prop = find_exact_prop(schema, ["player"], "relation")
    question_rel_prop = find_exact_prop(schema, ["question", "statement"], "relation")
    question_id_prop = find_exact_prop(schema, ["question_id"], "rich_text")
    item_id_prop = find_exact_prop(schema, ["item_id"], "rich_text")
    value_prop = find_exact_prop(schema, ["value"], "rich_text")
    value_number_prop = find_exact_prop(schema, ["value_number"], "number")
    title_prop = find_exact_prop(schema, ["Name"], "title")

    query: Dict[str, Any] = {
        "data_source_id": ds_id,
        "sorts": [{"timestamp": "created_time", "direction": "descending"}],
        "page_size": 100,
    }
    if session_prop:
        query["filter"] = {"property": session_prop, "relation": {"contains": session_id}}
    out: List[Dict[str, Any]] = []
    while True:
        payload = _execute_with_retry(repo.client.data_sources.query, **query)
        for page in payload.get("results", []):
            props = page.get("properties", {}) if isinstance(page, dict) else {}
            qid = (
                read_relation_first(props, question_rel_prop)
                or read_rich_text(props, question_id_prop)
                or read_rich_text(props, item_id_prop)
            )
            title_text = read_title(props, title_prop)
            player_id = read_relation_first(props, player_prop)
            out.append(
                {
                    "id": str(page.get("id") or ""),
                    "player_id": player_id,
                    "question_id": qid or title_text,
                    "value": parse_json_text(read_rich_text(props, value_prop)),
                    "value_number": read_number(props, value_number_prop)
                    if value_number_prop
                    else None,
                    "created_at": str(page.get("created_time") or ""),
                    "title_key": title_text,
                }
            )
        if not payload.get("has_more"):
            break
        query["start_cursor"] = payload.get("next_cursor")
    return out


def _latest_by_player_question(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Keep the latest response row per player/question pair."""
    latest: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for row in sorted(rows, key=lambda x: str(x.get("created_at") or ""), reverse=True):
        pid = str(row.get("player_id") or "")
        qid = str(row.get("question_id") or "")
        if not pid or not qid:
            continue
        latest.setdefault(pid, {})
        if qid not in latest[pid]:
            latest[pid][qid] = row
    return latest


def _contribution_qids(questions: List[Dict[str, Any]]) -> List[str]:
    """Extract likely contribution question ids from session questions."""
    qids: List[str] = []
    for q in questions:
        text = str(q.get("text") or "").lower()
        if "contribution" in text:
            qid = str(q.get("id") or "")
            if qid:
                qids.append(qid)
    return qids


def _projected_contribution_for_session(repo: Any, session_id: str) -> float:
    """Compute projected total contribution from latest participant responses."""
    questions = repo.list_questions(session_id)
    contribution_ids = _contribution_qids(questions)
    if not contribution_ids:
        contribution_ids = ["contribution", "economic_contribution"]
    responses = _load_responses_for_session(repo, session_id)
    latest = _latest_by_player_question(responses)
    total = 0.0
    for _, qmap in latest.items():
        amount = None
        for qid in contribution_ids:
            row = qmap.get(qid)
            if not row:
                continue
            amount = parse_number(row.get("value_number"))
            if amount is None:
                amount = parse_number(row.get("value"))
            if amount is not None:
                break
        if amount is not None:
            total += float(amount)
    return round(total, 2)


def _extract_transaction_payload(data: Any) -> Dict[str, Any]:
    """Extract the canonical transaction object from SumUp details responses."""
    if isinstance(data, dict):
        if any(key in data for key in ("id", "status", "amount", "timestamp")):
            return data
        if isinstance(data.get("items"), list) and data["items"]:
            first = data["items"][0]
            return first if isinstance(first, dict) else {}
        if isinstance(data.get("transactions"), list) and data["transactions"]:
            first = data["transactions"][0]
            return first if isinstance(first, dict) else {}
    if isinstance(data, list) and data:
        first = data[0]
        return first if isinstance(first, dict) else {}
    return {}


def _parse_metadata(value: Any) -> Dict[str, Any]:
    """Parse SumUp metadata regardless of whether it arrives as dict or JSON string."""
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        parsed = parse_json_text(value)
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _transaction_id(item: Dict[str, Any]) -> str:
    """Return the best transaction identifier available for a SumUp item."""
    return str(item.get("transaction_id") or item.get("id") or "").strip()


def _checkout_id(item: Dict[str, Any]) -> str:
    """Return the best checkout identifier available for a SumUp item."""
    return str(item.get("checkout_id") or item.get("sumup_checkout_id") or "").strip()


def _utc_from_timestamp(value: str) -> pd.Timestamp:
    """Parse a SumUp timestamp to a UTC-aware pandas timestamp."""
    if not value:
        return pd.NaT
    return pd.to_datetime(value, utc=True, errors="coerce")


def _start_utc(selected_date: date) -> pd.Timestamp:
    """Convert a local date selector into a UTC threshold."""
    dt = datetime.combine(selected_date, time.min).astimezone()
    return pd.Timestamp(dt.astimezone(timezone.utc))


def _load_payment_attempts_all_sessions(
    repo: Any, sessions: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Load all locally persisted payment attempts across all sessions."""
    out: List[Dict[str, Any]] = []
    session_labels = {str(item.get("id") or ""): str(item.get("label") or "") for item in sessions}
    for session in sessions:
        session_id = str(session.get("id") or "")
        if not session_id:
            continue
        rows = repo.list_decisions(session_id, decision_type=None)
        for row in rows:
            payload_raw = str(row.get("payload") or "")
            try:
                payload = json.loads(payload_raw) if payload_raw else {}
            except Exception:
                payload = {}
            if str(payload.get("record_type") or "") != "payment_attempt":
                continue
            out.append(
                {
                    "id": str(row.get("id") or ""),
                    "created_at": str(row.get("created_at") or ""),
                    "session_id": session_id,
                    "session_label": session_labels.get(session_id, session_id[:8]),
                    "payload": payload,
                }
            )
    out.sort(key=lambda item: str(item.get("created_at") or ""), reverse=True)
    return out


def _build_payment_attempt_indexes(
    attempts: List[Dict[str, Any]]
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """Index local payment attempts by transaction id and checkout id."""
    by_tx: Dict[str, Dict[str, Any]] = {}
    by_checkout: Dict[str, Dict[str, Any]] = {}
    for attempt in attempts:
        payload = attempt.get("payload", {}) or {}
        tx_id = str(payload.get("sumup_transaction_id") or "").strip()
        checkout_id = str(payload.get("sumup_checkout_id") or "").strip()
        if tx_id and tx_id not in by_tx:
            by_tx[tx_id] = attempt
        if checkout_id and checkout_id not in by_checkout:
            by_checkout[checkout_id] = attempt
    return by_tx, by_checkout


def _is_affranchis_transaction(
    item: Dict[str, Any], metadata: Dict[str, Any], local_attempt: Dict[str, Any] | None
) -> bool:
    """Identify Affranchis transactions via metadata, local mapping, or naming conventions."""
    if local_attempt:
        return True
    if str(metadata.get("app_tag") or "").lower() == "affranchis":
        return True
    reference = str(
        item.get("checkout_reference")
        or item.get("reference")
        or item.get("client_transaction_id")
        or item.get("transaction_code")
        or ""
    ).lower()
    if reference.startswith("affr-") or reference.startswith("aff-") or "affranchis" in reference:
        return True
    summary = str(item.get("product_summary") or item.get("description") or "").lower()
    return "affranchis" in summary


def _attribute_transaction(
    item: Dict[str, Any],
    metadata: Dict[str, Any],
    local_attempt: Dict[str, Any] | None,
) -> Dict[str, str]:
    """Resolve session/player attribution from SumUp metadata first, then local attempt mapping."""
    if local_attempt:
        payload = local_attempt.get("payload", {}) or {}
        return {
            "session_id": str(payload.get("session_id") or local_attempt.get("session_id") or ""),
            "session_label": str(local_attempt.get("session_label") or ""),
            "player_id": str(payload.get("player_id") or ""),
            "participant_key": str(payload.get("participant_key") or ""),
            "source": "local_payment_attempt",
        }
    return {
        "session_id": str(metadata.get("session_id") or ""),
        "session_label": str(metadata.get("session_title") or metadata.get("session_id") or ""),
        "player_id": str(metadata.get("player_id") or ""),
        "participant_key": str(metadata.get("participant_key") or ""),
        "source": "sumup_metadata" if metadata else "unattributed",
    }


def _enrich_transactions(
    client: SumUpClient,
    items: List[Dict[str, Any]],
    *,
    enrich_details: bool,
    by_tx: Dict[str, Dict[str, Any]],
    by_checkout: Dict[str, Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Filter and enrich Affranchis transactions, returning rows plus debug fetch traces."""
    rows: List[Dict[str, Any]] = []
    detail_traces: List[Dict[str, Any]] = []
    for item in items:
        tx_id = _transaction_id(item)
        details_payload: Dict[str, Any] = {}
        if enrich_details and tx_id:
            result = client.transaction_details(tx_id)
            detail_traces.append(
                {
                    "tx_id": tx_id,
                    "ok": bool(result.get("ok")),
                    "status_code": int(result.get("status_code") or 0),
                    "error": str(result.get("error") or ""),
                }
            )
            if result.get("ok"):
                details_payload = _extract_transaction_payload(result.get("json"))
        merged = dict(item)
        merged.update(details_payload)
        metadata = _parse_metadata(merged.get("metadata"))
        local_attempt = by_tx.get(tx_id)
        if not local_attempt:
            local_attempt = by_checkout.get(_checkout_id(merged))
        if not _is_affranchis_transaction(merged, metadata, local_attempt):
            continue
        attribution = _attribute_transaction(merged, metadata, local_attempt)
        ts = _utc_from_timestamp(str(merged.get("timestamp") or ""))
        amount = parse_number(merged.get("amount"))
        rows.append(
            {
                "transaction_id": tx_id,
                "checkout_id": _checkout_id(merged) or str((local_attempt or {}).get("payload", {}).get("sumup_checkout_id") or ""),
                "timestamp": ts,
                "timestamp_text": str(merged.get("timestamp") or ""),
                "local_time": str(merged.get("local_time") or ""),
                "amount": float(amount) if amount is not None else 0.0,
                "currency": str(merged.get("currency") or "EUR"),
                "status": str(merged.get("status") or ""),
                "product_summary": str(merged.get("product_summary") or merged.get("description") or ""),
                "transaction_code": str(merged.get("transaction_code") or ""),
                "session_id": attribution["session_id"],
                "session_label": attribution["session_label"] or "Session inconnue",
                "player_id": attribution["player_id"],
                "participant_key": attribution["participant_key"],
                "mapping_source": attribution["source"],
                "metadata_app_tag": str(metadata.get("app_tag") or ""),
                "metadata_flow": str(metadata.get("flow") or ""),
                "raw": merged,
            }
        )
    rows.sort(key=lambda row: row["timestamp_text"], reverse=True)
    return rows, detail_traces


def _summarize_by_session(df: pd.DataFrame) -> pd.DataFrame:
    """Build per-session treasury metrics."""
    if df.empty:
        return pd.DataFrame()
    tmp = df.copy()
    tmp["is_success"] = tmp["status"].str.upper().isin(SUCCESS_STATUSES)
    tmp["is_pending"] = tmp["status"].str.upper().isin(PENDING_STATUSES)
    grouped = (
        tmp.groupby(["session_label", "session_id"], dropna=False)
        .agg(
            transactions=("transaction_id", "count"),
            received_eur=("amount", lambda s: round(float(s[tmp.loc[s.index, "is_success"]].sum()), 2)),
            pending_eur=("amount", lambda s: round(float(s[tmp.loc[s.index, "is_pending"]].sum()), 2)),
            latest_tx=("timestamp_text", "max"),
        )
        .reset_index()
    )
    return grouped.sort_values(["received_eur", "transactions"], ascending=[False, False])


def _timeline_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Build event-level timeline rows for successful transactions."""
    if df.empty:
        return pd.DataFrame()
    tmp = df.copy()
    tmp = tmp[tmp["status"].str.upper().isin(SUCCESS_STATUSES)]
    if tmp.empty:
        return pd.DataFrame()
    tmp = tmp.dropna(subset=["timestamp"]).sort_values("timestamp")
    return tmp[["timestamp", "amount", "session_label", "transaction_id"]].copy()


def _projected_total_for_sessions(repo: Any, session_ids: List[str]) -> float:
    """Sum projected contributions across the attributed sessions in the treasury view."""
    total = 0.0
    for session_id in sorted({sid for sid in session_ids if sid}):
        total += float(_projected_contribution_for_session(repo, session_id) or 0.0)
    return round(total, 2)


def main() -> None:
    """Render the treasury admin page for Affranchis-linked SumUp funds."""
    set_page()
    apply_theme()
    ensure_session_state()
    repo = get_notion_repo()
    authenticator = get_authenticator(repo)
    logged_in = sidebar_auth_controls(
        authenticator,
        callback=remember_access,
        key_prefix="treasury",
    )
    if not logged_in:
        st.info("Connecte-toi quand tu es prêt·e, puis reviens ici.")
        st.stop()
    ensure_session_context(repo)

    st.title("Trésorerie")
    st.caption(
        "Poll SumUp transactions, isolate Affranchis flows, and reconcile them to local session mappings."
    )

    client = SumUpClient.from_secrets()
    sessions = list_sessions_for_ui(repo, limit=300)
    local_attempts = _load_payment_attempts_all_sessions(repo, sessions)
    attempts_by_tx, attempts_by_checkout = _build_payment_attempt_indexes(local_attempts)

    controls_left, controls_mid, controls_right = st.columns(3)
    with controls_left:
        start_date = st.date_input(
            "Start date",
            value=date.today().replace(day=1),
            key="treasury-start-date",
        )
    with controls_mid:
        history_limit = st.number_input(
            "History limit",
            min_value=1,
            max_value=500,
            value=200,
            step=10,
            key="treasury-history-limit",
        )
    with controls_right:
        enrich_details = st.toggle(
            "Fetch details per transaction",
            value=True,
            help="Needed to recover metadata when the history endpoint is incomplete.",
            key="treasury-enrich-details",
        )

    status_filter = st.multiselect(
        "Statuses",
        options=["SUCCESSFUL", "FAILED", "CANCELLED", "PENDING"],
        default=["SUCCESSFUL", "PENDING"],
        key="treasury-status-filter",
    )

    if st.button("Poll SumUp treasury", type="primary", use_container_width=True):
        with st.spinner("Fetching SumUp transaction history..."):
            result = client.transaction_history(
                limit=int(history_limit),
                statuses=status_filter or None,
                tx_types=["PAYMENT"],
            )
        if result.get("ok"):
            items = _extract_history_items(result.get("json"))
            threshold = _start_utc(start_date)
            filtered = [
                item
                for item in items
                if pd.notna(ts := _utc_from_timestamp(str(item.get("timestamp") or "")))
                and ts >= threshold
            ]
            with st.spinner("Reconciling Affranchis transactions..."):
                rows, detail_traces = _enrich_transactions(
                    client,
                    filtered,
                    enrich_details=bool(enrich_details),
                    by_tx=attempts_by_tx,
                    by_checkout=attempts_by_checkout,
                )
            st.session_state["_treasury_rows"] = rows
            st.session_state["_treasury_detail_traces"] = detail_traces
            st.session_state["_treasury_history_meta"] = {
                "raw_count": len(items),
                "date_filtered_count": len(filtered),
                "start_date": str(start_date),
                "limit": int(history_limit),
                "status_filter": list(status_filter),
            }
            st.success("Treasury history updated.")
        else:
            st.error(f"SumUp history poll failed: {result.get('error')}")

    rows = list(st.session_state.get("_treasury_rows", []) or [])
    meta = dict(st.session_state.get("_treasury_history_meta", {}) or {})
    detail_traces = list(st.session_state.get("_treasury_detail_traces", []) or [])

    df = pd.DataFrame(rows)
    successful_total = 0.0
    pending_total = 0.0
    sessions_count = 0
    projected_total = 0.0
    if not df.empty:
        successful_total = round(
            float(df.loc[df["status"].str.upper().isin(SUCCESS_STATUSES), "amount"].sum()),
            2,
        )
        pending_total = round(
            float(df.loc[df["status"].str.upper().isin(PENDING_STATUSES), "amount"].sum()),
            2,
        )
        sessions_count = int(df["session_label"].nunique())
        projected_total = _projected_total_for_sessions(
            repo, df["session_id"].dropna().astype(str).tolist()
        )

    sidebar_technical_debug(
        page_label=PAGE_ID,
        repo=repo,
        extra={
            "sumup_configured": client.is_configured(),
            "local_payment_attempts": len(local_attempts),
            "transactions_polled": meta.get("raw_count", 0),
            "transactions_after_date_filter": meta.get("date_filtered_count", 0),
            "affranchis_transactions": len(rows),
            "successful_total_eur": successful_total,
            "pending_total_eur": pending_total,
            "sessions_count": sessions_count,
            "detail_fetches": len(detail_traces),
        },
    )
    with st.sidebar:
        with st.expander("Debug · Current treasury selection", expanded=True):
            st.json(
                {
                    "start_date": str(start_date),
                    "history_limit": int(history_limit),
                    "status_filter": list(status_filter),
                    "enrich_details": bool(enrich_details),
                    "sumup_configured": client.is_configured(),
                    "local_attempt_index_sizes": {
                        "by_transaction_id": len(attempts_by_tx),
                        "by_checkout_id": len(attempts_by_checkout),
                    },
                    "mapping_model": {
                        "primary": "SumUp metadata.session_id / session_title",
                        "fallback": "local payment_attempt by transaction_id or checkout_id",
                        "affranchis_markers": [
                            "metadata.app_tag == affranchis",
                            "checkout/reference starts with affr-",
                            "summary contains affranchis",
                        ],
                    },
                },
                expanded=False,
            )
        with st.expander("Debug · Detail fetch traces", expanded=False):
            if detail_traces:
                st.dataframe(pd.DataFrame(detail_traces), use_container_width=True, hide_index=True)
            else:
                st.caption("No per-transaction detail fetches yet.")

    if not client.is_configured():
        st.error("SumUp is not configured in secrets.")
        st.stop()

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Affranchis transactions", int(len(rows)))
    m2.metric("Received funds (EUR)", f"{successful_total:.2f}")
    m3.metric("Pending funds (EUR)", f"{pending_total:.2f}")
    m4.metric("Sessions covered", sessions_count)
    if projected_total > 0:
        progress_ratio = min(successful_total / projected_total, 1.0)
        st.markdown("#### Progress collected / proposed")
        st.progress(progress_ratio)
        st.caption(
            f"{successful_total:.2f} EUR collected out of {projected_total:.2f} EUR proposed "
            f"({progress_ratio * 100:.1f}%)."
        )

    if not rows:
        st.info("No Affranchis treasury data loaded yet. Poll SumUp history first.")
        return

    session_summary = _summarize_by_session(df)
    if not session_summary.empty:
        st.markdown("#### Funds by session")
        st.dataframe(session_summary, use_container_width=True, hide_index=True)

    timeline = _timeline_frame(df)
    st.markdown("#### Contribution timeline by session")
    if timeline.empty:
        st.info("No successful session-attributed transactions available for the timeline yet.")
    else:
        chart = (
            alt.Chart(timeline)
            .mark_circle(size=90)
            .encode(
                x=alt.X("timestamp:T", title="Timestamp"),
                y=alt.Y("amount:Q", title="Contribution (EUR)"),
                color=alt.Color("session_label:N", title="Session"),
                tooltip=[
                    alt.Tooltip("timestamp:T", title="Timestamp"),
                    alt.Tooltip("session_label:N", title="Session"),
                    alt.Tooltip("amount:Q", title="Amount", format=".2f"),
                    alt.Tooltip("transaction_id:N", title="Transaction"),
                ],
            )
            .interactive()
        )
        st.altair_chart(chart, use_container_width=True)

    st.markdown("#### Reconciled transaction ledger")
    table_df = df[
        [
            "timestamp_text",
            "amount",
            "currency",
            "status",
            "session_label",
            "mapping_source",
            "transaction_id",
            "checkout_id",
            "product_summary",
        ]
    ].copy()
    st.dataframe(table_df, use_container_width=True, hide_index=True)

    with st.expander("Raw reconciled rows", expanded=False):
        st.json(rows, expanded=False)


if __name__ == "__main__":
    main()
