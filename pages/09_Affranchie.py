from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import streamlit as st
import streamlit.components.v1 as components

from config import settings
from infra.app_context import get_authenticator, get_notion_repo
from infra.app_state import (
    ensure_session_context,
    ensure_session_state,
    remember_access,
)
from infra.notion_repo import (
    _execute_with_retry,
    _resolve_data_source_id,
    get_database_schema,
)
from services.audio_storage import get_audio_storage_adapter
from services.notion_value_utils import (
    as_list_labels,
    find_exact_prop,
    parse_json_text,
    parse_number,
    read_multiselect_names,
    read_number,
    read_relation_first,
    read_rich_text,
    read_title,
)
from services.sumup_client import SumUpClient
from ui import apply_theme, set_page, sidebar_technical_debug


STEPS = ["participation", "feedback", "contribution"]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _format_eur(amount: Any) -> str:
    """Format EUR values with visible decimals for payment-critical UI."""
    parsed = parse_number(amount)
    if parsed is None:
        return "0.00"
    return f"{float(parsed):.2f}"


def _relation_first_in(props: Dict[str, Any], names: List[str]) -> str:
    for name in names:
        rel_id = read_relation_first(props, name)
        if rel_id:
            return rel_id
    return ""


def _question_ids(questions: List[Dict[str, Any]], key: str) -> List[str]:
    aliases = {
        "participation": ["participation", "participer", "présence", "presence"],
        "contribution": ["contribution", "economic_contribution"],
    }
    terms = aliases.get(key, [key])
    out: List[str] = []
    for q in questions:
        text = str(q.get("text") or "").lower()
        if any(term in text for term in terms):
            qid = str(q.get("id") or "")
            if qid:
                out.append(qid)
    return out


def _load_player_page(repo: Any, player_page_id: str) -> Dict[str, Any]:
    players_db_id = str(getattr(repo, "players_db_id", "") or "")
    schema = get_database_schema(repo.client, players_db_id) if players_db_id else {}
    page = _execute_with_retry(repo.client.pages.retrieve, page_id=player_page_id)
    props = page.get("properties", {}) if isinstance(page, dict) else {}
    notes_prop = find_exact_prop(schema, ["notes_public"], "rich_text")
    nickname_prop = find_exact_prop(schema, ["nickname"], "rich_text")
    title_prop = find_exact_prop(schema, ["Name"], "title")
    access_prop = find_exact_prop(schema, ["access_key", "player_id"], "rich_text")
    session_prop = find_exact_prop(schema, ["session"], "relation")
    diet_prop = find_exact_prop(schema, ["diet"], "multi_select")
    allergens_prop = find_exact_prop(schema, ["allergens"], "multi_select")
    hard_no_prop = find_exact_prop(schema, ["hard_no"], "multi_select")

    session_ids: List[str] = []
    if session_prop:
        rel = props.get(session_prop)
        if isinstance(rel, dict) and rel.get("type") == "relation":
            session_ids = [
                str(item.get("id"))
                for item in rel.get("relation", [])
                if isinstance(item, dict) and item.get("id")
            ]

    return {
        "id": player_page_id,
        "nickname": read_rich_text(props, nickname_prop)
        or read_title(props, title_prop)
        or player_page_id[:8],
        "access_key": read_rich_text(props, access_prop),
        "comment_text": read_rich_text(props, notes_prop),
        "session_ids": session_ids,
        "diet": read_multiselect_names(props, diet_prop),
        "allergens": read_multiselect_names(props, allergens_prop),
        "hard_no": read_multiselect_names(props, hard_no_prop),
    }


def _load_player_responses(
    repo: Any, session_id: str, player_page_id: str
) -> List[Dict[str, Any]]:
    responses_db_id = str(getattr(repo, "responses_db_id", "") or "")
    if not responses_db_id:
        return []
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

    filters: List[Dict[str, Any]] = []
    if session_prop:
        filters.append({"property": session_prop, "relation": {"contains": session_id}})
    if player_prop:
        filters.append(
            {"property": player_prop, "relation": {"contains": player_page_id}}
        )

    query: Dict[str, Any] = {
        "data_source_id": ds_id,
        "sorts": [{"timestamp": "created_time", "direction": "descending"}],
        "page_size": 100,
    }
    if len(filters) == 1:
        query["filter"] = filters[0]
    elif len(filters) > 1:
        query["filter"] = {"and": filters}

    out: List[Dict[str, Any]] = []
    while True:
        payload = _execute_with_retry(repo.client.data_sources.query, **query)
        for page in payload.get("results", []):
            props = page.get("properties", {}) if isinstance(page, dict) else {}
            title_key = read_title(props, title_prop)
            qid = (
                _relation_first_in(props, [question_rel_prop])
                or read_rich_text(props, question_id_prop)
                or read_rich_text(props, item_id_prop)
            )
            if not qid and title_key:
                mq = re.search(r"Q:([0-9a-fA-F-]{16,40})", title_key)
                if mq:
                    qid = mq.group(1)
            pid = _relation_first_in(props, [player_prop])
            if not pid and title_key:
                mp = re.search(r"P:([0-9a-fA-F-]{16,40})", title_key)
                if mp:
                    pid = mp.group(1)
            if pid and pid != player_page_id:
                continue
            out.append(
                {
                    "id": str(page.get("id") or ""),
                    "question_id": str(qid or ""),
                    "player_id": pid,
                    "value": parse_json_text(read_rich_text(props, value_prop)),
                    "value_number": read_number(props, value_number_prop),
                    "created_at": str(page.get("created_time") or ""),
                }
            )
        if not payload.get("has_more"):
            break
        query["start_cursor"] = payload.get("next_cursor")
    return out


def _latest_by_qid(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in sorted(rows, key=lambda x: str(x.get("created_at") or ""), reverse=True):
        qid = str(row.get("question_id") or "")
        if qid and qid not in out:
            out[qid] = row
    return out


def _resolve_state(repo: Any, session_id: str, player_page_id: str) -> Dict[str, Any]:
    player = _load_player_page(repo, player_page_id)
    questions = repo.list_questions(session_id)
    responses = _load_player_responses(repo, session_id, player_page_id)
    latest = _latest_by_qid(responses)

    participation_qids = _question_ids(questions, "participation")
    contribution_qids = _question_ids(questions, "contribution")

    contribution = None
    for qid in contribution_qids:
        row = latest.get(qid)
        if not row:
            continue
        contribution = parse_number(row.get("value_number"))
        if contribution is None:
            contribution = parse_number(row.get("value"))
        if contribution is not None:
            break

    participation = session_id in set(player.get("session_ids", []))
    for qid in participation_qids:
        row = latest.get(qid)
        if not row:
            continue
        val = str(row.get("value") or "").strip().lower()
        if val in {"oui", "yes", "1", "true"}:
            participation = True
            break
        if val in {"non", "no", "0", "false"}:
            participation = False
            break

    return {
        "player": player,
        "questions": questions,
        "participation_qids": participation_qids,
        "contribution_qids": contribution_qids,
        "resolved": {
            "participation": participation,
            "feedback_text": str(player.get("comment_text") or ""),
            "contribution_eur": contribution if contribution is not None else 0.0,
            "diet": ", ".join(player.get("diet", []) or []) or "—",
            "allergens": ", ".join(player.get("allergens", []) or []) or "—",
            "hard_no": ", ".join(player.get("hard_no", []) or []) or "—",
        },
    }


def _upsert_response(
    repo: Any, session_id: str, player_id: str, question_id: str, value: Any
) -> None:
    responses_db_id = str(getattr(repo, "responses_db_id", "") or "")
    if not responses_db_id:
        raise RuntimeError("Responses database id missing.")
    schema = get_database_schema(repo.client, responses_db_id)
    ds_id = _resolve_data_source_id(repo.client, responses_db_id)
    if not ds_id:
        raise RuntimeError("Responses data source id missing.")

    title_prop = find_exact_prop(schema, ["Name"], "title")
    session_prop = find_exact_prop(schema, ["session"], "relation")
    player_prop = find_exact_prop(schema, ["player"], "relation")
    question_rel_prop = find_exact_prop(schema, ["question", "statement"], "relation")
    question_id_prop = find_exact_prop(schema, ["question_id"], "rich_text")
    value_prop = find_exact_prop(schema, ["value"], "rich_text")
    value_number_prop = find_exact_prop(schema, ["value_number"], "number")
    created_prop = find_exact_prop(schema, ["created_at"], "date")
    logical_key = f"Q:{question_id} P:{player_id}"

    filters: List[Dict[str, Any]] = []
    if title_prop:
        filters.append({"property": title_prop, "title": {"equals": logical_key}})
    if session_prop:
        filters.append({"property": session_prop, "relation": {"contains": session_id}})
    if player_prop:
        filters.append({"property": player_prop, "relation": {"contains": player_id}})
    if question_rel_prop:
        filters.append(
            {"property": question_rel_prop, "relation": {"contains": question_id}}
        )
    elif question_id_prop:
        filters.append(
            {"property": question_id_prop, "rich_text": {"equals": question_id}}
        )

    query: Dict[str, Any] = {"data_source_id": ds_id, "page_size": 1}
    if len(filters) == 1:
        query["filter"] = filters[0]
    elif len(filters) > 1:
        query["filter"] = {"and": filters}
    payload = _execute_with_retry(repo.client.data_sources.query, **query)
    existing = payload.get("results", [])
    target_page_id = existing[0].get("id") if existing else None

    props: Dict[str, Any] = {}
    if title_prop:
        props[title_prop] = {
            "title": [{"type": "text", "text": {"content": logical_key}}]
        }
    if session_prop:
        props[session_prop] = {"relation": [{"id": session_id}]}
    if player_prop:
        props[player_prop] = {"relation": [{"id": player_id}]}
    if question_rel_prop:
        props[question_rel_prop] = {"relation": [{"id": question_id}]}
    if question_id_prop:
        props[question_id_prop] = {
            "rich_text": [{"type": "text", "text": {"content": question_id}}]
        }
    if value_prop:
        props[value_prop] = {
            "rich_text": [
                {
                    "type": "text",
                    "text": {"content": json.dumps(value, ensure_ascii=False)},
                }
            ]
        }
    if value_number_prop and isinstance(value, (int, float)):
        props[value_number_prop] = {"number": float(value)}
    if created_prop:
        props[created_prop] = {"date": {"start": _now_iso()}}

    if target_page_id:
        _execute_with_retry(
            repo.client.pages.update, page_id=target_page_id, properties=props
        )
    else:
        _execute_with_retry(
            repo.client.pages.create,
            parent={"database_id": responses_db_id},
            properties=props,
        )


def _save_comment(repo: Any, player_id: str, comment: str) -> None:
    players_db_id = str(getattr(repo, "players_db_id", "") or "")
    if not players_db_id:
        raise RuntimeError("Players database id missing.")
    schema = get_database_schema(repo.client, players_db_id)
    notes_prop = find_exact_prop(schema, ["notes_public"], "rich_text")
    if not notes_prop:
        raise RuntimeError("Players schema has no notes_public rich_text field.")
    _execute_with_retry(
        repo.client.pages.update,
        page_id=player_id,
        properties={
            notes_prop: {
                "rich_text": [{"type": "text", "text": {"content": comment.strip()}}]
            }
        },
    )


def _persist_aftercare_inputs(
    repo: Any,
    *,
    session_id: str,
    player_id: str,
    participation_qid: str,
    resolved_feedback_text: str,
) -> None:
    """Persist the current participation choice and in-progress feedback before payment."""
    participation_choice = str(
        st.session_state.get("affranchie-participation-choice") or ""
    ).strip()
    if participation_qid and participation_choice in {"Oui", "Non"}:
        _upsert_response(
            repo,
            session_id,
            player_id,
            participation_qid,
            "oui" if participation_choice == "Oui" else "non",
        )

    feedback_enabled = bool(st.session_state.get("affranchie-feedback-enabled"))
    if not feedback_enabled:
        return
    feedback_text = str(
        st.session_state.get("affranchie-feedback-text") or resolved_feedback_text or ""
    )
    _save_comment(repo, player_id, feedback_text)


def _save_audio_note(
    repo: Any,
    session_id: str,
    player_id: str,
    file_name: str,
    mime_type: str,
    size_bytes: int,
    *,
    storage_provider: str,
    storage_path: str,
    public_url: str,
    created_at: str = "",
) -> None:
    payload = {
        "record_type": "audio_note",
        "participant_key": st.session_state.get("player_access_key", ""),
        "player_id": player_id,
        "session_id": session_id,
        "audio_note_id": f"audio-{int(datetime.now().timestamp())}",
        "audio_url": public_url,
        "storage_provider": storage_provider,
        "storage_path": storage_path,
        "public_url": public_url,
        "mime_type": mime_type,
        "size_bytes": size_bytes,
        "file_name": file_name,
        "created_at": created_at or _now_iso(),
    }
    repo.create_decision(
        session_id=session_id,
        player_id=player_id,
        decision_type=_decision_type_for_storage(repo),
        payload=json.dumps(payload, ensure_ascii=False),
    )


def _load_audio_notes(
    repo: Any, session_id: str, player_id: str
) -> List[Dict[str, Any]]:
    rows = repo.list_decisions(session_id, decision_type=None)
    out: List[Dict[str, Any]] = []
    for row in rows:
        pids = row.get("player_id") or []
        if player_id not in pids:
            continue
        payload_raw = str(row.get("payload") or "")
        try:
            payload = json.loads(payload_raw) if payload_raw else {}
        except Exception:
            payload = {}
        if str(payload.get("record_type") or "") != "audio_note":
            continue
        out.append(
            {
                "id": row.get("id"),
                "created_at": row.get("created_at"),
                "payload": payload,
            }
        )
    out.sort(key=lambda x: str(x.get("created_at") or ""), reverse=True)
    return out


def _persist_payment_attempt(
    repo: Any,
    *,
    session_id: str,
    player_id: str,
    amount_eur: float,
    currency: str,
    checkout_id: str,
    status: str,
    payment_url: str = "",
    transaction_id: str = "",
    payload: Optional[Dict[str, Any]] = None,
) -> None:
    body = {
        "record_type": "payment_attempt",
        "participant_key": st.session_state.get("player_access_key", ""),
        "player_id": player_id,
        "session_id": session_id,
        "contribution_amount_eur": float(amount_eur),
        "currency": currency,
        "sumup_checkout_id": checkout_id,
        "sumup_transaction_id": transaction_id,
        "sumup_status": status,
        "payment_link_url": payment_url,
        "created_at": _now_iso(),
        "updated_at": _now_iso(),
        "sumup_response_payload": payload or {},
    }
    repo.create_decision(
        session_id=session_id,
        player_id=player_id,
        decision_type=_decision_type_for_storage(repo),
        payload=json.dumps(body, ensure_ascii=False),
    )


def _load_payment_attempts(
    repo: Any, session_id: str, player_id: str
) -> List[Dict[str, Any]]:
    rows = repo.list_decisions(session_id, decision_type=None)
    out: List[Dict[str, Any]] = []
    for row in rows:
        pids = row.get("player_id") or []
        if player_id not in pids:
            continue
        payload_raw = str(row.get("payload") or "")
        try:
            payload = json.loads(payload_raw) if payload_raw else {}
        except Exception:
            payload = {}
        if str(payload.get("record_type") or "") != "payment_attempt":
            continue
        out.append(
            {
                "id": row.get("id"),
                "created_at": row.get("created_at"),
                "payload": payload,
            }
        )
    out.sort(key=lambda x: str(x.get("created_at") or ""), reverse=True)
    return out


def _status_fr(raw_status: str) -> str:
    mapping = {
        "not_started": "Non démarré",
        "checkout_created": "Lien généré",
        "pending": "Paiement en attente",
        "paid": "Paiement confirmé",
        "successful": "Paiement confirmé",
        "paid_out": "Paiement confirmé",
        "failed": "Paiement échoué",
        "expired": "Expiré",
        "cancelled": "Annulé",
    }
    key = str(raw_status or "").strip().lower()
    return mapping.get(key, key or "Non démarré")


def _normalized_payment_status(raw_status: str) -> str:
    """Normalize SumUp statuses into the local participant payment state model."""
    key = str(raw_status or "").strip().lower()
    if key in {"successful", "paid", "paid_out"}:
        return "paid"
    if key in {"pending"}:
        return "pending"
    if key in {"failed"}:
        return "failed"
    if key in {"cancelled"}:
        return "cancelled"
    if key in {"expired"}:
        return "expired"
    if key in {"checkout_created"}:
        return "checkout_created"
    return key or "pending"


def _extract_receipt_svg_url(payload: Dict[str, Any]) -> str:
    """Extract the SVG receipt URL from a SumUp payload when present."""
    links = payload.get("links") or []
    if not isinstance(links, list):
        return ""
    for link in links:
        if not isinstance(link, dict):
            continue
        if (
            str(link.get("rel") or "") == "receipt"
            and str(link.get("type") or "") == "image/svg+xml"
        ):
            return str(link.get("href") or "")
    return ""


def _extract_payment_link(payload: Dict[str, Any]) -> str:
    """Extract the hosted payment URL from a checkout payload using tolerant matching."""
    if not isinstance(payload, dict):
        return ""
    direct_candidates = [
        str(payload.get("payment_link_url") or ""),
        str(payload.get("checkout_url") or ""),
        str(payload.get("hosted_checkout_url") or ""),
        str(payload.get("hosted_url") or ""),
        str(payload.get("url") or ""),
        str(payload.get("href") or ""),
    ]
    for candidate in direct_candidates:
        if candidate.startswith("http://") or candidate.startswith("https://"):
            return candidate
    links = payload.get("links") or []
    if not isinstance(links, list):
        return ""
    preferred_rels = {"hosted_checkout", "checkout", "payment", "pay"}
    fallback = ""
    for link in links:
        if not isinstance(link, dict):
            continue
        href = str(link.get("href") or "")
        rel = str(link.get("rel") or "").strip().lower()
        if not href.startswith("http://") and not href.startswith("https://"):
            continue
        if rel in preferred_rels:
            return href
        if rel not in {"receipt", "refund", "self"} and not fallback:
            fallback = href
    return fallback


def _extract_transaction_payload(data: Any) -> Dict[str, Any]:
    """Normalize SumUp transaction-details responses to a single payload dict."""
    if isinstance(data, dict):
        if data.get("id") and data.get("amount") is not None:
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


def _audio_path_from_payload(payload: Dict[str, Any]) -> str:
    return str(
        payload.get("storage_path")
        or payload.get("public_url")
        or payload.get("audio_url")
        or ""
    )


def _append_payment_trace(event: str, **payload: Any) -> None:
    """Store a compact developer-facing payment trace in session state."""
    traces = list(st.session_state.get("affranchie_payment_trace", []) or [])
    traces.append(
        {
            "at_utc": _now_iso(),
            "event": event,
            "payload": payload,
        }
    )
    st.session_state["affranchie_payment_trace"] = traces[-12:]


def _set_contribution_amount(amount: float) -> None:
    """Update the working contribution amount and reset local confirmation state."""
    st.session_state["affranchie-contribution-amount"] = float(amount)
    st.session_state["affranchie_contribution_confirmed"] = False
    st.session_state["affranchie_contribution_confirmed_amount"] = None
    st.session_state["affranchie_contribution_confirmed_mode"] = None


def _render_payment_attempts(attempts: List[Dict[str, Any]]) -> None:
    """Render a compact list of persisted payment attempts for the current participant."""
    if not attempts:
        st.caption("Aucune tentative de contribution enregistrée pour l’instant.")
        return
    rows: List[Dict[str, Any]] = []
    for attempt in attempts:
        payload = attempt.get("payload", {}) or {}
        rows.append(
            {
                "créé le": str(attempt.get("created_at") or "")[:19].replace("T", " "),
                "montant": payload.get("contribution_amount_eur"),
                "devise": payload.get("currency") or "EUR",
                "statut": _status_fr(str(payload.get("sumup_status") or "")),
                "checkout_id": str(payload.get("sumup_checkout_id") or ""),
                "transaction_id": str(payload.get("sumup_transaction_id") or ""),
            }
        )
    st.dataframe(rows, use_container_width=True, hide_index=True)


def _render_payment_attempts_sidebar(attempts: List[Dict[str, Any]]) -> None:
    """Render developer-facing payment attempts with actionable links in the sidebar."""
    st.caption("State legend")
    st.markdown(
        "- `Lien généré`: a checkout exists locally and a payment link was created.\n"
        "- `Paiement en attente`: SumUp has a live payment object, but no confirmed success yet.\n"
        "- `Paiement confirmé`: SumUp returned a successful terminal state."
    )
    if not attempts:
        st.caption("No recorded attempts yet.")
        return
    for idx, attempt in enumerate(attempts):
        payload = attempt.get("payload", {}) or {}
        created_at = str(attempt.get("created_at") or "")[:19].replace("T", " ")
        amount = payload.get("contribution_amount_eur")
        status = _status_fr(str(payload.get("sumup_status") or ""))
        checkout_id = str(payload.get("sumup_checkout_id") or "")
        transaction_id = str(payload.get("sumup_transaction_id") or "")
        payment_link = _extract_payment_link(payload)
        receipt_svg_url = _extract_receipt_svg_url(payload)
        with st.container():
            st.markdown(f"**{amount} EUR · {status}**")
            st.caption(created_at or "—")
            if checkout_id:
                st.code(checkout_id, language=None)
            if transaction_id:
                st.caption(f"tx: {transaction_id}")
            if payment_link:
                st.link_button(
                    "Open payment",
                    payment_link,
                    key=f"sidebar-payment-link-{idx}",
                    use_container_width=True,
                )
            if receipt_svg_url:
                st.link_button(
                    "Open SVG receipt",
                    receipt_svg_url,
                    key=f"sidebar-receipt-link-{idx}",
                    use_container_width=True,
                )


def _render_current_transaction_sidebar(
    attempt: Dict[str, Any],
    *,
    player: Dict[str, Any],
    session_id: str,
    session_label: str,
) -> None:
    """Render the current payment attempt with explicit participant/session mapping details."""
    if not attempt:
        st.caption("No current transaction selected.")
        return
    payload = attempt.get("payload", {}) or {}
    created_at = str(attempt.get("created_at") or "")[:19].replace("T", " ")
    checkout_id = str(payload.get("sumup_checkout_id") or "")
    transaction_id = str(payload.get("sumup_transaction_id") or "")
    amount = _format_eur(payload.get("contribution_amount_eur"))
    currency = str(payload.get("currency") or "EUR")
    status = _status_fr(str(payload.get("sumup_status") or ""))
    payment_link = _extract_payment_link(payload)
    receipt_svg_url = _extract_receipt_svg_url(payload)
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
    checkout_reference = str(
        payload.get("checkout_reference")
        or payload.get("reference")
        or payload.get("client_transaction_id")
        or ""
    )
    participant_key = str(
        metadata.get("participant_key") or st.session_state.get("player_access_key") or ""
    )
    player_id = str(metadata.get("player_id") or player.get("id") or "")
    session_meta_id = str(metadata.get("session_id") or session_id or "")
    session_meta_title = str(metadata.get("session_title") or session_label or "")

    st.markdown("**Current transaction**")
    st.markdown(f"**{amount} {currency} · {status}**")
    if created_at:
        st.caption(created_at)
    if checkout_id:
        st.caption("checkout_id")
        st.code(checkout_id, language=None)
    if transaction_id:
        st.caption("transaction_id")
        st.code(transaction_id, language=None)
    if checkout_reference:
        st.caption("checkout_reference / client_transaction_id")
        st.code(checkout_reference, language=None)
    if payment_link:
        st.link_button(
            "Open payment",
            payment_link,
            key="sidebar-current-payment-link",
            use_container_width=True,
        )
    if receipt_svg_url:
        st.link_button(
            "Open SVG receipt",
            receipt_svg_url,
            key="sidebar-current-receipt-link",
            use_container_width=True,
        )

    st.markdown("**Mapped to participant/session**")
    rows = [
        {"field": "player", "value": str(player.get("nickname") or "—")},
        {"field": "player_id", "value": player_id or "—"},
        {
            "field": "participant_key",
            "value": participant_key or "—",
        },
        {"field": "session", "value": session_meta_title or session_label or "—"},
        {"field": "session_id", "value": session_meta_id or "—"},
        {
            "field": "flow",
            "value": str(metadata.get("flow") or "participant_contribution" or "—"),
        },
        {"field": "app_tag", "value": str(metadata.get("app_tag") or "affranchis")},
    ]
    st.dataframe(rows, use_container_width=True, hide_index=True)
    st.caption(
        "Mapping is encoded in two places: "
        "1) metadata fields passed to SumUp (`participant_key`, `player_id`, `session_id`, `session_title`, `flow`, `app_tag`), "
        "2) the checkout reference pattern `affr-{session8}-{player8}-{timestamp}`."
    )


def _latest_unique_attempts(attempts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Keep only the latest attempt per checkout or transaction identity."""
    seen: set[str] = set()
    unique: List[Dict[str, Any]] = []
    for attempt in attempts:
        payload = attempt.get("payload", {}) or {}
        key = (
            str(payload.get("sumup_checkout_id") or "").strip()
            or str(payload.get("sumup_transaction_id") or "").strip()
            or str(attempt.get("id") or "").strip()
        )
        if not key or key in seen:
            continue
        seen.add(key)
        unique.append(attempt)
    return unique


def _invalidate_payment_link(
    repo: Any,
    *,
    session_id: str,
    player_id: str,
    attempt_payload: Dict[str, Any],
) -> None:
    """Persist a superseding local record that removes the active checkout link."""
    payload = dict(attempt_payload or {})
    payload["sumup_status"] = "cancelled"
    payload["payment_link_url"] = ""
    payload["updated_at"] = _now_iso()
    _persist_payment_attempt(
        repo,
        session_id=session_id,
        player_id=player_id,
        amount_eur=float(parse_number(payload.get("contribution_amount_eur")) or 0.0),
        currency=str(payload.get("currency") or "EUR"),
        checkout_id=str(payload.get("sumup_checkout_id") or ""),
        status="cancelled",
        payment_url="",
        transaction_id=str(payload.get("sumup_transaction_id") or ""),
        payload=payload,
    )


def _refresh_checkout_attempt(
    repo: Any,
    *,
    session_id: str,
    player_id: str,
    attempt_payload: Dict[str, Any],
) -> None:
    """Refresh one checkout from SumUp and persist the updated local attempt."""
    checkout_id = str(attempt_payload.get("sumup_checkout_id") or "").strip()
    if not checkout_id:
        raise ValueError("Checkout id missing.")
    client = SumUpClient.from_secrets()
    result = client.checkout_details(checkout_id)
    if not result.get("ok"):
        raise RuntimeError(str(result.get("error") or "Unable to fetch checkout details."))
    checkout_payload = result.get("json") or {}
    payload_now = dict(attempt_payload or {})
    payload_now.update(checkout_payload)
    status_now = _normalized_payment_status(str(payload_now.get("status") or "pending"))
    transaction_id = str(payload_now.get("transaction_id") or attempt_payload.get("sumup_transaction_id") or "")
    if transaction_id:
        tx_result = client.transaction_details(transaction_id)
        if tx_result.get("ok"):
            tx_payload = _extract_transaction_payload(tx_result.get("json")) or {}
            merged_links = list(checkout_payload.get("links") or [])
            for link in list(tx_payload.get("links") or []):
                if link not in merged_links:
                    merged_links.append(link)
            payload_now.update(tx_payload)
            if merged_links:
                payload_now["links"] = merged_links
            payload_now["sumup_checkout_id"] = checkout_id
            status_now = _normalized_payment_status(
                str(payload_now.get("status") or status_now)
            )
    payment_link = _extract_payment_link(payload_now) or _extract_payment_link(attempt_payload)
    _persist_payment_attempt(
        repo,
        session_id=session_id,
        player_id=player_id,
        amount_eur=float(parse_number(payload_now.get("amount")) or parse_number(attempt_payload.get("contribution_amount_eur")) or 0.0),
        currency=str(payload_now.get("currency") or attempt_payload.get("currency") or "EUR"),
        checkout_id=checkout_id,
        status=status_now,
        payment_url=payment_link,
        transaction_id=transaction_id,
        payload=payload_now,
    )


def _verify_checkout_attempt(
    repo: Any,
    *,
    session_id: str,
    player_id: str,
    amount_eur: float,
    checkout_id: str,
    fallback_payload: Dict[str, Any],
) -> Dict[str, Any]:
    """Verify a checkout against SumUp, persist the latest state, and enrich with transaction data."""
    client = SumUpClient.from_secrets()
    result = client.checkout_details(checkout_id)
    if not result.get("ok"):
        raise RuntimeError(str(result.get("error") or "Unable to verify checkout status."))
    checkout_payload = result.get("json") or {}
    payload_now = dict(fallback_payload or {})
    payload_now.update(checkout_payload)
    transaction_id = str(
        payload_now.get("transaction_id") or fallback_payload.get("sumup_transaction_id") or ""
    )
    if transaction_id:
        tx_result = client.transaction_details(transaction_id)
        if tx_result.get("ok"):
            tx_payload = _extract_transaction_payload(tx_result.get("json")) or {}
            merged_links = list(checkout_payload.get("links") or [])
            for link in list(tx_payload.get("links") or []):
                if link not in merged_links:
                    merged_links.append(link)
            payload_now.update(tx_payload)
            if merged_links:
                payload_now["links"] = merged_links
            payload_now["sumup_checkout_id"] = checkout_id
    status_now = _normalized_payment_status(str(payload_now.get("status") or "pending"))
    payment_link = _extract_payment_link(payload_now) or _extract_payment_link(fallback_payload)
    currency = str(payload_now.get("currency") or fallback_payload.get("currency") or "EUR")
    amount_now = float(
        parse_number(payload_now.get("amount"))
        or parse_number(fallback_payload.get("contribution_amount_eur"))
        or amount_eur
    )
    transaction_id = str(
        payload_now.get("transaction_id") or fallback_payload.get("sumup_transaction_id") or ""
    )
    _persist_payment_attempt(
        repo,
        session_id=session_id,
        player_id=player_id,
        amount_eur=amount_now,
        currency=currency,
        checkout_id=checkout_id,
        status=status_now,
        payment_url=payment_link,
        transaction_id=transaction_id,
        payload=payload_now,
    )
    return {
        "status": status_now,
        "transaction_id": transaction_id,
        "payment_link": payment_link,
        "receipt_svg_url": _extract_receipt_svg_url(payload_now),
        "payload": payload_now,
    }


def _refresh_actionable_attempts(
    repo: Any, *, session_id: str, player_id: str, attempts: List[Dict[str, Any]]
) -> None:
    """Best-effort refresh of locally actionable checkouts so execution controls can appear."""
    refreshed_keys = set(st.session_state.get("affranchie_refreshed_checkouts", []) or [])
    changed = False
    for attempt in _latest_unique_attempts(attempts):
        payload = attempt.get("payload", {}) or {}
        status = str(payload.get("sumup_status") or "").lower()
        checkout_id = str(payload.get("sumup_checkout_id") or "").strip()
        payment_link = _extract_payment_link(payload)
        if not checkout_id or status not in {"checkout_created", "pending"} or payment_link or checkout_id in refreshed_keys:
            continue
        try:
            _refresh_checkout_attempt(
                repo,
                session_id=session_id,
                player_id=player_id,
                attempt_payload=payload,
            )
            refreshed_keys.add(checkout_id)
            changed = True
        except Exception:
            refreshed_keys.add(checkout_id)
    st.session_state["affranchie_refreshed_checkouts"] = list(refreshed_keys)[-50:]
    if changed:
        st.rerun()


@st.dialog("Paiement SumUp")
def _sumup_execute_dialog(checkout_id: str) -> None:
    """Render the embedded SumUp execution dialog for an existing checkout."""
    st.caption(
        "Finalise le paiement dans ce module sécurisé SumUp. "
        "Après le paiement, ferme ce dialogue manuellement: il ne se fermera pas automatiquement."
    )
    js_code = f"""
        <div id="sumup-card" style="min-height: 520px;"></div>
        <script type="text/javascript" src="https://gateway.sumup.com/gateway/ecom/card/v2/sdk.js"></script>
        <script type="text/javascript">
            const mountCard = () => {{
                if (!window.SumUpCard) return;
                window.SumUpCard.mount({{
                    id: "sumup-card",
                    checkoutId: "{checkout_id}",
                    donateSubmitButton: false,
                    showInstallments: false,
                    onResponse: function (type, body) {{
                        console.log("SumUp onResponse type:", type);
                        console.log("SumUp onResponse body:", body);
                    }},
                }});
            }};
            if (document.readyState === "loading") {{
                document.addEventListener("DOMContentLoaded", mountCard);
            }} else {{
                mountCard();
            }}
        </script>
    """
    components.html(js_code, height=620, scrolling=True)


def _render_contribution_history(
    repo: Any, session_id: str, player_id: str, attempts: List[Dict[str, Any]]
) -> None:
    """Render a participant-facing contribution history."""
    if not attempts:
        st.caption("Aucune contribution enregistrée pour le moment.")
        return
    for idx, attempt in enumerate(_latest_unique_attempts(attempts)):
        payload = attempt.get("payload", {}) or {}
        created_at = str(attempt.get("created_at") or "")[:19].replace("T", " ")
        amount = payload.get("contribution_amount_eur")
        raw_status = str(payload.get("sumup_status") or "")
        status = _status_fr(raw_status)
        transaction_id = str(payload.get("sumup_transaction_id") or "")
        checkout_id = str(payload.get("sumup_checkout_id") or "")
        payment_link = _extract_payment_link(payload)
        receipt_svg_url = _extract_receipt_svg_url(payload)
        top_left, top_mid, top_right = st.columns([2, 2, 1])
        with top_left:
            st.markdown(f"**{_format_eur(amount)} EUR**")
            st.caption(created_at or "—")
        with top_mid:
            st.markdown(f"**{status}**")
            if transaction_id:
                st.caption(f"Transaction: {transaction_id}")
        with top_right:
            if receipt_svg_url:
                st.link_button(
                    "Reçu SVG",
                    receipt_svg_url,
                    key=f"history-receipt-{idx}",
                    use_container_width=True,
                )
        if raw_status.lower() in {"checkout_created", "pending"} and checkout_id:
            action_execute, action_verify, action_delete = st.columns(3)
            with action_execute:
                if st.button(
                    "Exécuter",
                    key=f"history-execute-{idx}",
                    use_container_width=True,
                ):
                    _sumup_execute_dialog(checkout_id)
            with action_verify:
                if st.button(
                    "Vérifier",
                    key=f"history-verify-{idx}",
                    use_container_width=True,
                ):
                    try:
                        verify_result = _verify_checkout_attempt(
                            repo,
                            session_id=session_id,
                            player_id=player_id,
                            amount_eur=float(parse_number(amount) or 0.0),
                            checkout_id=checkout_id,
                            fallback_payload=payload,
                        )
                        _append_payment_trace(
                            "history_checkout_verified",
                            checkout_id=checkout_id,
                            transaction_id=str(verify_result.get("transaction_id") or ""),
                            status=str(verify_result.get("status") or ""),
                        )
                        if str(verify_result.get("status") or "") == "paid":
                            st.balloons()
                            st.success("Paiement confirmé.")
                        else:
                            st.success(
                                f"Statut mis à jour: {_status_fr(str(verify_result.get('status') or 'pending'))}"
                            )
                        st.rerun()
                    except Exception as exc:
                        _append_payment_trace(
                            "history_checkout_verify_failed",
                            checkout_id=checkout_id,
                            error=str(exc),
                        )
                        st.error(f"Vérification échouée: {exc}")
            with action_delete:
                if st.button(
                    "Retirer le lien",
                    key=f"history-delete-link-{idx}",
                    use_container_width=True,
                ):
                    try:
                        _invalidate_payment_link(
                            repo,
                            session_id=session_id,
                            player_id=player_id,
                            attempt_payload=payload,
                        )
                        _append_payment_trace(
                            "checkout_link_invalidated",
                            checkout_id=str(payload.get("sumup_checkout_id") or ""),
                            transaction_id=str(payload.get("sumup_transaction_id") or ""),
                        )
                        st.success("Lien retiré de l’historique actif.")
                        st.rerun()
                    except Exception as exc:
                        st.error(f"Impossible de retirer ce lien: {exc}")
        st.divider()


def _latest_attempt_for_amount(
    attempts: List[Dict[str, Any]], amount: float
) -> Dict[str, Any]:
    """Return the latest payment attempt matching the currently confirmed amount."""
    for attempt in attempts:
        payload = attempt.get("payload", {}) or {}
        attempt_amount = parse_number(payload.get("contribution_amount_eur"))
        if attempt_amount is not None and float(attempt_amount) == float(amount):
            return attempt
    return {}


def _attempt_identity(attempt: Dict[str, Any]) -> str:
    """Build a stable identity for a payment attempt row."""
    payload = attempt.get("payload", {}) or {}
    checkout_id = str(payload.get("sumup_checkout_id") or "").strip()
    transaction_id = str(payload.get("sumup_transaction_id") or "").strip()
    created_at = str(attempt.get("created_at") or "").strip()
    if checkout_id:
        return f"checkout:{checkout_id}"
    if transaction_id:
        return f"tx:{transaction_id}"
    return f"created:{created_at}"


def _attempt_exists_for_transaction(
    attempts: List[Dict[str, Any]], transaction_id: str
) -> bool:
    """Return whether a transaction id is already attached locally."""
    target = str(transaction_id or "").strip()
    if not target:
        return False
    for attempt in attempts:
        payload = attempt.get("payload", {}) or {}
        if str(payload.get("sumup_transaction_id") or "").strip() == target:
            return True
    return False


def _create_checkout_and_persist(
    repo: Any,
    *,
    session_id: str,
    player_page_id: str,
    player_nickname: str,
    amount: float,
) -> None:
    """Create a SumUp checkout, persist it locally, and prepare user feedback."""
    client = SumUpClient.from_secrets()
    if not client.is_configured():
        st.error("Configuration SumUp manquante.")
        return
    ref = (
        f"affr-{session_id[:8]}-{player_page_id[:8]}-{int(datetime.now().timestamp())}"
    )
    metadata = {
        "app_tag": "affranchis",
        "flow": "participant_contribution",
        "participant_key": st.session_state.get("player_access_key", ""),
        "player_id": player_page_id,
        "session_id": session_id,
        "session_title": str(st.session_state.get("session_title") or ""),
    }
    result = client.create_checkout(
        amount=float(amount),
        currency="EUR",
        checkout_reference=ref,
        description=f"Contribution participant {player_nickname}",
        metadata=metadata,
        return_url=_build_affranchie_return_url(),
    )
    if result.get("ok"):
        payload = result.get("json") or {}
        checkout_id = str(payload.get("id") or "")
        links = payload.get("links") or []
        payment_link = ""
        if isinstance(links, list):
            for link in links:
                if isinstance(link, dict) and str(link.get("href") or ""):
                    payment_link = str(link.get("href") or "")
                    break
        return_url = _build_affranchie_return_url()
        payload["return_url"] = return_url
        try:
            _persist_payment_attempt(
                repo,
                session_id=session_id,
                player_id=player_page_id,
                amount_eur=float(amount),
                currency="EUR",
                checkout_id=checkout_id,
                status="checkout_created",
                payment_url=payment_link,
                payload=payload,
            )
            _append_payment_trace(
                "checkout_created",
                amount_eur=float(amount),
                checkout_id=checkout_id,
                payment_link=payment_link,
                return_url=return_url,
                status="checkout_created",
            )
            st.session_state["affranchie_return_feedback"] = {
                "kind": "info",
                "message": "Lien de paiement généré. Tu peux maintenant ouvrir le paiement, puis revenir ici pour vérifier le statut.",
            }
            st.rerun()
        except Exception as exc:
            st.error(f"Checkout créé mais persistence échouée: {exc}")
    else:
        _append_payment_trace(
            "checkout_create_failed",
            amount_eur=float(amount),
            error=str(result.get("error") or ""),
        )
        st.error(f"Échec création checkout: {result.get('error')}")


def _public_app_base_url() -> str:
    """Resolve the externally reachable base URL used for SumUp return flows."""
    candidates = [
        str(st.secrets.get("APP_BASE_URL", "") or "").strip(),
        str((st.secrets.get("app", {}) or {}).get("base_url", "")).strip()
        if isinstance(st.secrets.get("app", {}), dict)
        else "",
    ]
    for candidate in candidates:
        if candidate:
            return candidate.rstrip("/")
    return "http://localhost:8503"


def _build_affranchie_return_url() -> str:
    """Build the return URL that brings the user back to the affranchie page."""
    base = _public_app_base_url()
    return f"{base}/affranchie?sumup_return=1"


def _reconcile_returned_checkout(
    repo: Any, session_id: str, player_page_id: str, amount: float
) -> None:
    """Auto-refresh checkout status when SumUp redirects back to this page."""
    qp = st.query_params
    if str(qp.get("sumup_return", "")).strip() != "1":
        return
    attempts = _load_payment_attempts(repo, session_id, player_page_id)
    pending_attempt = next(
        (
            item
            for item in attempts
            if str((item.get("payload", {}) or {}).get("sumup_status") or "").lower()
            in {"checkout_created", "pending"}
        ),
        {},
    )
    pending_payload = (
        (pending_attempt.get("payload", {}) or {}) if pending_attempt else {}
    )
    checkout_id = str(pending_payload.get("sumup_checkout_id") or "").strip()
    if not checkout_id:
        return
    if st.session_state.get("affranchie_last_returned_checkout_id") == checkout_id:
        return

    client = SumUpClient.from_secrets()
    result = client.checkout_details(checkout_id)
    if result.get("ok"):
        payload_now = result.get("json") or {}
        status_now = str(payload_now.get("status") or "pending").lower()
        transaction_id = str(payload_now.get("transaction_id") or "")
        try:
            _persist_payment_attempt(
                repo,
                session_id=session_id,
                player_id=player_page_id,
                amount_eur=float(amount),
                currency=str(payload_now.get("currency") or "EUR"),
                checkout_id=checkout_id,
                status=status_now,
                payment_url="",
                transaction_id=transaction_id,
                payload=payload_now,
            )
            _append_payment_trace(
                "checkout_return_reconciled",
                checkout_id=checkout_id,
                transaction_id=transaction_id,
                status=status_now,
            )
            st.session_state["affranchie_return_feedback"] = {
                "kind": "success" if status_now == "paid" else "info",
                "message": "Paiement confirmé."
                if status_now == "paid"
                else f"Retour reçu. Statut actuel: {_status_fr(status_now)}.",
            }
        except Exception as exc:
            _append_payment_trace(
                "checkout_return_persist_failed",
                checkout_id=checkout_id,
                error=str(exc),
            )
            st.session_state["affranchie_return_feedback"] = {
                "kind": "error",
                "message": f"Retour reçu, mais la mise à jour locale a échoué: {exc}",
            }
    else:
        _append_payment_trace(
            "checkout_return_failed",
            checkout_id=checkout_id,
            error=str(result.get("error") or ""),
        )
        st.session_state["affranchie_return_feedback"] = {
            "kind": "warning",
            "message": "Retour reçu, mais le statut n’a pas encore pu être vérifié.",
        }
    st.session_state["affranchie_last_returned_checkout_id"] = checkout_id
    st.query_params.clear()


def _decision_type_for_storage(repo: Any) -> str:
    db_id = str(getattr(repo, "decisions_db_id", "") or "")
    if not db_id:
        return "description_status"
    try:
        schema = get_database_schema(repo.client, db_id)
        type_meta = schema.get("type")
        if isinstance(type_meta, dict) and type_meta.get("type") == "select":
            options = (type_meta.get("select") or {}).get("options") or []
            names = [
                str(opt.get("name") or "")
                for opt in options
                if isinstance(opt, dict) and opt.get("name")
            ]
            for preferred in [
                "description_status",
                "journey_A",
                "journey_B",
                "structure_choice",
            ]:
                if preferred in names:
                    return preferred
            if names:
                return names[0]
    except Exception:
        pass
    return "description_status"


def _set_step(step: str) -> None:
    if step in STEPS:
        st.session_state["affranchie_step"] = step


def _step_nav(current: str) -> None:
    index = STEPS.index(current)
    c1, c2 = st.columns(2)
    with c1:
        if st.button("Retour", use_container_width=True, disabled=index == 0):
            _set_step(STEPS[index - 1])
            st.rerun()
    with c2:
        if st.button(
            "Continuer",
            type="primary",
            use_container_width=True,
            disabled=index == len(STEPS) - 1,
        ):
            _set_step(STEPS[index + 1])
            st.rerun()


def main() -> None:
    set_page()
    apply_theme()
    ensure_session_state()
    repo = get_notion_repo()
    if not repo:
        st.error("La connexion à la base n’est pas disponible pour le moment.")
        st.stop()
    authenticator = get_authenticator(repo)
    authenticator.login(
        location="hidden", key="affranchie-auth", callback=remember_access
    )
    ensure_session_context(repo)
    if not st.session_state.get("authentication_status"):
        st.info("Connecte-toi quand tu es prêt·e, puis reviens ici.")
        if st.button("Aller à la connexion", type="primary", use_container_width=True):
            st.switch_page("pages/02_Login.py")
        st.stop()

    player_page_id = str(st.session_state.get("player_page_id") or "").strip()
    session_id = str(st.session_state.get("session_id") or "").strip()
    session_label = str(st.session_state.get("session_title") or "Session")
    if not player_page_id or not session_id:
        st.error("Il manque encore quelques informations pour ouvrir ton espace.")
        st.stop()

    _reconcile_returned_checkout(
        repo,
        session_id,
        player_page_id,
        float(
            parse_number(st.session_state.get("affranchie-contribution-amount")) or 0.0
        ),
    )

    state = _resolve_state(repo, session_id, player_page_id)
    resolved = state["resolved"]
    questions = state["questions"]
    player = state["player"]
    attempts = _load_payment_attempts(repo, session_id, player_page_id)
    _refresh_actionable_attempts(
        repo,
        session_id=session_id,
        player_id=player_page_id,
        attempts=attempts,
    )
    attempts = _load_payment_attempts(repo, session_id, player_page_id)
    latest_attempt_payload = (attempts[0].get("payload", {}) if attempts else {}) or {}
    confirmed_amount_for_sidebar = parse_number(
        st.session_state.get("affranchie_contribution_confirmed_amount")
    )
    current_sidebar_attempt = (
        _latest_attempt_for_amount(attempts, float(confirmed_amount_for_sidebar))
        if confirmed_amount_for_sidebar is not None
        else (attempts[0] if attempts else {})
    )
    current_sidebar_payload = (
        (current_sidebar_attempt.get("payload", {}) or {})
        if current_sidebar_attempt
        else latest_attempt_payload
    )
    pending_attempts = [
        item
        for item in attempts
        if str((item.get("payload", {}) or {}).get("sumup_status") or "").lower()
        in {"checkout_created", "pending"}
    ]

    if settings.show_debug or not settings.is_production:
        sidebar_technical_debug(
            page_label="09_Affranchie",
            repo=repo,
            extra={
                "current_step": step if "step" in locals() else "",
                "payment_attempts_count": len(attempts),
                "pending_attempts_count": len(pending_attempts),
                "latest_payment_status": str(
                    current_sidebar_payload.get("sumup_status") or ""
                ),
                "latest_checkout_id": str(
                    current_sidebar_payload.get("sumup_checkout_id") or ""
                ),
                "latest_transaction_id": str(
                    current_sidebar_payload.get("sumup_transaction_id") or ""
                ),
                "latest_payment_link": str(
                    _extract_payment_link(current_sidebar_payload) or ""
                ),
                "query_params": dict(st.query_params),
                "payment_trace": list(
                    st.session_state.get("affranchie_payment_trace", []) or []
                ),
            },
        )
        with st.sidebar:
            with st.expander("Debug · Transaction courante", expanded=True):
                _render_current_transaction_sidebar(
                    current_sidebar_attempt or {},
                    player=player,
                    session_id=session_id,
                    session_label=session_label,
                )
            with st.expander("Debug · Tentatives enregistrées", expanded=False):
                _render_payment_attempts_sidebar(attempts)

    st.title("Espace Affranchi·e")
    st.caption(f"{session_label} · {player.get('nickname')}")
    st.caption("En trois étapes : présence, retour, support.")
    returned_feedback = st.session_state.pop("affranchie_return_feedback", None)
    if isinstance(returned_feedback, dict):
        kind = str(returned_feedback.get("kind") or "info")
        message = str(returned_feedback.get("message") or "").strip()
        if message:
            if kind == "success":
                st.success(message)
            elif kind == "warning":
                st.warning(message)
            elif kind == "error":
                st.error(message)
            else:
                st.info(message)

    st.session_state.setdefault("affranchie_step", "participation")
    step = str(st.session_state.get("affranchie_step") or "participation")
    if step not in STEPS:
        step = "participation"
        _set_step(step)
    progress_idx = STEPS.index(step) + 1
    st.caption(f"Étape {progress_idx}/3")

    if step == "participation":
        st.subheader("As-tu participé à la session ?")
        st.write("Cela nous aide à comprendre qui était présent·e.")
        default_value = "Oui" if bool(resolved.get("participation")) else "Non"
        choice = st.radio(
            "Participation",
            options=["Oui", "Non"],
            index=0 if default_value == "Oui" else 1,
            horizontal=True,
            key="affranchie-participation-choice",
            label_visibility="collapsed",
        )
        if choice == "Oui":
            st.caption("Ta participation sera signalée à l’équipe.")
        else:
            st.caption(
                "Tu peux quand même laisser un retour ou contribuer si tu le souhaites."
            )

        participation_qids = list(state.get("participation_qids") or [])
        if participation_qids:
            qid = participation_qids[0]
            try:
                _upsert_response(
                    repo,
                    session_id,
                    player_page_id,
                    qid,
                    "oui" if choice == "Oui" else "non",
                )
            except Exception:
                pass
        _step_nav(step)

    elif step == "feedback":
        st.subheader("Un retour, une impression ?")
        st.write("Un mot, une idée, une sensation — tout est bienvenu.")
        feedback_enabled = st.toggle(
            "Ajouter un retour",
            value=bool(resolved.get("feedback_text")),
            key="affranchie-feedback-enabled",
            help="Active ce volet si tu veux laisser un message ou une note audio.",
        )
        if feedback_enabled:
            msg = st.text_area(
                "Message (optionnel)",
                value=str(resolved.get("feedback_text") or ""),
                placeholder="Ce que tu as ressenti, aimé, ou ce qui pourrait évoluer…",
                key="affranchie-feedback-text",
                height=120,
            )
            if st.button("Enregistrer", use_container_width=True):
                try:
                    _save_comment(repo, player_page_id, msg)
                    st.success("Message enregistré.")
                except Exception as exc:
                    st.error(f"Échec enregistrement message: {exc}")

            st.markdown("#### Ou une note audio")
            st.caption("Quelques secondes suffisent.")
            audio_notes = _load_audio_notes(repo, session_id, player_page_id)
            if audio_notes:
                latest_audio = audio_notes[0].get("payload", {})
                latest_path = _audio_path_from_payload(latest_audio)
                if latest_path and Path(latest_path).exists():
                    st.audio(latest_path)
                with st.expander("Historique", expanded=False):
                    for note in audio_notes:
                        payload = note.get("payload", {})
                        st.caption(
                            f"{payload.get('file_name', 'audio')} · {note.get('created_at', '')}"
                        )
            recorded = st.audio_input("Enregistrer une note")
            if recorded is not None and st.button(
                "Sauvegarder la note audio", use_container_width=True
            ):
                try:
                    adapter = get_audio_storage_adapter()
                    stored = adapter.store(
                        session_id=session_id,
                        player_id=player_page_id,
                        file_name=recorded.name,
                        mime_type=str(recorded.type or ""),
                        content=recorded.getvalue(),
                    )
                    _save_audio_note(
                        repo,
                        session_id=session_id,
                        player_id=player_page_id,
                        file_name=stored.file_name,
                        mime_type=stored.mime_type,
                        size_bytes=stored.size_bytes,
                        storage_provider=stored.storage_provider,
                        storage_path=stored.storage_path,
                        public_url=stored.public_url,
                        created_at=stored.created_at,
                    )
                    st.success("Note audio enregistrée.")
                except Exception as exc:
                    st.error(f"Échec enregistrement audio: {exc}")
        else:
            st.caption("Tu peux continuer sans laisser de message.")
        _step_nav(step)

    else:
        st.subheader("Soutenir la session")
        st.write("Si tu le souhaites, tu peux contribuer aux frais du dîner.")
        st.caption("Chacun·e contribue selon ses moyens et son envie.")
        proposed_amount = float(parse_number(resolved.get("contribution_eur")) or 0.0)
        st.session_state.setdefault("affranchie-contribution-amount", proposed_amount)
        default_mode = "Garder la proposition" if proposed_amount > 0 else "Changer"
        st.session_state.setdefault("affranchie-contribution-mode", default_mode)
        st.session_state.setdefault("affranchie_contribution_confirmed", False)
        st.session_state.setdefault("affranchie_contribution_confirmed_amount", None)
        st.session_state.setdefault("affranchie_contribution_confirmed_mode", None)

        confirmed_amount = parse_number(
            st.session_state.get("affranchie_contribution_confirmed_amount")
        )
        confirmed_mode = str(
            st.session_state.get("affranchie_contribution_confirmed_mode") or ""
        )
        mode = str(st.session_state.get("affranchie-contribution-mode") or default_mode)
        if proposed_amount <= 0:
            mode = "Changer"
        if mode == "Garder la proposition":
            st.session_state["affranchie-contribution-amount"] = proposed_amount
        amount = float(st.session_state.get("affranchie-contribution-amount") or 0.0)
        is_confirmed = confirmed_amount == float(amount) and confirmed_mode == mode

        if not is_confirmed:
            if proposed_amount > 0:
                st.info(f"Montant actuellement proposé: {_format_eur(proposed_amount)} EUR.")
                mode = st.radio(
                    "Choix du montant",
                    options=["Garder la proposition", "Changer"],
                    horizontal=True,
                    key="affranchie-contribution-mode",
                    label_visibility="collapsed",
                )
                if mode == "Garder la proposition":
                    st.session_state["affranchie-contribution-amount"] = proposed_amount
            else:
                mode = "Changer"
                st.caption("Aucun montant n’est encore proposé. Tu peux en choisir un ici.")

            if mode == "Changer":
                presets = [0, 10, 15, 20, 30]
                pcols = st.columns(len(presets))
                for i, val in enumerate(presets):
                    with pcols[i]:
                        if st.button(
                            f"{val} EUR",
                            key=f"affranchie-preset-{val}",
                            use_container_width=True,
                        ):
                            _set_contribution_amount(float(val))
                            st.rerun()
                amount = st.number_input(
                    "Montant",
                    min_value=0.0,
                    max_value=100000.0,
                    step=0.01,
                    key="affranchie-contribution-amount",
                )
            amount = float(st.session_state.get("affranchie-contribution-amount") or 0.0)

        contribution_qids = list(state.get("contribution_qids") or [])
        contribution_qid = contribution_qids[0] if contribution_qids else ""
        participation_qids = list(state.get("participation_qids") or [])
        participation_qid = participation_qids[0] if participation_qids else ""
        if not is_confirmed:
            confirm_label = "Confirmer la contribution"
            if st.button(confirm_label, type="primary", use_container_width=True):
                if amount <= 0:
                    st.warning("Choisis un montant supérieur à 0 pour continuer.")
                elif not contribution_qid:
                    st.warning("Question contribution introuvable.")
                else:
                    try:
                        _persist_aftercare_inputs(
                            repo,
                            session_id=session_id,
                            player_id=player_page_id,
                            participation_qid=participation_qid,
                            resolved_feedback_text=str(
                                resolved.get("feedback_text") or ""
                            ),
                        )
                        _upsert_response(
                            repo,
                            session_id,
                            player_page_id,
                            contribution_qid,
                            float(amount),
                        )
                        st.session_state["affranchie_contribution_confirmed"] = True
                        st.session_state["affranchie_contribution_confirmed_amount"] = (
                            float(amount)
                        )
                        st.session_state["affranchie_contribution_confirmed_mode"] = (
                            mode
                        )
                        _append_payment_trace(
                            "contribution_confirmed",
                            amount_eur=float(amount),
                            mode=mode,
                        )
                        _create_checkout_and_persist(
                            repo,
                            session_id=session_id,
                            player_page_id=player_page_id,
                            player_nickname=str(player.get("nickname") or ""),
                            amount=float(amount),
                        )
                    except Exception as exc:
                        st.error(f"Échec confirmation montant: {exc}")
        else:
            st.success(f"Montant confirmé: {_format_eur(amount)} EUR.")
            current_attempt = _latest_attempt_for_amount(attempts, amount)
            current_payload = (
                (current_attempt.get("payload", {}) or {}) if current_attempt else {}
            )
            current_status = str(current_payload.get("sumup_status") or "").lower()
            current_checkout_id = str(current_payload.get("sumup_checkout_id") or "")
            current_payment_link = _extract_payment_link(current_payload)

            if current_status == "paid":
                st.success(
                    "Une contribution pour ce montant a déjà été confirmée pour cette session."
                )
            else:
                st.caption(f"Statut actuel: {_status_fr(current_status or 'checkout_created')}")

            action_left, action_right = st.columns(2)
            with action_left:
                if current_payment_link:
                    st.link_button(
                        "Contribuer maintenant",
                        current_payment_link,
                        type="primary",
                        use_container_width=True,
                    )
                elif current_checkout_id:
                    if st.button(
                        "Contribuer maintenant",
                        type="primary",
                        use_container_width=True,
                        key=f"affranchie-pay-now-{current_checkout_id}",
                    ):
                        _sumup_execute_dialog(current_checkout_id)
                else:
                    st.button(
                        "Contribuer maintenant",
                        type="primary",
                        use_container_width=True,
                        disabled=True,
                    )
            with action_right:
                verify_checkout_id = current_checkout_id or str(
                    current_payload.get("sumup_checkout_id") or ""
                )
                if st.button(
                    "Vérifier le statut",
                    use_container_width=True,
                    disabled=not bool(verify_checkout_id),
                ):
                    try:
                        verify_result = _verify_checkout_attempt(
                            repo,
                            session_id=session_id,
                            player_id=player_page_id,
                            amount_eur=float(amount),
                            checkout_id=verify_checkout_id,
                            fallback_payload=current_payload,
                        )
                        _append_payment_trace(
                            "checkout_status_checked",
                            checkout_id=verify_checkout_id,
                            transaction_id=str(verify_result.get("transaction_id") or ""),
                            status=str(verify_result.get("status") or ""),
                        )
                        if str(verify_result.get("status") or "") == "paid":
                            st.balloons()
                            st.success("Paiement confirmé.")
                            if verify_result.get("receipt_svg_url"):
                                st.link_button(
                                    "Télécharger le reçu SVG",
                                    str(verify_result.get("receipt_svg_url")),
                                    use_container_width=True,
                                )
                        else:
                            st.success(
                                f"Statut mis à jour: {_status_fr(str(verify_result.get('status') or 'pending'))}"
                            )
                    except Exception as exc:
                        _append_payment_trace(
                            "checkout_status_failed",
                            checkout_id=verify_checkout_id,
                            error=str(exc),
                        )
                        st.error(f"Vérification échouée: {exc}")

        attempts = _load_payment_attempts(repo, session_id, player_page_id)
        active_identity = _attempt_identity(current_attempt) if current_attempt else ""
        remaining_attempts = [
            attempt
            for attempt in _latest_unique_attempts(attempts)
            if _attempt_identity(attempt) != active_identity
        ]
        if remaining_attempts:
            with st.expander("Historique des contributions", expanded=False):
                _render_contribution_history(
                    repo, session_id, player_page_id, remaining_attempts
                )
        with st.expander("Rattacher un paiement existant", expanded=False):
            st.caption(
                "Si un paiement SumUp a abouti mais n’apparaît pas encore ici, tu peux le rattacher avec son identifiant de transaction."
            )
            attach_tx_id = st.text_input(
                "Identifiant de transaction SumUp",
                key="affranchie-attach-transaction-id",
                placeholder="ex. 7da90897-2f56-47dd-83b7-47b0e749d286",
            ).strip()
            if st.button("Rattacher cette transaction", use_container_width=True):
                if not attach_tx_id:
                    st.warning("Indique d’abord un identifiant de transaction.")
                elif _attempt_exists_for_transaction(attempts, attach_tx_id):
                    st.info("Cette transaction est déjà rattachée à ton historique.")
                else:
                    client = SumUpClient.from_secrets()
                    result = client.transaction_details(attach_tx_id)
                    if result.get("ok"):
                        payload_tx = _extract_transaction_payload(result.get("json"))
                        if not payload_tx:
                            st.warning(
                                "La transaction a été trouvée, mais son format n’a pas pu être interprété."
                            )
                        else:
                            tx_status = _normalized_payment_status(
                                str(payload_tx.get("status") or "")
                            )
                            tx_amount = parse_number(payload_tx.get("amount"))
                            if tx_amount is None:
                                tx_amount = amount
                            try:
                                _persist_payment_attempt(
                                    repo,
                                    session_id=session_id,
                                    player_id=player_page_id,
                                    amount_eur=float(tx_amount),
                                    currency=str(payload_tx.get("currency") or "EUR"),
                                    checkout_id="",
                                    status=tx_status,
                                    payment_url="",
                                    transaction_id=str(
                                        payload_tx.get("id") or attach_tx_id
                                    ),
                                    payload=payload_tx,
                                )
                                _append_payment_trace(
                                    "transaction_attached",
                                    transaction_id=str(
                                        payload_tx.get("id") or attach_tx_id
                                    ),
                                    status=tx_status,
                                    amount_eur=float(tx_amount),
                                )
                                st.success("Transaction rattachée à ton historique.")
                                st.rerun()
                            except Exception as exc:
                                st.error(f"Rattachement impossible: {exc}")
                    else:
                        _append_payment_trace(
                            "transaction_attach_failed",
                            transaction_id=attach_tx_id,
                            error=str(result.get("error") or ""),
                        )
                        st.error(
                            f"Impossible de récupérer cette transaction: {result.get('error')}"
                        )

        st.success(
            "Merci 🙏 Ta participation, ton retour et ta contribution font vivre ce moment."
        )
        if st.button("Laisser un retour", use_container_width=True):
            _set_step("feedback")
            st.rerun()


if __name__ == "__main__":
    main()
