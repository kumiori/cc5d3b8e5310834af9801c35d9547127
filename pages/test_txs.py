from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from infra.app_context import get_authenticator, get_notion_repo
from infra.app_state import ensure_auth, ensure_session_context, ensure_session_state, remember_access
from services.sumup_client import SumUpClient, build_tx_stats, parse_metadata_text
from ui import apply_theme, set_page


TRACE_KEY = "_sumup_test_traces"
CHECKOUTS_KEY = "_sumup_created_checkouts"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _append_trace(title: str, result: Dict[str, Any]) -> None:
    traces = st.session_state.setdefault(TRACE_KEY, [])
    trace = {
        "at": _now(),
        "title": title,
        "ok": bool(result.get("ok")),
        "status_code": int(result.get("status_code") or 0),
        "error": str(result.get("error") or ""),
        "request": result.get("trace") or {},
        "response_json": result.get("json"),
        "response_text": (result.get("text") or "")[:2000],
    }
    traces.insert(0, trace)
    st.session_state[TRACE_KEY] = traces[:30]


def _register_checkout(payload: Dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        return
    checkout_id = str(payload.get("id") or "").strip()
    if not checkout_id:
        return
    rows = st.session_state.setdefault(CHECKOUTS_KEY, [])
    exists = any(str(row.get("id") or "") == checkout_id for row in rows if isinstance(row, dict))
    if exists:
        return
    rows.insert(
        0,
        {
            "id": checkout_id,
            "checkout_reference": str(payload.get("checkout_reference") or ""),
            "amount": payload.get("amount"),
            "currency": str(payload.get("currency") or ""),
            "status": str(payload.get("status") or "CREATED"),
            "date": str(payload.get("date") or _now()),
            "description": str(payload.get("description") or ""),
            "payment_link": _extract_checkout_payment_link(payload),
            "payload": payload,
        },
    )
    st.session_state[CHECKOUTS_KEY] = rows[:100]


def _update_registered_checkout(payload: Dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        return
    checkout_id = str(payload.get("id") or "").strip()
    if not checkout_id:
        return
    rows = st.session_state.setdefault(CHECKOUTS_KEY, [])
    replaced = False
    for idx, row in enumerate(rows):
        if str((row or {}).get("id") or "") == checkout_id:
            rows[idx] = {
                **(row or {}),
                "checkout_reference": str(payload.get("checkout_reference") or row.get("checkout_reference") or ""),
                "amount": payload.get("amount", row.get("amount")),
                "currency": str(payload.get("currency") or row.get("currency") or ""),
                "status": str(payload.get("status") or row.get("status") or ""),
                "date": str(payload.get("date") or row.get("date") or _now()),
                "description": str(payload.get("description") or row.get("description") or ""),
                "payment_link": _extract_checkout_payment_link(payload) or str(row.get("payment_link") or ""),
                "payload": payload,
            }
            replaced = True
            break
    if not replaced:
        _register_checkout(payload)
    st.session_state[CHECKOUTS_KEY] = rows


def _extract_history_items(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, dict):
        if isinstance(payload.get("items"), list):
            return payload["items"]
        if isinstance(payload.get("transactions"), list):
            return payload["transactions"]
    if isinstance(payload, list):
        return payload
    return []


def _json_text(value: Any) -> str:
    try:
        return str(value) if isinstance(value, str) else __import__("json").dumps(value, ensure_ascii=False)
    except Exception:
        return str(value)


def _match_text(item: Dict[str, Any], query: str) -> bool:
    q = str(query or "").strip().lower()
    if not q:
        return True
    blob = _json_text(item).lower()
    return q in blob


def _to_table_rows(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for item in items:
        rows.append(
            {
                "id": item.get("transaction_id") or item.get("id"),
                "status": item.get("status"),
                "amount": item.get("amount"),
                "currency": item.get("currency"),
                "payment_type": item.get("payment_type"),
                "timestamp": item.get("timestamp"),
                "card_type": (item.get("card") or {}).get("type", ""),
                "product_summary": item.get("product_summary", ""),
            }
        )
    return rows


def _extract_checkout_payment_link(checkout_payload: Any) -> str:
    if not isinstance(checkout_payload, dict):
        return ""
    links = checkout_payload.get("links") or []
    if not isinstance(links, list):
        return ""
    for link in links:
        if not isinstance(link, dict):
            continue
        rel = str(link.get("rel") or "").lower()
        href = str(link.get("href") or "")
        if rel in {"hosted_checkout", "checkout", "payment"} and href:
            return href
    return ""


@st.dialog("Execute checkout (SumUp widget)")
def _sumup_execute_dialog(checkout_id: str) -> None:
    st.caption(
        "Embedded execution using SumUp Card SDK. "
        "Widget callbacks are visible in browser console; server-side confirmation is done via API poll."
    )
    js_code = f"""
        <div id="sumup-card" style="min-height: 520px;"></div>
        <script type="text/javascript" src="https://gateway.sumup.com/gateway/ecom/card/v2/sdk.js"></script>
        <script type="text/javascript">
            const mountCard = () => {{
                if (!window.SumUpCard) {{
                    console.error("SumUpCard not available");
                    return;
                }}
                window.SumUpCard.mount({{
                    id: "sumup-card",
                    checkoutId: "{checkout_id}",
                    donateSubmitButton: false,
                    showInstallments: true,
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


def main() -> None:
    set_page()
    apply_theme()
    ensure_session_state()
    repo = get_notion_repo()
    authenticator = get_authenticator(repo)
    ensure_auth(authenticator, callback=remember_access, key="sumup-test-login")
    ensure_session_context(repo)

    st.title("Test SumUp · Transactions")
    st.caption(
        "Debug page for SumUp integration. "
        "Includes connection checks, checkout creation with metadata/custom headers, "
        "and transaction pooling for transparency stats."
    )

    client = SumUpClient.from_secrets()
    st.subheader("Connection status")
    st.json(client.config_debug(), expanded=False)
    if not client.is_configured():
        st.error("SumUp secrets are missing (`sumup.CLIENT_API_SECRET` and `sumup.MERCHANT_ID`).")
        st.stop()
    st.session_state.setdefault(CHECKOUTS_KEY, [])

    with st.expander("Request controls", expanded=True):
        c1, c2, c3 = st.columns(3)
        with c1:
            history_limit = st.number_input(
                "History limit",
                min_value=1,
                max_value=200,
                value=20,
                step=1,
            )
        with c2:
            status_filter = st.multiselect(
                "Statuses",
                options=["SUCCESSFUL", "FAILED", "CANCELLED", "PENDING"],
                default=["SUCCESSFUL"],
            )
        with c3:
            type_filter = st.multiselect(
                "Transaction types",
                options=["PAYMENT", "REFUND"],
                default=["PAYMENT"],
            )

        st.markdown("**Custom headers (debug):**")
        h1, h2 = st.columns(2)
        with h1:
            hdr_trace_id = st.text_input("X-Affranchis-Trace-Id", value=f"txs-{int(datetime.now().timestamp())}")
        with h2:
            hdr_source = st.text_input("X-Affranchis-Source", value="app_affranchis/test_txs")
        custom_headers = {
            "X-Affranchis-Trace-Id": hdr_trace_id,
            "X-Affranchis-Source": hdr_source,
        }

    st.subheader("Actions")
    a1, a2, a3 = st.columns(3)
    with a1:
        if st.button("1) Test `/me` connection", use_container_width=True):
            with st.spinner("Calling SumUp `/me`..."):
                result = client.me(extra_headers=custom_headers)
            _append_trace("GET /me", result)
            if result.get("ok"):
                st.success("Connection OK.")
            else:
                st.error(f"Connection failed: {result.get('error')}")

    with a2:
        if st.button("2) Fetch transaction history", use_container_width=True):
            with st.spinner("Fetching transaction history..."):
                result = client.transaction_history(
                    limit=int(history_limit),
                    statuses=status_filter,
                    tx_types=type_filter,
                    extra_headers=custom_headers,
                )
            _append_trace("GET /me/transactions/history", result)
            if result.get("ok"):
                st.success("History fetched.")
                st.session_state["_sumup_history_payload"] = result.get("json")
            else:
                st.error(f"History request failed: {result.get('error')}")

    with a3:
        tx_id = st.text_input("Transaction ID for details", key="sumup_tx_id")
        if st.button("3) Fetch transaction details", use_container_width=True):
            if not tx_id.strip():
                st.warning("Enter a transaction id first.")
            else:
                with st.spinner("Fetching transaction details..."):
                    result = client.transaction_details(tx_id.strip(), extra_headers=custom_headers)
                _append_trace("GET /me/transactions?id=...", result)
                if result.get("ok"):
                    st.success("Transaction details fetched.")
                    st.session_state["_sumup_tx_details"] = result.get("json")
                else:
                    st.error(f"Details request failed: {result.get('error')}")

    st.subheader("Create checkout test (custom metadata)")
    c1, c2, c3 = st.columns(3)
    with c1:
        checkout_amount = st.number_input("Amount", min_value=0.01, value=10.0, step=0.5)
    with c2:
        checkout_currency = st.text_input("Currency", value="EUR")
    with c3:
        checkout_ref = st.text_input("Checkout reference", value=f"affranchis-{int(datetime.now().timestamp())}")
    checkout_description = st.text_input("Description", value="Affranchis transparency test checkout")
    checkout_return_url = st.text_input("Return URL (optional)", value="")
    metadata_text = st.text_area(
        "Metadata JSON",
        value='{"source":"test_txs","purpose":"debug","pool":"transparency"}',
        help="JSON object that will be sent to SumUp as checkout metadata.",
    )
    if st.button("4) Create checkout", type="primary", use_container_width=True):
        try:
            metadata = parse_metadata_text(metadata_text)
        except ValueError as exc:
            st.error(str(exc))
        else:
            with st.spinner("Creating checkout..."):
                result = client.create_checkout(
                    amount=float(checkout_amount),
                    currency=checkout_currency.strip().upper(),
                    checkout_reference=checkout_ref.strip(),
                    description=checkout_description.strip(),
                    metadata=metadata,
                    return_url=checkout_return_url.strip() or None,
                    extra_headers=custom_headers,
                )
            _append_trace("POST /checkouts", result)
            if result.get("ok"):
                st.success("Checkout created.")
                checkout_payload = result.get("json")
                st.session_state["_sumup_checkout"] = checkout_payload
                checkout_id = str((checkout_payload or {}).get("id") or "")
                if checkout_id:
                    st.session_state["_sumup_last_checkout_id"] = checkout_id
                if isinstance(checkout_payload, dict):
                    _register_checkout(checkout_payload)
            else:
                st.error(f"Checkout creation failed: {result.get('error')}")

    st.subheader("Execute / verify checkout")
    checkout_payload = st.session_state.get("_sumup_checkout") or {}
    checkout_id_from_state = str(checkout_payload.get("id") or st.session_state.get("_sumup_last_checkout_id") or "")
    checkout_id_for_exec = st.text_input(
        "Checkout ID to execute/verify",
        value=checkout_id_from_state,
        key="sumup_checkout_id_for_exec",
    ).strip()
    cexec1, cexec2, cexec3 = st.columns(3)
    with cexec1:
        if st.button("Open execution dialog", use_container_width=True, disabled=not bool(checkout_id_for_exec)):
            _sumup_execute_dialog(checkout_id_for_exec)
    with cexec2:
        payment_link = _extract_checkout_payment_link(checkout_payload)
        if payment_link:
            st.link_button("Open hosted checkout page", payment_link, use_container_width=True)
        else:
            st.button("Open hosted checkout page", use_container_width=True, disabled=True)
    with cexec3:
        if st.button("Fetch checkout details", use_container_width=True, disabled=not bool(checkout_id_for_exec)):
            with st.spinner("Fetching checkout details..."):
                result = client.checkout_details(checkout_id_for_exec, extra_headers=custom_headers)
            _append_trace("GET /checkouts/{id}", result)
            if result.get("ok"):
                st.success("Checkout details fetched.")
                details_payload = result.get("json")
                st.session_state["_sumup_checkout_details"] = details_payload
                if isinstance(details_payload, dict):
                    _update_registered_checkout(details_payload)
            else:
                st.error(f"Checkout details failed: {result.get('error')}")

    st.markdown("**Recover/attach checkout by ID**")
    rec1, rec2 = st.columns([3, 1])
    with rec1:
        recover_checkout_id = st.text_input(
            "Checkout ID to attach to registry",
            value="",
            key="sumup_recover_checkout_id",
            help="Use this when a checkout exists but is not listed in current session state.",
        ).strip()
    with rec2:
        if st.button("Attach", use_container_width=True, disabled=not bool(recover_checkout_id)):
            with st.spinner("Fetching checkout for registry attach..."):
                result = client.checkout_details(recover_checkout_id, extra_headers=custom_headers)
            _append_trace("GET /checkouts/{id} attach", result)
            if result.get("ok") and isinstance(result.get("json"), dict):
                _update_registered_checkout(result["json"])
                st.success("Checkout attached to registry.")
            else:
                st.error(f"Attach failed: {result.get('error')}")

    st.markdown("**Polling helper (status tracking)**")
    p1, p2, p3 = st.columns(3)
    with p1:
        poll_attempts = st.number_input("Attempts", min_value=1, max_value=20, value=5, step=1)
    with p2:
        poll_interval = st.number_input("Interval (seconds)", min_value=1.0, max_value=30.0, value=2.0, step=0.5)
    with p3:
        poll_run = st.button("Poll checkout status", use_container_width=True, disabled=not bool(checkout_id_for_exec))
    if poll_run:
        terminal_statuses = {"PAID", "FAILED", "CANCELLED", "EXPIRED", "SUCCESSFUL"}
        for attempt in range(int(poll_attempts)):
            with st.spinner(f"Polling status {attempt + 1}/{int(poll_attempts)}..."):
                result = client.checkout_details(checkout_id_for_exec, extra_headers=custom_headers)
            _append_trace(f"GET /checkouts/{{id}} poll #{attempt + 1}", result)
            payload = result.get("json") if isinstance(result.get("json"), dict) else {}
            status = str((payload or {}).get("status") or "").upper()
            st.write(f"Attempt {attempt + 1}: status = `{status or 'unknown'}`")
            if status in terminal_statuses:
                st.success(f"Terminal status reached: {status}")
                st.session_state["_sumup_checkout_details"] = payload
                if isinstance(payload, dict):
                    _update_registered_checkout(payload)
                break
            if attempt < int(poll_attempts) - 1:
                import time as _t
                _t.sleep(float(poll_interval))

    history_payload = st.session_state.get("_sumup_history_payload")
    st.subheader("Transparency stats (pooled from fetched history)")
    if history_payload:
        stats = build_tx_stats(history_payload)
        m1, m2, m3 = st.columns(3)
        m1.metric("Transactions in pool", int(stats.get("count", 0)))
        m2.metric("Latest tx timestamp", str(stats.get("latest_timestamp") or "—"))
        m3.metric("Earliest tx timestamp", str(stats.get("earliest_timestamp") or "—"))
        st.write("Totals by currency")
        st.json(stats.get("totals_by_currency", {}), expanded=False)
        st.write("Counts by status")
        st.json(stats.get("count_by_status", {}), expanded=False)

        items = _extract_history_items(history_payload)
        if items:
            search_text = st.text_input(
                "Filter pooled transactions (id/reference/metadata/status text search)",
                value="",
                key="sumup_history_search_text",
            )
            filtered_items = [item for item in items if _match_text(item, search_text)]
            st.caption(f"Filtered results: {len(filtered_items)} / {len(items)}")
            df = pd.DataFrame(_to_table_rows(filtered_items))
            st.dataframe(df, use_container_width=True, hide_index=True)
            if filtered_items:
                with st.expander("Filtered transaction raw JSON", expanded=False):
                    st.json(filtered_items, expanded=False)
    else:
        st.info("Fetch transaction history first to compute transparency stats.")

    if st.session_state.get("_sumup_tx_details") is not None:
        with st.expander("Latest transaction details JSON", expanded=False):
            st.json(st.session_state.get("_sumup_tx_details"), expanded=False)
    if st.session_state.get("_sumup_checkout") is not None:
        with st.expander("Latest checkout JSON", expanded=False):
            st.json(st.session_state.get("_sumup_checkout"), expanded=False)
    if st.session_state.get("_sumup_checkout_details") is not None:
        with st.expander("Latest checkout details JSON", expanded=False):
            st.json(st.session_state.get("_sumup_checkout_details"), expanded=False)

    st.subheader("Return URL / query-param verification")
    qp = dict(st.query_params)
    st.caption("If SumUp redirects back with params, this block can verify checkout/transaction status.")
    st.json(qp, expanded=False)
    query_checkout_id = str(
        qp.get("checkout_id")
        or qp.get("id")
        or qp.get("checkoutId")
        or ""
    ).strip()
    query_tx_id = str(
        qp.get("tx_id")
        or qp.get("transaction_id")
        or qp.get("transactionId")
        or ""
    ).strip()
    if query_checkout_id:
        if st.button("Verify returned checkout_id", use_container_width=True):
            with st.spinner("Verifying returned checkout id..."):
                result = client.checkout_details(query_checkout_id, extra_headers=custom_headers)
            _append_trace("GET /checkouts/{id} from query params", result)
            if result.get("ok"):
                st.success("Returned checkout verified.")
                payload = result.get("json")
                st.session_state["_sumup_checkout_details"] = payload
                if isinstance(payload, dict):
                    _update_registered_checkout(payload)
            else:
                st.error(f"Verification failed: {result.get('error')}")
    elif query_tx_id:
        if st.button("Verify returned tx_id", use_container_width=True):
            with st.spinner("Verifying returned tx id..."):
                result = client.transaction_details(query_tx_id, extra_headers=custom_headers)
            _append_trace("GET /me/transactions from query params", result)
            if result.get("ok"):
                st.success("Returned transaction verified.")
                st.session_state["_sumup_tx_details"] = result.get("json")
            else:
                st.error(f"Verification failed: {result.get('error')}")
    else:
        st.info("No `checkout_id` or `tx_id` found in query params yet.")

    st.subheader("Created checkouts (session registry)")
    created_checkouts = st.session_state.get(CHECKOUTS_KEY, [])
    if not created_checkouts:
        st.caption("No checkout created yet in this session.")
    else:
        summary_rows = []
        for row in created_checkouts:
            summary_rows.append(
                {
                    "id": row.get("id"),
                    "reference": row.get("checkout_reference"),
                    "amount": row.get("amount"),
                    "currency": row.get("currency"),
                    "status": row.get("status"),
                    "date": row.get("date"),
                }
            )
        st.dataframe(pd.DataFrame(summary_rows), hide_index=True, use_container_width=True)
        for idx, row in enumerate(created_checkouts):
            cid = str(row.get("id") or "")
            with st.expander(
                f"Checkout {idx + 1} · {cid} · {row.get('amount')} {row.get('currency')} · {row.get('status')}",
                expanded=False,
            ):
                b1, b2, b3, b4 = st.columns(4)
                with b1:
                    if st.button("Execute", key=f"exec_checkout_{cid}", use_container_width=True):
                        _sumup_execute_dialog(cid)
                with b2:
                    payment_link = str(row.get("payment_link") or "")
                    if payment_link:
                        st.link_button(
                            "Hosted page",
                            payment_link,
                            key=f"hosted_checkout_{cid}",
                            use_container_width=True,
                        )
                    else:
                        st.button("Hosted page", key=f"hosted_checkout_disabled_{cid}", disabled=True, use_container_width=True)
                with b3:
                    if st.button("Fetch details", key=f"details_checkout_{cid}", use_container_width=True):
                        with st.spinner(f"Fetching checkout {cid} details..."):
                            result = client.checkout_details(cid, extra_headers=custom_headers)
                        _append_trace(f"GET /checkouts/{cid}", result)
                        if result.get("ok"):
                            payload = result.get("json")
                            st.session_state["_sumup_checkout_details"] = payload
                            if isinstance(payload, dict):
                                _update_registered_checkout(payload)
                            st.success("Details updated.")
                        else:
                            st.error(f"Details failed: {result.get('error')}")
                with b4:
                    if st.button("Use as current", key=f"use_current_checkout_{cid}", use_container_width=True):
                        st.session_state["_sumup_last_checkout_id"] = cid
                        st.success("Set as current checkout.")
                st.json(row.get("payload") or row, expanded=False)

        if st.button("Clear created checkout registry", use_container_width=True):
            st.session_state[CHECKOUTS_KEY] = []
            st.success("Checkout registry cleared.")

    st.subheader("Detailed debug trace")
    traces = st.session_state.get(TRACE_KEY, [])
    if not traces:
        st.caption("No SumUp calls yet.")
    else:
        headers = st.columns([2, 1, 1, 3])
        headers[0].markdown("**When**")
        headers[1].markdown("**Status**")
        headers[2].markdown("**HTTP**")
        headers[3].markdown("**Call**")
        for i, trace in enumerate(traces):
            cols = st.columns([2, 1, 1, 3])
            cols[0].write(trace.get("at"))
            cols[1].write("OK" if trace.get("ok") else "ERROR")
            cols[2].write(trace.get("status_code"))
            cols[3].write(trace.get("title"))
            with st.expander(f"Trace #{i+1}: {trace.get('title')}"):
                st.write("Request")
                st.json(trace.get("request"), expanded=False)
                st.write("Response JSON")
                st.json(trace.get("response_json"), expanded=False)
                if trace.get("response_text"):
                    st.write("Response text")
                    st.code(str(trace.get("response_text")))
                if trace.get("error"):
                    st.error(str(trace.get("error")))

    if st.button("Clear debug traces", use_container_width=True):
        st.session_state[TRACE_KEY] = []
        st.success("SumUp debug traces cleared.")


if __name__ == "__main__":
    main()
