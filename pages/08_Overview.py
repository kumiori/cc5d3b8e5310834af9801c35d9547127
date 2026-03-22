from __future__ import annotations

from collections import Counter
from datetime import datetime
from statistics import mean, median
from typing import Any, Dict, List
import re

import pandas as pd
import streamlit as st

from infra.app_context import get_authenticator, get_notion_repo
from infra.app_state import ensure_session_context, ensure_session_state, remember_access
from infra.notion_repo import _execute_with_retry, _resolve_data_source_id, get_database_schema
from services.notion_value_utils import (
    as_list_labels,
    find_exact_prop,
    find_prop,
    parse_json_text,
    parse_number,
    read_checkbox,
    read_multiselect_names,
    read_number,
    read_relation_first,
    read_rich_text,
    read_select_name,
    read_title,
)
from ui import apply_theme, set_page, sidebar_auth_controls, sidebar_technical_debug


def _list_sessions_from_db(repo: Any, *, limit: int = 300) -> List[Dict[str, Any]]:
    """Load sessions directly from Notion with schema-based property resolution."""
    sessions_db_id = str(getattr(repo, "session_db_id", "") or "")
    if not sessions_db_id:
        return []
    ds_id = _resolve_data_source_id(repo.client, sessions_db_id)
    if not ds_id:
        return []
    schema = get_database_schema(repo.client, sessions_db_id)
    code_prop = find_prop(schema, "session_code", "rich_text")
    code_title_prop = find_prop(schema, "session_code", "title")
    name_prop = find_prop(schema, "session_name", "rich_text")
    name_title_prop = find_prop(schema, "session_name", "title")
    title_prop = find_prop(schema, "session_title", "rich_text")
    title_title_prop = find_prop(schema, "session_title", "title")
    default_title_prop = find_prop(schema, "Name", "title")
    status_prop = find_prop(schema, "status", "select")
    active_prop = find_prop(schema, "active", "checkbox")
    mode_prop = find_prop(schema, "mode", "select")

    out: List[Dict[str, Any]] = []
    query: Dict[str, Any] = {
        "data_source_id": ds_id,
        "page_size": min(100, max(1, limit)),
        "sorts": [{"timestamp": "created_time", "direction": "descending"}],
    }
    while True:
        payload = _execute_with_retry(repo.client.data_sources.query, **query)
        for page in payload.get("results", []):
            props = page.get("properties", {}) if isinstance(page, dict) else {}
            session_code = (
                read_rich_text(props, code_prop)
                or read_title(props, code_title_prop)
                or read_rich_text(props, name_prop)
                or read_title(props, name_title_prop)
                or read_rich_text(props, title_prop)
                or read_title(props, title_title_prop)
                or read_title(props, default_title_prop)
                or "session"
            )
            out.append(
                {
                    "id": str(page.get("id") or ""),
                    "session_code": session_code,
                    "status": read_select_name(props, status_prop),
                    "mode": read_select_name(props, mode_prop),
                    "active": read_checkbox(props, active_prop),
                }
            )
        if not payload.get("has_more") or len(out) >= limit:
            break
        query["start_cursor"] = payload.get("next_cursor")
    # keep unique ids only
    unique: Dict[str, Dict[str, Any]] = {}
    for row in out:
        sid = str(row.get("id") or "")
        if sid and sid not in unique:
            unique[sid] = row
    return list(unique.values())


def _load_responses_for_session(repo: Any, session_id: str) -> List[Dict[str, Any]]:
    """Load response rows for one session and normalize key columns."""
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
            qid = read_relation_first(props, question_rel_prop) or read_rich_text(props, question_id_prop) or read_rich_text(props, item_id_prop)
            title_text = read_title(props, title_prop)
            if not qid and title_text:
                m = re.search(r"Q:([0-9a-fA-F-]{16,40})", title_text)
                if m:
                    qid = m.group(1)
            player_id = read_relation_first(props, player_prop)
            if not player_id and title_text:
                mp = re.search(r"P:([0-9a-fA-F-]{16,40})", title_text)
                if mp:
                    player_id = mp.group(1)
            value_raw = read_rich_text(props, value_prop) if value_prop else ""
            out.append(
                {
                    "id": str(page.get("id") or ""),
                    "player_id": player_id,
                    "question_id": qid,
                    "value": parse_json_text(value_raw),
                    "value_number": read_number(props, value_number_prop) if value_number_prop else None,
                    "created_at": str(page.get("created_time") or ""),
                    "title_key": title_text,
                }
            )
        if not payload.get("has_more"):
            break
        query["start_cursor"] = payload.get("next_cursor")
    return out


def _latest_by_player_question(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Keep latest row per player/question based on created_at ordering."""
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


def _contribution_question_ids(questions: List[Dict[str, Any]]) -> List[str]:
    """Extract question ids that look like contribution prompts."""
    ids: List[str] = []
    for q in questions:
        text = str(q.get("text") or "").lower()
        if "contribution" in text:
            qid = str(q.get("id") or "")
            if qid:
                ids.append(qid)
    return ids


def _question_ids_for(questions: List[Dict[str, Any]], key: str) -> List[str]:
    """Extract question ids for semantic buckets used in overview aggregates."""
    aliases = {
        "diet": ["régime", "regime", "preference", "préférence"],
        "allergens": ["allergène", "allergen"],
        "hard_no": ["ingrédients non", "ingredient", "exclusion", "non"],
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


def _load_player_page_profiles(repo: Any, player_page_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """Load player profile fields for a set of player page ids."""
    players_db_id = str(getattr(repo, "players_db_id", "") or "")
    if not players_db_id:
        return {}
    schema = get_database_schema(repo.client, players_db_id)
    nickname_prop = find_prop(schema, "nickname", "rich_text")
    title_prop = find_prop(schema, "Name", "title")
    access_prop = find_prop(schema, "access_key", "rich_text")
    diet_prop = find_prop(schema, "diet", "multi_select")
    allergens_prop = find_prop(schema, "allergens", "multi_select")
    hard_no_prop = find_prop(schema, "hard_no", "multi_select")
    out: Dict[str, Dict[str, Any]] = {}
    for pid in sorted(set(player_page_ids)):
        if not pid:
            continue
        try:
            page = _execute_with_retry(repo.client.pages.retrieve, page_id=pid)
        except Exception:
            continue
        props = page.get("properties", {}) if isinstance(page, dict) else {}
        out[pid] = {
            "id": pid,
            "nickname": read_rich_text(props, nickname_prop) or read_title(props, title_prop) or pid[:8],
            "access_key": read_rich_text(props, access_prop),
            "diet": read_multiselect_names(props, diet_prop),
            "allergens": read_multiselect_names(props, allergens_prop),
            "hard_no": read_multiselect_names(props, hard_no_prop),
        }
    return out


def _session_summary(repo: Any, session: Dict[str, Any]) -> Dict[str, Any]:
    """Compute aggregate and per-player summaries for a selected session."""
    session_id = str(session.get("id") or "")
    questions = repo.list_questions(session_id)
    responses = _load_responses_for_session(repo, session_id)
    latest = _latest_by_player_question(responses)
    response_player_ids = [str(pid) for pid in latest.keys() if str(pid)]
    profile_by_player = _load_player_page_profiles(repo, response_player_ids)

    players_from_repo = repo.list_players(session_id)
    for player in players_from_repo:
        pid = str(player.get("id") or "")
        if not pid or pid in profile_by_player:
            continue
        profile_by_player[pid] = {
            "id": pid,
            "nickname": player.get("nickname") or player.get("access_key") or pid[:8],
            "access_key": player.get("access_key") or "",
            "diet": list(player.get("diet", []) or []),
            "allergens": list(player.get("allergens", []) or []),
            "hard_no": list(player.get("hard_no", []) or []),
        }

    all_player_ids = sorted(set(response_player_ids) | set(profile_by_player.keys()))
    players_count = len(all_player_ids)
    contribution_qids = set(_contribution_question_ids(questions))
    if not contribution_qids:
        contribution_qids = {"contribution", "economic_contribution"}
    diet_qids = set(_question_ids_for(questions, "diet"))
    allerg_qids = set(_question_ids_for(questions, "allergens"))
    hard_no_qids = set(_question_ids_for(questions, "hard_no"))

    diet_counter = Counter()
    allergens_counter = Counter()
    hard_no_counter = Counter()

    contrib_values: List[float] = []
    contribution_debug_rows: List[Dict[str, Any]] = []
    per_player_rows: List[Dict[str, Any]] = []
    for pid in all_player_ids:
        player = profile_by_player.get(pid, {})
        qmap = latest.get(pid, {})

        response_diet: List[str] = []
        response_allergens: List[str] = []
        response_hard_no: List[str] = []
        for qid in diet_qids:
            response_diet.extend(as_list_labels((qmap.get(qid) or {}).get("value")))
        for qid in allerg_qids:
            response_allergens.extend(as_list_labels((qmap.get(qid) or {}).get("value")))
        for qid in hard_no_qids:
            response_hard_no.extend(as_list_labels((qmap.get(qid) or {}).get("value")))

        effective_diet = response_diet or list(player.get("diet", []) or [])
        effective_allergens = response_allergens or list(player.get("allergens", []) or [])
        effective_hard_no = response_hard_no or list(player.get("hard_no", []) or [])

        for val in effective_diet:
            diet_counter[str(val)] += 1
        for val in effective_allergens:
            allergens_counter[str(val)] += 1
        for val in effective_hard_no:
            hard_no_counter[str(val)] += 1

        contribution = None
        matched_qid = ""
        for qid in contribution_qids:
            row = qmap.get(qid)
            if not row:
                continue
            contribution = parse_number(row.get("value_number"))
            if contribution is None:
                contribution = parse_number(row.get("value"))
            if contribution is not None:
                matched_qid = qid
                break
        if contribution is not None:
            contrib_values.append(float(contribution))
        contribution_debug_rows.append(
            {
                "player_id": pid,
                "player": player.get("nickname") or player.get("access_key") or pid[:8],
                "matched_contribution_qid": matched_qid,
                "contribution_value": contribution,
                "known_qids_for_player": sorted(list(qmap.keys())),
            }
        )
        per_player_rows.append(
            {
                "player": player.get("nickname") or player.get("access_key") or pid[:8],
                "diet": ", ".join(effective_diet) or "none",
                "allergens": ", ".join(effective_allergens) or "none",
                "hard_no": ", ".join(effective_hard_no) or "none",
                "contribution_eur": contribution,
            }
        )

    return {
        "session_id": session_id,
        "session_code": session.get("session_code") or "session",
        "players_count": players_count,
        "respondents_count": len(latest),
        "contribution_count": len(contrib_values),
        "contribution_sum": round(sum(contrib_values), 2) if contrib_values else 0.0,
        "contribution_avg": round(mean(contrib_values), 2) if contrib_values else 0.0,
        "contribution_median": round(median(contrib_values), 2) if contrib_values else 0.0,
        "diet_counter": diet_counter,
        "allergens_counter": allergens_counter,
        "hard_no_counter": hard_no_counter,
        "per_player_rows": per_player_rows,
        "debug": {
            "question_ids": {
                "contribution": sorted(contribution_qids),
                "diet": sorted(diet_qids),
                "allergens": sorted(allerg_qids),
                "hard_no": sorted(hard_no_qids),
            },
            "responses_total": len(responses),
            "responses_with_value_number": len([r for r in responses if r.get("value_number") is not None]),
            "sample_recent_rows": responses[:20],
            "contribution_match_rows": contribution_debug_rows,
        },
    }


def main() -> None:
    """Render session-level cuisine aggregates with debug and exports."""
    set_page()
    apply_theme()
    ensure_session_state()
    repo = get_notion_repo()
    if not repo:
        st.error("Notion repository unavailable.")
        st.stop()
    authenticator = get_authenticator(repo)
    authentication_status = sidebar_auth_controls(
        authenticator,
        callback=remember_access,
        key_prefix="overview-auth",
    )
    ensure_session_context(repo)
    sidebar_technical_debug(
        page_label="08_Overview",
        repo=repo,
        extra={
            "overview_cache_keys": len(st.session_state.get("_overview_cache", {})),
        },
    )
    if not authentication_status:
        st.warning("Please log in first.")
        st.stop()

    st.title("Overview · Cuisine")
    st.caption("Aggregate responses per session: kitchen preferences and prospective contributions.")

    sessions = _list_sessions_from_db(repo, limit=300)
    if not sessions:
        sessions = repo.list_sessions(limit=100)
    if not sessions:
        st.info("No sessions found.")
        st.stop()

    session_options = [str(s.get("id") or "") for s in sessions if s.get("id")]
    selected_id = st.selectbox(
        "Session",
        options=session_options,
        format_func=lambda sid: next(
            (
                f"{s.get('session_code') or 'session'}"
                f" · {s.get('status') or '—'}"
                f"{' · active' if s.get('active') else ''}"
                f" · {str(sid)[:8]}"
                for s in sessions
                if s.get("id") == sid
            ),
            sid,
        ),
    )
    selected = next((s for s in sessions if s.get("id") == selected_id), sessions[0])

    refresh = st.button("Refresh overview", use_container_width=True)
    cache = st.session_state.setdefault("_overview_cache", {})
    cache_key = str(selected.get("id") or "")
    if refresh or cache_key not in cache:
        with st.spinner("Computing overview..."):
            cache[cache_key] = _session_summary(repo, selected)
            cache[f"{cache_key}:at"] = datetime.now().isoformat(timespec="seconds")
    summary = cache.get(cache_key, {})
    st.caption(f"Computed at: {cache.get(f'{cache_key}:at', '—')}")

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Players", int(summary.get("players_count", 0)))
    m2.metric("Respondents", int(summary.get("respondents_count", 0)))
    m3.metric("Contributors", int(summary.get("contribution_count", 0)))
    m4.metric("Contribution sum (EUR)", float(summary.get("contribution_sum", 0.0)))

    m5, m6 = st.columns(2)
    m5.metric("Contribution avg (EUR)", float(summary.get("contribution_avg", 0.0)))
    m6.metric("Contribution median (EUR)", float(summary.get("contribution_median", 0.0)))
    with st.expander("Debug · Aggregation internals", expanded=False):
        st.json(
            {
                "session_id": summary.get("session_id"),
                "players_count": summary.get("players_count"),
                "respondents_count": summary.get("respondents_count"),
                "contribution_count": summary.get("contribution_count"),
                "diet_items": len(summary.get("diet_counter", {})),
                "allergen_items": len(summary.get("allergens_counter", {})),
                "hard_no_items": len(summary.get("hard_no_counter", {})),
                "responses_total": (summary.get("debug", {}) or {}).get("responses_total", 0),
                "responses_with_value_number": (summary.get("debug", {}) or {}).get("responses_with_value_number", 0),
                "question_ids": (summary.get("debug", {}) or {}).get("question_ids", {}),
            },
            expanded=False,
        )
        st.write("Recent response rows (sample)")
        st.json((summary.get("debug", {}) or {}).get("sample_recent_rows", []), expanded=False)
        st.write("Contribution matching by player")
        st.dataframe(
            pd.DataFrame((summary.get("debug", {}) or {}).get("contribution_match_rows", [])),
            hide_index=True,
            use_container_width=True,
        )

    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown("**Regime / preference**")
        diet_df = pd.DataFrame((summary.get("diet_counter") or {}).items(), columns=["item", "count"])
        st.dataframe(diet_df, hide_index=True, use_container_width=True)
    with c2:
        st.markdown("**Allergens**")
        allerg_df = pd.DataFrame((summary.get("allergens_counter") or {}).items(), columns=["item", "count"])
        st.dataframe(allerg_df, hide_index=True, use_container_width=True)
    with c3:
        st.markdown('**Ingrédients "non"**')
        hard_no_df = pd.DataFrame((summary.get("hard_no_counter") or {}).items(), columns=["item", "count"])
        st.dataframe(hard_no_df, hide_index=True, use_container_width=True)

    st.markdown("**Per-player detail**")
    detail_df = pd.DataFrame(summary.get("per_player_rows", []))
    st.dataframe(detail_df, hide_index=True, use_container_width=True)
    st.download_button(
        "Download CSV",
        data=detail_df.to_csv(index=False).encode("utf-8"),
        file_name=f"affranchis_overview_{summary.get('session_code','session')}.csv",
        mime="text/csv",
        use_container_width=True,
    )

    st.markdown("### Cross-session aggregate")
    if st.button("Compute all sessions aggregate", use_container_width=True):
        with st.spinner("Aggregating all sessions..."):
            all_rows = []
            for session in sessions:
                s = _session_summary(repo, session)
                all_rows.append(
                    {
                        "session_code": s.get("session_code"),
                        "players": s.get("players_count", 0),
                        "respondents": s.get("respondents_count", 0),
                        "contributors": s.get("contribution_count", 0),
                        "sum_eur": s.get("contribution_sum", 0.0),
                        "avg_eur": s.get("contribution_avg", 0.0),
                        "median_eur": s.get("contribution_median", 0.0),
                    }
                )
            all_df = pd.DataFrame(all_rows).sort_values(by="sum_eur", ascending=False)
            st.dataframe(all_df, hide_index=True, use_container_width=True)


if __name__ == "__main__":
    main()
