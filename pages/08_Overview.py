from __future__ import annotations

import json
from collections import Counter
from datetime import datetime
from statistics import mean, median
from typing import Any, Dict, List, Optional
import re

import pandas as pd
import streamlit as st

from infra.app_context import get_authenticator, get_notion_repo
from infra.app_state import ensure_auth, ensure_session_context, ensure_session_state, remember_access, require_login
from infra.notion_repo import _execute_with_retry, _resolve_data_source_id, get_database_schema
from ui import apply_theme, set_page


def _find_prop(schema: Dict[str, Any], expected: str, ptype: Optional[str] = None) -> str:
    if expected in schema:
        return expected
    if ptype:
        for name, meta in schema.items():
            if isinstance(meta, dict) and meta.get("type") == ptype:
                return str(name)
    return expected


def _find_exact_prop(schema: Dict[str, Any], names: List[str], ptype: str) -> str:
    for name in names:
        meta = schema.get(name)
        if isinstance(meta, dict) and meta.get("type") == ptype:
            return name
    return ""


def _rt(props: Dict[str, Any], name: str) -> str:
    value = props.get(name)
    if not isinstance(value, dict) or value.get("type") != "rich_text":
        return ""
    return "".join(part.get("plain_text", "") for part in value.get("rich_text", []) if isinstance(part, dict))


def _title(props: Dict[str, Any], name: str) -> str:
    value = props.get(name)
    if not isinstance(value, dict) or value.get("type") != "title":
        return ""
    return "".join(part.get("plain_text", "") for part in value.get("title", []) if isinstance(part, dict))


def _relation_first(props: Dict[str, Any], name: str) -> str:
    value = props.get(name)
    if not isinstance(value, dict) or value.get("type") != "relation":
        return ""
    rel = value.get("relation", [])
    if rel and isinstance(rel[0], dict):
        return str(rel[0].get("id") or "")
    return ""


def _select_name(props: Dict[str, Any], name: str) -> str:
    value = props.get(name)
    if not isinstance(value, dict):
        return ""
    selected = value.get("select")
    if not isinstance(selected, dict):
        return ""
    return str(selected.get("name") or "")


def _checkbox_value(props: Dict[str, Any], name: str) -> bool:
    value = props.get(name)
    if not isinstance(value, dict):
        return False
    return bool(value.get("checkbox"))


def _number(props: Dict[str, Any], name: str) -> Optional[float]:
    value = props.get(name)
    if not isinstance(value, dict) or value.get("type") != "number":
        return None
    raw = value.get("number")
    if raw is None:
        return None
    try:
        return float(raw)
    except Exception:
        return None


def _from_json_text(text: str) -> Any:
    raw = str(text or "").strip()
    if not raw:
        return ""
    try:
        return json.loads(raw)
    except Exception:
        return raw


def _parse_numeric_value(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip().replace(",", ".")
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except Exception:
            return None
    return None


def _as_list_labels(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        if text.startswith("[") and text.endswith("]"):
            try:
                parsed = json.loads(text)
                if isinstance(parsed, list):
                    return [str(v).strip() for v in parsed if str(v).strip()]
            except Exception:
                pass
        return [text]
    return []


def _list_sessions_from_db(repo: Any, *, limit: int = 300) -> List[Dict[str, Any]]:
    sessions_db_id = str(getattr(repo, "session_db_id", "") or "")
    if not sessions_db_id:
        return []
    ds_id = _resolve_data_source_id(repo.client, sessions_db_id)
    if not ds_id:
        return []
    schema = get_database_schema(repo.client, sessions_db_id)
    code_prop = _find_prop(schema, "session_code", "rich_text")
    code_title_prop = _find_prop(schema, "session_code", "title")
    name_prop = _find_prop(schema, "session_name", "rich_text")
    name_title_prop = _find_prop(schema, "session_name", "title")
    title_prop = _find_prop(schema, "session_title", "rich_text")
    title_title_prop = _find_prop(schema, "session_title", "title")
    default_title_prop = _find_prop(schema, "Name", "title")
    status_prop = _find_prop(schema, "status", "select")
    active_prop = _find_prop(schema, "active", "checkbox")
    mode_prop = _find_prop(schema, "mode", "select")

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
                _rt(props, code_prop)
                or _title(props, code_title_prop)
                or _rt(props, name_prop)
                or _title(props, name_title_prop)
                or _rt(props, title_prop)
                or _title(props, title_title_prop)
                or _title(props, default_title_prop)
                or "session"
            )
            out.append(
                {
                    "id": str(page.get("id") or ""),
                    "session_code": session_code,
                    "status": _select_name(props, status_prop),
                    "mode": _select_name(props, mode_prop),
                    "active": _checkbox_value(props, active_prop),
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
    responses_db_id = str(getattr(repo, "responses_db_id", "") or "")
    if not responses_db_id:
        return []
    ds_id = _resolve_data_source_id(repo.client, responses_db_id)
    if not ds_id:
        return []
    schema = get_database_schema(repo.client, responses_db_id)
    session_prop = _find_exact_prop(schema, ["session"], "relation")
    player_prop = _find_exact_prop(schema, ["player"], "relation")
    question_rel_prop = _find_exact_prop(schema, ["question", "statement"], "relation")
    question_id_prop = _find_exact_prop(schema, ["question_id"], "rich_text")
    item_id_prop = _find_exact_prop(schema, ["item_id"], "rich_text")
    value_prop = _find_exact_prop(schema, ["value"], "rich_text")
    value_number_prop = _find_exact_prop(schema, ["value_number"], "number")
    title_prop = _find_exact_prop(schema, ["Name"], "title")

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
            qid = _relation_first(props, question_rel_prop) or _rt(props, question_id_prop) or _rt(props, item_id_prop)
            title_text = _title(props, title_prop)
            if not qid and title_text:
                m = re.search(r"Q:([0-9a-fA-F-]{16,40})", title_text)
                if m:
                    qid = m.group(1)
            player_id = _relation_first(props, player_prop)
            if not player_id and title_text:
                mp = re.search(r"P:([0-9a-fA-F-]{16,40})", title_text)
                if mp:
                    player_id = mp.group(1)
            value_raw = _rt(props, value_prop) if value_prop else ""
            out.append(
                {
                    "id": str(page.get("id") or ""),
                    "player_id": player_id,
                    "question_id": qid,
                    "value": _from_json_text(value_raw),
                    "value_number": _number(props, value_number_prop) if value_number_prop else None,
                    "created_at": str(page.get("created_time") or ""),
                    "title_key": title_text,
                }
            )
        if not payload.get("has_more"):
            break
        query["start_cursor"] = payload.get("next_cursor")
    return out


def _latest_by_player_question(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Dict[str, Any]]]:
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
    ids: List[str] = []
    for q in questions:
        text = str(q.get("text") or "").lower()
        if "contribution" in text:
            qid = str(q.get("id") or "")
            if qid:
                ids.append(qid)
    return ids


def _question_ids_for(questions: List[Dict[str, Any]], key: str) -> List[str]:
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
    players_db_id = str(getattr(repo, "players_db_id", "") or "")
    if not players_db_id:
        return {}
    schema = get_database_schema(repo.client, players_db_id)
    nickname_prop = _find_prop(schema, "nickname", "rich_text")
    title_prop = _find_prop(schema, "Name", "title")
    access_prop = _find_prop(schema, "access_key", "rich_text")
    diet_prop = _find_prop(schema, "diet", "multi_select")
    allergens_prop = _find_prop(schema, "allergens", "multi_select")
    hard_no_prop = _find_prop(schema, "hard_no", "multi_select")
    out: Dict[str, Dict[str, Any]] = {}
    for pid in sorted(set(player_page_ids)):
        if not pid:
            continue
        try:
            page = _execute_with_retry(repo.client.pages.retrieve, page_id=pid)
        except Exception:
            continue
        props = page.get("properties", {}) if isinstance(page, dict) else {}
        diet_vals = []
        allerg_vals = []
        hard_no_vals = []
        for raw in ((props.get(diet_prop) or {}).get("multi_select", []) if isinstance(props.get(diet_prop), dict) else []):
            if isinstance(raw, dict) and raw.get("name"):
                diet_vals.append(str(raw.get("name")))
        for raw in ((props.get(allergens_prop) or {}).get("multi_select", []) if isinstance(props.get(allergens_prop), dict) else []):
            if isinstance(raw, dict) and raw.get("name"):
                allerg_vals.append(str(raw.get("name")))
        for raw in ((props.get(hard_no_prop) or {}).get("multi_select", []) if isinstance(props.get(hard_no_prop), dict) else []):
            if isinstance(raw, dict) and raw.get("name"):
                hard_no_vals.append(str(raw.get("name")))
        out[pid] = {
            "id": pid,
            "nickname": _rt(props, nickname_prop) or _title(props, title_prop) or pid[:8],
            "access_key": _rt(props, access_prop),
            "diet": diet_vals,
            "allergens": allerg_vals,
            "hard_no": hard_no_vals,
        }
    return out


def _session_summary(repo: Any, session: Dict[str, Any]) -> Dict[str, Any]:
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
            response_diet.extend(_as_list_labels((qmap.get(qid) or {}).get("value")))
        for qid in allerg_qids:
            response_allergens.extend(_as_list_labels((qmap.get(qid) or {}).get("value")))
        for qid in hard_no_qids:
            response_hard_no.extend(_as_list_labels((qmap.get(qid) or {}).get("value")))

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
            contribution = _parse_numeric_value(row.get("value_number"))
            if contribution is None:
                contribution = _parse_numeric_value(row.get("value"))
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
    set_page()
    apply_theme()
    ensure_session_state()
    repo = get_notion_repo()
    authenticator = get_authenticator(repo)
    ensure_auth(authenticator, callback=remember_access, key="overview-login")
    ensure_session_context(repo)
    require_login()

    st.title("Overview · Cuisine")
    st.caption("Aggregate responses per session: kitchen preferences and prospective contributions.")

    if not repo:
        st.error("Notion repository unavailable.")
        st.stop()

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
