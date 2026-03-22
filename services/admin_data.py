from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import streamlit as st

from infra.notion_repo import _execute_with_retry, _resolve_data_source_id
from services.admin_cache import get_cached_value, invalidate_cache_prefix
from services.admin_metrics import compute_players_metrics

CONTACT_METHOD_ITEM_ID = "CONTACT_METHOD"


def _rt(value: Any) -> str:
    if not isinstance(value, dict):
        return ""
    parts = value.get("rich_text", [])
    return "".join(part.get("plain_text", "") for part in parts if isinstance(part, dict))


def _textish(value: Any) -> str:
    if not isinstance(value, dict):
        return ""
    kind = value.get("type")
    if kind == "rich_text":
        return _rt(value)
    if kind == "title":
        return _title(value)
    if kind == "select":
        return _select(value)
    if kind == "number":
        raw = _number(value)
        return "" if raw is None else str(raw)
    return ""


def _title(value: Any) -> str:
    if not isinstance(value, dict):
        return ""
    parts = value.get("title", [])
    return "".join(part.get("plain_text", "") for part in parts if isinstance(part, dict))


def _select(value: Any) -> str:
    if not isinstance(value, dict):
        return ""
    data = value.get("select") or {}
    return str(data.get("name") or "")


def _date(value: Any) -> str:
    if not isinstance(value, dict):
        return ""
    data = value.get("date") or {}
    return str(data.get("start") or "")


def _checkbox(value: Any, default: bool = False) -> bool:
    if not isinstance(value, dict):
        return default
    if value.get("type") != "checkbox":
        return default
    return bool(value.get("checkbox"))


def _number(value: Any) -> float | None:
    if not isinstance(value, dict):
        return None
    if value.get("type") != "number":
        return None
    raw = value.get("number")
    if raw is None:
        return None
    try:
        return float(raw)
    except Exception:
        return None


def _relations(value: Any) -> List[str]:
    if not isinstance(value, dict):
        return []
    rels = value.get("relation", [])
    return [str(item.get("id")) for item in rels if isinstance(item, dict) and item.get("id")]


def _pick(props: Dict[str, Any], preferred: List[str], ptype: str) -> Any:
    for name in preferred:
        candidate = props.get(name)
        if isinstance(candidate, dict) and candidate.get("type") == ptype:
            return candidate
    for candidate in props.values():
        if isinstance(candidate, dict) and candidate.get("type") == ptype:
            return candidate
    return {}


@st.cache_data(ttl=30, show_spinner=False)
def _load_question_counts(_repo: Any, limit: int = 500) -> Dict[str, int]:
    questions_db_id = str(getattr(_repo, "questions_db_id", "") or "")
    if not questions_db_id:
        return {}
    ds_id = _resolve_data_source_id(_repo.client, questions_db_id)
    if not ds_id:
        return {}
    props = _repo._db_props(questions_db_id)  # noqa: SLF001
    session_prop = "session" if "session" in props else _repo._prop_name(questions_db_id, "session", "relation")  # noqa: SLF001
    counts: Dict[str, int] = {}
    query: Dict[str, Any] = {"data_source_id": ds_id, "page_size": min(100, max(1, limit))}
    while True:
        payload = _execute_with_retry(_repo.client.data_sources.query, **query)
        for page in payload.get("results", []):
            page_props = page.get("properties", {})
            session_ids = _relations(page_props.get(session_prop))
            for sid in session_ids:
                counts[sid] = counts.get(sid, 0) + 1
        if not payload.get("has_more"):
            break
        query["start_cursor"] = payload.get("next_cursor")
    return counts


@st.cache_data(ttl=30, show_spinner=False)
def _load_sessions(_repo: Any, limit: int) -> List[Dict[str, Any]]:
    question_counts = _load_question_counts(_repo)
    sessions = _repo.list_sessions(limit=limit)
    output: List[Dict[str, Any]] = []
    for item in sessions:
        output.append(
            {
                "id": str(item.get("id") or ""),
                "session_code": str(item.get("session_code") or ""),
                "session_name": str(item.get("session_name") or item.get("session_code") or ""),
                "session_title": str(item.get("session_title") or item.get("session_code") or ""),
                "session_order": int(item.get("session_order") or item.get("round_index") or 0),
                "active": bool(item.get("active")),
                "status": str(item.get("status") or ""),
                "mode": str(item.get("mode") or ""),
                "question_count": int(question_counts.get(str(item.get("id") or ""), 0)),
            }
        )
    return output


def get_sessions(repo: Any, *, limit: int = 200) -> List[Dict[str, Any]]:
    if not repo:
        return []
    return _load_sessions(repo, limit)


def _fetch_all_players(repo: Any, *, limit: int = 500) -> List[Dict[str, Any]]:
    db_id = str(getattr(repo, "players_db_id", "") or "")
    if not db_id:
        return []
    ds_id = _resolve_data_source_id(repo.client, db_id)
    if not ds_id:
        return []
    out: List[Dict[str, Any]] = []
    query: Dict[str, Any] = {
        "data_source_id": ds_id,
        "page_size": min(100, max(1, limit)),
        "sorts": [{"timestamp": "last_edited_time", "direction": "descending"}],
    }
    while True:
        payload = _execute_with_retry(repo.client.data_sources.query, **query)
        for page in payload.get("results", []):
            props = page.get("properties", {}) if isinstance(page, dict) else {}
            access_key = _rt(_pick(props, ["access_key", "player_id"], "rich_text"))
            nickname = _rt(_pick(props, ["nickname"], "rich_text")) or _title(
                _pick(props, ["nickname_title", "Name"], "title")
            )
            role = _select(_pick(props, ["role", "status"], "select"))
            email = _rt(_pick(props, ["email", "Email", "contact_email"], "rich_text"))
            last_joined_on = _date(_pick(props, ["last_seen", "last_joined_on"], "date"))
            created_at = str(page.get("created_time") or "")
            out.append(
                {
                    "id": str(page.get("id") or ""),
                    "access_key": access_key,
                    "nickname": nickname,
                    "role": role or "Contributor",
                    "email": email,
                    "last_joined_on": last_joined_on,
                    "created_at": created_at,
                    "last_activity": last_joined_on or str(page.get("last_edited_time") or ""),
                    "contact_preference": "",
                    "contact_value": "",
                    "session_ids": _relations(_pick(props, ["session"], "relation")),
                    "consent_contact": _checkbox(_pick(props, ["consent_contact"], "checkbox"), False),
                }
            )
        if not payload.get("has_more") or len(out) >= limit:
            break
        query["start_cursor"] = payload.get("next_cursor")
    return out[:limit]


@st.cache_data(ttl=20, show_spinner=False)
def _load_players(_repo: Any, limit: int) -> List[Dict[str, Any]]:
    return _fetch_all_players(_repo, limit=limit)


def get_players(repo: Any, *, limit: int = 500, force_refresh: bool = False) -> List[Dict[str, Any]]:
    if not repo:
        return []
    return get_cached_value(
        f"players:{limit}",
        lambda: _load_players(repo, limit),
        ttl_seconds=20.0,
        force_refresh=force_refresh,
    )


@st.cache_data(ttl=20, show_spinner=False)
def _load_contact_preferences(_repo: Any, session_id: str) -> List[Dict[str, Any]]:
    responses_db_id = str(getattr(_repo, "responses_db_id", "") or "")
    if not responses_db_id or not session_id:
        return []
    ds_id = _resolve_data_source_id(_repo.client, responses_db_id)
    if not ds_id:
        return []
    props = _repo._db_props(responses_db_id)  # noqa: SLF001
    item_prop = "item_id" if "item_id" in props else _repo._prop_name(responses_db_id, "item_id", "rich_text")  # noqa: SLF001
    session_prop = "session" if "session" in props else _repo._prop_name(responses_db_id, "session", "relation")  # noqa: SLF001
    player_prop = "player" if "player" in props else _repo._prop_name(responses_db_id, "player", "relation")  # noqa: SLF001
    value_prop = "value" if "value" in props else _repo._prop_name(responses_db_id, "value", "rich_text")  # noqa: SLF001
    value_label_prop = (
        "value_label"
        if "value_label" in props
        else _repo._prop_name(responses_db_id, "value_label", "rich_text")  # noqa: SLF001
    )
    question_type_prop = (
        "question_type"
        if "question_type" in props
        else _repo._prop_name(responses_db_id, "question_type", "rich_text")  # noqa: SLF001
    )
    query: Dict[str, Any] = {
        "data_source_id": ds_id,
        "filter": {
            "and": [
                {"property": session_prop, "relation": {"contains": session_id}},
                {"property": item_prop, "rich_text": {"equals": CONTACT_METHOD_ITEM_ID}},
            ]
        },
        "sorts": [{"timestamp": "created_time", "direction": "descending"}],
        "page_size": 100,
    }
    out: List[Dict[str, Any]] = []
    while True:
        payload = _execute_with_retry(_repo.client.data_sources.query, **query)
        for page in payload.get("results", []):
            page_props = page.get("properties", {})
            player_ids = _relations(page_props.get(player_prop))
            out.append(
                {
                    "id": str(page.get("id") or ""),
                    "session_id": session_id,
                    "player_id": player_ids[0] if player_ids else "",
                    "item_id": _rt(page_props.get(item_prop)),
                    "value_json": _textish(page_props.get(value_prop)),
                    "value_label": _textish(page_props.get(value_label_prop)),
                    "question_type": _textish(page_props.get(question_type_prop)),
                    "created_at": str(page.get("created_time") or ""),
                }
            )
        if not payload.get("has_more"):
            break
        query["start_cursor"] = payload.get("next_cursor")
    return out


def get_contact_preferences(
    repo: Any,
    *,
    session_id: str,
    force_refresh: bool = False,
) -> List[Dict[str, Any]]:
    if not repo or not session_id:
        return []
    return get_cached_value(
        f"contact_preferences:{session_id}",
        lambda: _load_contact_preferences(repo, session_id),
        ttl_seconds=20.0,
        force_refresh=force_refresh,
    )


def build_players_dashboard_rows(
    players: List[Dict[str, Any]],
    contact_preferences: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    latest_by_player: dict[str, Dict[str, Any]] = {}
    for item in sorted(contact_preferences, key=lambda x: str(x.get("created_at") or ""), reverse=True):
        pid = str(item.get("player_id") or "").strip()
        if pid and pid not in latest_by_player:
            latest_by_player[pid] = item

    rows: List[Dict[str, Any]] = []
    for player in players:
        pid = str(player.get("id") or "")
        pref = latest_by_player.get(pid, {})
        value_label = str(pref.get("value_label") or "").strip()
        value_json = str(pref.get("value_json") or "").strip()
        rows.append(
            {
                "name": player.get("nickname") or "",
                "role": player.get("role") or "",
                "email": player.get("email") or "",
                "last_activity": player.get("last_activity") or player.get("last_joined_on") or "",
                "contact_preference": value_label or value_json,
                "contact_preference_set_at": pref.get("created_at") or "",
                "player_id": pid,
                "access_key": player.get("access_key") or "",
            }
        )
    return rows, compute_players_metrics(players, contact_preferences)


def build_player_name_snapshot(players: List[Dict[str, Any]]) -> List[str]:
    names = [str(player.get("nickname") or "").strip() for player in players]
    return sorted([name for name in names if name])


def clear_admin_caches() -> None:
    _load_sessions.clear()
    _load_question_counts.clear()
    _load_players.clear()
    _load_contact_preferences.clear()
    invalidate_cache_prefix("players:")
    invalidate_cache_prefix("contact_preferences:")
    invalidate_cache_prefix("sessions:")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
