"""Session catalogue helpers for UI selectors and routing context."""

from __future__ import annotations

import re
from typing import Any, Dict, List

from infra.notion_repo import _execute_with_retry, _resolve_data_source_id, get_database_schema
from services.notion_value_utils import (
    find_prop,
    read_checkbox,
    read_rich_text,
    read_select_name,
    read_title,
)


def list_sessions_for_ui(repo: Any, *, limit: int = 300) -> List[Dict[str, Any]]:
    """Load sessions with stable labels, preferring human-readable names."""
    sessions_db_id = str(getattr(repo, "session_db_id", "") or "")
    if not sessions_db_id:
        return []
    ds_id = _resolve_data_source_id(repo.client, sessions_db_id)
    if not ds_id:
        return []

    schema = get_database_schema(repo.client, sessions_db_id)
    code_rt = find_prop(schema, "session_code", "rich_text")
    code_title = find_prop(schema, "session_code", "title")
    name_rt = find_prop(schema, "session_name", "rich_text")
    name_title = find_prop(schema, "session_name", "title")
    title_rt = find_prop(schema, "session_title", "rich_text")
    title_title = find_prop(schema, "session_title", "title")
    default_title = find_prop(schema, "Name", "title")
    status_prop = find_prop(schema, "status", "select")
    mode_prop = find_prop(schema, "mode", "select")
    active_prop = find_prop(schema, "active", "checkbox")

    rows: List[Dict[str, Any]] = []
    query: Dict[str, Any] = {
        "data_source_id": ds_id,
        "page_size": min(100, max(1, limit)),
        "sorts": [{"timestamp": "created_time", "direction": "descending"}],
    }
    while True:
        payload = _execute_with_retry(repo.client.data_sources.query, **query)
        for page in payload.get("results", []):
            props = page.get("properties", {}) if isinstance(page, dict) else {}
            raw_code = read_rich_text(props, code_rt) or read_title(props, code_title)
            raw_name = read_rich_text(props, name_rt) or read_title(props, name_title)
            raw_title = read_rich_text(props, title_rt) or read_title(props, title_title)
            raw_default = read_title(props, default_title)
            code_value = raw_code or raw_name or raw_title or raw_default or "session"
            if raw_code and re.fullmatch(r"\d{6,}", raw_code.strip() or ""):
                code_value = raw_name or raw_title or raw_default or raw_code
            status = read_select_name(props, status_prop)
            mode = read_select_name(props, mode_prop)
            active = read_checkbox(props, active_prop)
            sid = str(page.get("id") or "")
            label_parts = [code_value]
            if mode:
                label_parts.append(mode)
            if status:
                label_parts.append(status)
            if active:
                label_parts.append("active")
            label_parts.append(sid[:8] if sid else "—")
            rows.append(
                {
                    "id": sid,
                    "session_code": code_value,
                    "status": status,
                    "mode": mode,
                    "active": active,
                    "label": " · ".join([part for part in label_parts if part]),
                }
            )
        if not payload.get("has_more") or len(rows) >= limit:
            break
        query["start_cursor"] = payload.get("next_cursor")

    unique: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        sid = str(row.get("id") or "")
        if sid and sid not in unique:
            unique[sid] = row
    return list(unique.values())
