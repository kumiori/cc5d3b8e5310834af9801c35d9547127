from __future__ import annotations

from datetime import datetime, timedelta, timezone
import logging
from typing import Any, Optional, Tuple

from infra.app_context import get_notion_repo, load_config, reset_notion_repo_cache
from infra.notion_repo import _execute_with_retry, _resolve_data_source_id

LOGGER = logging.getLogger("affranchis.presence")


def _date_prop_name(repo, db_id: str) -> Optional[str]:
    for candidate in ["last_seen", "last_joined_on"]:
        try:
            if repo._prop_exists(db_id, candidate):  # noqa: SLF001
                return candidate
        except Exception:
            continue
    return None


def _parse_iso(value: str) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        parsed = datetime.fromisoformat(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def touch_player_presence(
    player_id: str,
    page: Optional[str] = None,
    session_slug: Optional[str] = None,
) -> Tuple[bool, str]:
    del page, session_slug
    cfg = load_config() or {}
    presence_cfg = cfg.get("presence") or {}
    if not bool(presence_cfg.get("enabled", True)):
        return True, ""
    if not bool(presence_cfg.get("update_last_seen_on_interaction", True)):
        return True, ""
    repo = get_notion_repo()
    if not repo:
        return False, "Notion repository unavailable."
    if not player_id:
        return False, "Missing player id."

    db_id = repo.players_db_id
    if not db_id:
        return False, "Players database id missing."
    date_prop = _date_prop_name(repo, db_id)
    if not date_prop:
        # Schema might be stale right after adding new columns in Notion.
        reset_notion_repo_cache()
        repo = get_notion_repo()
        if not repo:
            return False, "Notion repository unavailable after cache reset."
        db_id = repo.players_db_id
        date_prop = _date_prop_name(repo, db_id)
    if not date_prop:
        return False, "Players schema missing date property ('last_seen' or 'last_joined_on')."

    target_player_id = player_id
    if "-" not in target_player_id:
        player = repo.get_player_by_id(target_player_id)
        resolved = (player or {}).get("id")
        if not resolved:
            return False, "Could not resolve player page id."
        target_player_id = str(resolved)

    try:
        now_iso = datetime.now(timezone.utc).isoformat()
        _execute_with_retry(
            repo.client.pages.update,
            page_id=target_player_id,
            properties={date_prop: {"date": {"start": now_iso}}},
        )
        LOGGER.info("presence.touch player_id=%s prop=%s", target_player_id, date_prop)
        return True, ""
    except Exception as exc:
        LOGGER.warning("presence.touch_failed player_id=%s error=%s", target_player_id, exc)
        return False, str(exc)


def count_active_users(window_minutes: int, session_id: Optional[str] = None) -> int:
    cfg = load_config() or {}
    if not bool((cfg.get("presence") or {}).get("enabled", True)):
        return 0
    repo = get_notion_repo()
    if not repo:
        return 0
    db_id = repo.players_db_id
    if not db_id:
        return 0
    date_prop = _date_prop_name(repo, db_id)
    if not date_prop:
        reset_notion_repo_cache()
        repo = get_notion_repo()
        if not repo:
            return 0
        db_id = repo.players_db_id
        date_prop = _date_prop_name(repo, db_id)
    if not date_prop:
        LOGGER.warning("presence.count no date prop found on players db")
        return 0

    try:
        ds_id = _resolve_data_source_id(repo.client, db_id)
        session_prop = (
            repo._prop_name(db_id, "session", "relation")  # noqa: SLF001
            if session_id and repo._prop_exists(db_id, "session")  # noqa: SLF001
            else None
        )
        filters: list[dict[str, Any]] = [
            {
                "property": date_prop,
                "date": {
                    "on_or_after": (
                        datetime.now(timezone.utc)
                        - timedelta(minutes=max(1, window_minutes))
                    ).isoformat()
                },
            }
        ]
        if session_id and session_prop:
            filters.append({"property": session_prop, "relation": {"contains": session_id}})

        query: dict[str, Any] = {
            "data_source_id": ds_id,
            "filter": {"and": filters} if len(filters) > 1 else filters[0],
            "page_size": 100,
        }
        count = 0
        while True:
            payload = _execute_with_retry(repo.client.data_sources.query, **query)
            for page in payload.get("results", []):
                props = page.get("properties", {})
                seen = (props.get(date_prop) or {}).get("date", {})
                seen_dt = _parse_iso((seen or {}).get("start", ""))
                if seen_dt:
                    count += 1
            if not payload.get("has_more"):
                break
            query["start_cursor"] = payload.get("next_cursor")
        LOGGER.info("presence.count window_minutes=%s session_id=%s count=%s", window_minutes, session_id, count)
        return count
    except Exception as exc:
        LOGGER.warning("presence.count_failed error=%s", exc)
        return 0
