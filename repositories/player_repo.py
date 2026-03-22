from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from infra.notion_repo import _execute_with_retry, _resolve_data_source_id


class PlayerRepository:
    def __init__(self, notion_repo: Any):
        self.repo = notion_repo

    def list_all_players(self, *, limit: int = 500) -> List[Dict[str, Any]]:
        if not self.repo:
            return []
        db_id = getattr(self.repo, "players_db_id", "")
        if not db_id:
            return []
        ds_id = _resolve_data_source_id(self.repo.client, db_id)
        if not ds_id:
            return []
        query = {
            "data_source_id": ds_id,
            "page_size": min(100, max(1, limit)),
            "sorts": [{"timestamp": "last_edited_time", "direction": "descending"}],
        }
        out: List[Dict[str, Any]] = []
        while True:
            payload = _execute_with_retry(self.repo.client.data_sources.query, **query)
            out.extend([self.repo._normalize_player(page) for page in payload.get("results", [])])  # noqa: SLF001
            if not payload.get("has_more") or len(out) >= limit:
                break
            query["start_cursor"] = payload.get("next_cursor")
        return out[:limit]

    def get_player_by_id(self, player_id: str) -> Optional[Dict[str, Any]]:
        if not self.repo:
            return None
        return self.repo.get_player_by_id(player_id)

    def get_player_by_access_key(self, access_key: str) -> Optional[Dict[str, Any]]:
        if not self.repo:
            return None
        return self.repo.get_player_by_id(access_key)

    def update_player_role(self, player_id: str, role: str) -> Dict[str, Any]:
        if not self.repo:
            raise RuntimeError("Notion repository unavailable.")
        db_id = getattr(self.repo, "players_db_id", "")
        if not db_id:
            raise RuntimeError("Players database id missing.")
        role_prop = self.repo._prop_name(db_id, "role", "select")  # noqa: SLF001
        page = _execute_with_retry(
            self.repo.client.pages.update,
            page_id=player_id,
            properties={role_prop: {"select": {"name": role}}},
        )
        return self.repo._normalize_player(page, players_db_id=db_id)  # noqa: SLF001

    def touch_last_seen(self, player_id: str) -> None:
        if not self.repo:
            raise RuntimeError("Notion repository unavailable.")
        db_id = getattr(self.repo, "players_db_id", "")
        if not db_id:
            raise RuntimeError("Players database id missing.")
        if self.repo._prop_exists(db_id, "last_seen"):  # noqa: SLF001
            prop_name = "last_seen"
        elif self.repo._prop_exists(db_id, "last_joined_on"):  # noqa: SLF001
            prop_name = "last_joined_on"
        else:
            raise RuntimeError("No last seen field found on players schema.")
        now_iso = datetime.now(timezone.utc).isoformat()
        _execute_with_retry(
            self.repo.client.pages.update,
            page_id=player_id,
            properties={prop_name: {"date": {"start": now_iso}}},
        )
