from __future__ import annotations

from typing import Any, Dict, List, Optional


class SessionRepository:
    def __init__(self, notion_repo: Any):
        self.repo = notion_repo

    def list_sessions(self, *, limit: int = 200) -> List[Dict[str, Any]]:
        if not self.repo:
            return []
        return self.repo.list_sessions(limit=limit)

    def update_session_active(self, session_id: str, active: bool) -> Dict[str, Any]:
        if not self.repo:
            raise RuntimeError("Notion repository unavailable.")
        return self.repo.update_session(session_id, active=active)

    def update_session_metadata(
        self,
        session_id: str,
        *,
        session_name: Optional[str] = None,
        session_title: Optional[str] = None,
        session_description: Optional[str] = None,
        session_order: Optional[int] = None,
        session_visualisation: Optional[str] = None,
    ) -> Dict[str, Any]:
        if not self.repo:
            raise RuntimeError("Notion repository unavailable.")
        fields: Dict[str, Any] = {}
        if session_name is not None:
            fields["session_code"] = session_name
        if session_title is not None:
            fields["notes"] = session_title
        if session_description is not None:
            fields["notes"] = session_description
        if session_order is not None:
            fields["round_index"] = int(session_order)
        if session_visualisation is not None:
            fields["mode"] = session_visualisation
        if not fields:
            return self.repo.get_session(session_id) or {}
        return self.repo.update_session(session_id, **fields)
