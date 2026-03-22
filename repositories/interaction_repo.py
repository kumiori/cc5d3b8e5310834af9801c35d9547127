from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from infra.notion_repo import get_database_schema
from repositories.base import InteractionRepository


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _extract_rich_text(props: Dict[str, Any], name: str) -> str:
    value = props.get(name) or {}
    parts = value.get("rich_text", []) if isinstance(value, dict) else []
    return "".join(part.get("plain_text", "") for part in parts if isinstance(part, dict))


def _resolve_data_source_id(client: Any, db_or_ds_id: str) -> str:
    data_sources_endpoint = getattr(client, "data_sources", None)
    ds_retrieve = getattr(data_sources_endpoint, "retrieve", None) if data_sources_endpoint else None
    if callable(ds_retrieve):
        try:
            ds_retrieve(db_or_ds_id)
            return db_or_ds_id
        except Exception:
            pass

    databases_endpoint = getattr(client, "databases", None)
    db_retrieve = getattr(databases_endpoint, "retrieve", None) if databases_endpoint else None
    if callable(db_retrieve):
        db = db_retrieve(db_or_ds_id)
        data_sources = db.get("data_sources", []) if isinstance(db, dict) else []
        if data_sources and isinstance(data_sources[0], dict):
            ds_id = data_sources[0].get("id")
            if ds_id:
                return str(ds_id)
    return db_or_ds_id


class NotionInteractionRepository(InteractionRepository):
    def __init__(self, notion_repo: Any, database_id: str):
        if not notion_repo or not database_id:
            raise ValueError("Notion repo and database id are required.")
        self.repo = notion_repo
        self.client = notion_repo.client
        self.database_id = database_id
        self.data_source_id = _resolve_data_source_id(self.client, database_id)
        self._properties = get_database_schema(self.client, self.database_id)
        if not self._find_prop("session", "relation"):
            raise ValueError("Interaction Notion DB must include a 'session' relation property.")
        if not self._find_prop("item_id", "rich_text"):
            raise ValueError("Interaction Notion DB must include an 'item_id' rich_text property.")
        if not self._find_prop("value", "rich_text"):
            raise ValueError("Interaction Notion DB must include a 'value' rich_text property.")

    def _find_prop(self, expected: str, ptype: Optional[str] = None) -> Optional[str]:
        if expected in self._properties:
            return expected
        if ptype:
            for name, meta in self._properties.items():
                if isinstance(meta, dict) and meta.get("type") == ptype:
                    return str(name)
        return None

    def save_response(
        self,
        session_id: str,
        player_id: Optional[str],
        question_id: str,
        value: Any,
        text_id: str,
        device_id: str,
    ) -> None:
        session_prop = self._find_prop("session", "relation")
        player_prop = self._find_prop("player", "relation")
        item_prop = self._find_prop("item_id", "rich_text")
        value_prop = self._find_prop("value", "rich_text")
        created_prop = self._find_prop("created_at", "date")
        text_prop = self._find_prop("text_id", "rich_text")
        device_prop = self._find_prop("device_id", "rich_text")
        title_prop = self._find_prop("Name", "title")

        properties: Dict[str, Any] = {}
        if session_prop:
            properties[session_prop] = {"relation": [{"id": session_id}]}
        if player_prop:
            properties[player_prop] = {"relation": [{"id": player_id}]} if player_id else {"relation": []}
        if item_prop:
            properties[item_prop] = {"rich_text": [{"type": "text", "text": {"content": question_id}}]}
        if value_prop:
            properties[value_prop] = {
                "rich_text": [
                    {"type": "text", "text": {"content": json.dumps(value, ensure_ascii=False)}}
                ]
            }
        if created_prop:
            properties[created_prop] = {"date": {"start": _now_iso()}}
        if text_prop:
            properties[text_prop] = {"rich_text": [{"type": "text", "text": {"content": text_id}}]}
        if device_prop:
            properties[device_prop] = {"rich_text": [{"type": "text", "text": {"content": device_id}}]}
        if title_prop:
            properties[title_prop] = {"title": [{"type": "text", "text": {"content": f"{question_id} · {text_id}"}}]}

        self.client.pages.create(
            parent={"database_id": self.database_id},
            properties=properties,
        )

    def get_responses(self, session_id: str) -> List[Dict[str, Any]]:
        session_prop = self._find_prop("session", "relation")
        item_prop = self._find_prop("item_id", "rich_text")
        value_prop = self._find_prop("value", "rich_text")
        created_prop = self._find_prop("created_at", "date")
        text_prop = self._find_prop("text_id", "rich_text")
        player_prop = self._find_prop("player", "relation")
        if not session_prop or not item_prop or not value_prop:
            return []

        out: List[Dict[str, Any]] = []
        next_cursor: Optional[str] = None
        while True:
            query: Dict[str, Any] = {
                "data_source_id": self.data_source_id,
                "filter": {"property": session_prop, "relation": {"contains": session_id}},
                "page_size": 100,
            }
            if next_cursor:
                query["start_cursor"] = next_cursor
            payload = self.client.data_sources.query(**query)
            for page in payload.get("results", []):
                props = page.get("properties", {})
                raw_value = _extract_rich_text(props, value_prop)
                parsed: Any
                try:
                    parsed = json.loads(raw_value)
                except Exception:
                    parsed = raw_value
                player_ids = []
                pval = props.get(player_prop) if player_prop else None
                if isinstance(pval, dict):
                    player_ids = [x.get("id") for x in pval.get("relation", []) if isinstance(x, dict)]
                created = None
                if created_prop and isinstance(props.get(created_prop), dict):
                    created = (props.get(created_prop) or {}).get("date", {}).get("start")
                out.append(
                    {
                        "session_id": session_id,
                        "player_id": player_ids[0] if player_ids else None,
                        "item_id": _extract_rich_text(props, item_prop),
                        "value": parsed,
                        "text_id": _extract_rich_text(props, text_prop) if text_prop else "",
                        "created_at": created or page.get("created_time"),
                    }
                )
            if not payload.get("has_more"):
                break
            next_cursor = payload.get("next_cursor")
        return out

    def get_responses_by_item(self, session_id: str, item_id: str) -> List[Dict[str, Any]]:
        return [row for row in self.get_responses(session_id) if str(row.get("item_id") or "") == item_id]


class SQLiteInteractionRepository(InteractionRepository):
    def __init__(self, sqlite_path: str):
        self.path = Path(sqlite_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS responses (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL,
                    player_id TEXT,
                    device_id TEXT,
                    text_id TEXT,
                    item_id TEXT NOT NULL,
                    value_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.commit()

    def save_response(
        self,
        session_id: str,
        player_id: Optional[str],
        question_id: str,
        value: Any,
        text_id: str,
        device_id: str,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO responses(session_id, player_id, device_id, text_id, item_id, value_json, created_at)
                VALUES(?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    session_id,
                    player_id,
                    device_id,
                    text_id,
                    question_id,
                    json.dumps(value, ensure_ascii=False),
                    _now_iso(),
                ),
            )
            conn.commit()

    def get_responses(self, session_id: str) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT session_id, player_id, text_id, item_id, value_json, created_at
                FROM responses
                WHERE session_id = ?
                ORDER BY created_at DESC
                """,
                (session_id,),
            ).fetchall()
        out: List[Dict[str, Any]] = []
        for row in rows:
            raw = row["value_json"] or ""
            try:
                parsed = json.loads(raw)
            except Exception:
                parsed = raw
            out.append(
                {
                    "session_id": row["session_id"],
                    "player_id": row["player_id"],
                    "item_id": row["item_id"],
                    "value": parsed,
                    "text_id": row["text_id"] or "",
                    "created_at": row["created_at"],
                }
            )
        return out

    def get_responses_by_item(self, session_id: str, item_id: str) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT session_id, player_id, text_id, item_id, value_json, created_at
                FROM responses
                WHERE session_id = ? AND item_id = ?
                ORDER BY created_at DESC
                """,
                (session_id, item_id),
            ).fetchall()
        out: List[Dict[str, Any]] = []
        for row in rows:
            raw = row["value_json"] or ""
            try:
                parsed = json.loads(raw)
            except Exception:
                parsed = raw
            out.append(
                {
                    "session_id": row["session_id"],
                    "player_id": row["player_id"],
                    "item_id": row["item_id"],
                    "value": parsed,
                    "text_id": row["text_id"] or "",
                    "created_at": row["created_at"],
                }
            )
        return out
