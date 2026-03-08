from __future__ import annotations

import inspect
import os
import re
import time
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional

import streamlit as st

try:
    from streamlit_notion import NotionConnection
except ImportError:  # pragma: no cover
    NotionConnection = None  # type: ignore

try:
    from notion_client import Client
    from notion_client.errors import APIResponseError
except ImportError:  # pragma: no cover - fallback when SDK unavailable
    Client = Any  # type: ignore

    class APIResponseError(Exception):  # type: ignore
        """Fallback error so caller logic still works."""

        status: int = 0


HASH_FUNCS = {}
if isinstance(Client, type):
    HASH_FUNCS[Client] = lambda _: 0


SESSIONS_DB_ID = ""
PLAYERS_DB_ID = ""

DEBUG_NOTION = str(st.secrets.get("notion", {}).get("debug", "")).lower() in {
    "1",
    "true",
    "yes",
}

try:
    from config import settings
except Exception:  # pragma: no cover
    settings = None


def _debug_client(label: str, client: Client) -> None:
    if not DEBUG_NOTION:
        return
    if client is None:
        st.write(f"{label}: client is None")
        return
    st.write(f"{label}: client={client!r}")
    st.write(f"{label}: client_type={type(client)} module={type(client).__module__}")
    data_sources = getattr(client, "data_sources", None)
    st.write(f"{label}: client.data_sources={data_sources!r}")
    st.write(
        f"{label}: client.data_sources_type={type(data_sources)} module={type(data_sources).__module__}"
    )
    query = getattr(data_sources, "query", None) if data_sources is not None else None
    st.write(f"{label}: client.data_sources.query={query!r}")
    query_func = getattr(query, "__func__", query)
    st.write(
        f"{label}: query_type={type(query)} module={getattr(query_func, '__module__', None)}"
    )
    st.write(f"{label}: query_qualname={getattr(query_func, '__qualname__', None)}")
    request = getattr(client, "request", None)
    if request is not None:
        sig = getattr(inspect, "signature", None)
        if sig:
            st.write(f"{label}: client.request signature={sig(request)}")
    base_url = getattr(client, "base_url", None)
    if base_url:
        st.write(f"{label}: client.base_url={base_url!r}")
    inner_client = getattr(client, "client", None)
    inner_base = getattr(inner_client, "base_url", None) if inner_client else None
    if inner_base:
        st.write(f"{label}: client.client.base_url={inner_base!r}")


def _ensure_base_url(client: Client) -> str:
    inner_client = getattr(client, "client", None)
    base_url = getattr(inner_client, "base_url", None) if inner_client else None
    if not base_url and inner_client is not None:
        try:
            inner_client.base_url = "https://api.notion.com/v1/"
            base_url = inner_client.base_url
        except Exception:
            base_url = None
    return str(base_url) if base_url else ""


def _clean_notion_id(value: Optional[str]) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    # Accept direct IDs, UUIDs inside URLs, and quoted values.
    raw = raw.strip("\"'")
    dashed = re.search(
        r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})",
        raw,
    )
    if dashed:
        return dashed.group(1).lower()
    compact = re.search(r"([0-9a-fA-F]{32})", raw)
    if compact:
        token = compact.group(1).lower()
        return (
            f"{token[0:8]}-{token[8:12]}-{token[12:16]}-{token[16:20]}-{token[20:32]}"
        )
    return raw


def _execute_with_retry(func, *args, **kwargs):
    last_error: Optional[Exception] = None
    for attempt in range(3):
        try:
            return func(*args, **kwargs)
        except APIResponseError as err:  # type: ignore[misc]
            last_error = err
            status = getattr(err, "status", None)
            if status != 429 or attempt == 2:
                break
            wait_for = 0.5 * (2**attempt)
            time.sleep(wait_for)
    if last_error:
        raise last_error
    raise RuntimeError("Request failed without APIResponseError.")


@lru_cache(maxsize=128)
def _resolve_data_source_id(client: Client, database_or_source_id: str) -> str:
    clean_id = _clean_notion_id(database_or_source_id)
    if not clean_id:
        return ""

    data_sources_endpoint = getattr(client, "data_sources", None)
    ds_retrieve = (
        getattr(data_sources_endpoint, "retrieve", None)
        if data_sources_endpoint
        else None
    )
    if callable(ds_retrieve):
        try:
            _execute_with_retry(ds_retrieve, clean_id)
            return clean_id
        except Exception:
            pass

    databases_endpoint = getattr(client, "databases", None)
    db_retrieve = (
        getattr(databases_endpoint, "retrieve", None) if databases_endpoint else None
    )
    if callable(db_retrieve):
        try:
            db = _execute_with_retry(db_retrieve, clean_id)
            data_sources = db.get("data_sources", []) if isinstance(db, dict) else []
            if data_sources and isinstance(data_sources[0], dict):
                ds_id = _clean_notion_id(data_sources[0].get("id"))
                if ds_id:
                    return ds_id
        except Exception:
            pass

    return clean_id


@st.cache_data(ttl=5, show_spinner=False, hash_funcs=HASH_FUNCS)
def _cached_query(client: Client, database_id: str, **kwargs) -> Dict[str, Any]:
    db_id = _resolve_data_source_id(client, database_id)
    if not db_id:
        raise ValueError("Missing database/data source id for query.")
    _debug_client("notion.query", client)
    # if settings and getattr(settings, "show_debug", False):
    #     st.write(
    #         f"Debug {settings.show_debug}: NotionRepo querying database {database_id}"
    #     )
    #     st.write(f"NotionRepo: querying database {database_id} with {kwargs}")

    data_sources_endpoint = getattr(client, "data_sources", None)
    ds_query = (
        getattr(data_sources_endpoint, "query", None) if data_sources_endpoint else None
    )
    if callable(ds_query):
        return _execute_with_retry(ds_query, data_source_id=db_id, **kwargs)

    raise AttributeError(
        "Notion client has no data_sources.query (requires notion-client 3.x)"
    )


@st.cache_data(ttl=5, show_spinner=False, hash_funcs=HASH_FUNCS)
def _cached_retrieve(client: Client, database_id: str) -> Dict[str, Any]:
    db_id = _resolve_data_source_id(client, database_id)
    if not db_id:
        raise ValueError("Missing database/data source id for retrieve.")

    data_sources_endpoint = getattr(client, "data_sources", None)
    ds_retrieve = (
        getattr(data_sources_endpoint, "retrieve", None)
        if data_sources_endpoint
        else None
    )
    if callable(ds_retrieve):
        return _execute_with_retry(ds_retrieve, db_id)

    raise AttributeError(
        "Notion client has no data_sources.retrieve (requires notion-client 3.x)"
    )


def _clear_query_cache():
    _cached_query.clear()


def get_database_schema(client: Client, database_id: str) -> Dict[str, Any]:
    """
    Return database/data source properties for notion-client 3.x.
    Falls back to databases.retrieve for clients that expose legacy schema there.
    """
    data = _cached_retrieve(client, database_id)
    props = data.get("properties")
    if isinstance(props, dict):
        return props

    databases_endpoint = getattr(client, "databases", None)
    db_retrieve = (
        getattr(databases_endpoint, "retrieve", None) if databases_endpoint else None
    )
    if callable(db_retrieve):
        db = _execute_with_retry(db_retrieve, _clean_notion_id(database_id))
        db_props = db.get("properties") if isinstance(db, dict) else None
        if isinstance(db_props, dict):
            return db_props
    return {}


class NotionRepo:
    """Thin repository around the API."""

    def __init__(
        self,
        client: Client,
        session_db_id: str = SESSIONS_DB_ID,
        players_db_id: str = "",
        ideas_db_id: str = "",
        links_db_id: str = "",
        outcomes_db_id: str = "",
        resonance_db_id: str = "",
        statements_db_id: str = "",
        responses_db_id: str = "",
        questions_db_id: str = "",
        moderation_votes_db_id: str = "",
        decisions_db_id: str = "",
        highlights_db_id: str = "",
    ):
        self.client = client
        self.session_db_id = session_db_id
        self.players_db_id = players_db_id
        self.ideas_db_id = ideas_db_id
        self.links_db_id = links_db_id
        self.outcomes_db_id = outcomes_db_id
        self.resonance_db_id = resonance_db_id
        self.statements_db_id = statements_db_id
        self.responses_db_id = responses_db_id
        self.questions_db_id = questions_db_id
        self.moderation_votes_db_id = moderation_votes_db_id
        self.decisions_db_id = decisions_db_id
        self.highlights_db_id = highlights_db_id

    # ---- helpers -----------------------------------------------------
    def _db_props(self, database_id: str) -> Dict[str, Any]:
        return get_database_schema(self.client, database_id)

    @lru_cache(maxsize=32)
    def _prop_name(
        self, database_id: str, expected: str, fallback_type: Optional[str] = None
    ) -> str:
        props = self._db_props(database_id)
        if expected in props:
            return expected
        if fallback_type:
            for name, meta in props.items():
                if meta.get("type") == fallback_type:
                    return name
        return expected

    def _prop_exists(self, database_id: str, name: str) -> bool:
        return name in self._db_props(database_id)

    def _build_title(self, name: str, value: str) -> Dict[str, Any]:
        return {name: {"title": [{"type": "text", "text": {"content": value}}]}}

    def _build_rich_text(self, name: str, value: str) -> Dict[str, Any]:
        return {
            name: {
                "rich_text": [{"type": "text", "text": {"content": value or ""}}],
            }
        }

    def _build_rich_text_chunks(
        self, name: str, value: str, chunk_size: int = 2000
    ) -> Dict[str, Any]:
        text = value or ""
        chunks = [
            text[i : i + chunk_size] for i in range(0, len(text), chunk_size)
        ] or [""]
        return {
            name: {
                "rich_text": [
                    {"type": "text", "text": {"content": chunk}} for chunk in chunks
                ],
            }
        }

    def _build_select(self, name: str, value: Optional[str]) -> Dict[str, Any]:
        if not value:
            return {}
        return {name: {"select": {"name": value}}}

    def _build_multi_select(self, name: str, values: List[str]) -> Dict[str, Any]:
        return {name: {"multi_select": [{"name": val} for val in values or []]}}

    def _build_number(self, name: str, value: Optional[float]) -> Dict[str, Any]:
        if value is None:
            return {}
        return {name: {"number": value}}

    def _build_checkbox(self, name: str, value: Optional[bool]) -> Dict[str, Any]:
        if value is None:
            return {}
        return {name: {"checkbox": bool(value)}}

    def _build_relation(self, name: str, ids: List[str]) -> Dict[str, Any]:
        return {name: {"relation": [{"id": rid} for rid in ids if rid]}}

    def _normalize_select(self, props: Dict[str, Any], name: str) -> Optional[str]:
        value = props.get(name)
        if value and value.get("type") == "select":
            selected = value.get("select") or {}
            return selected.get("name")
        return None

    def _normalize_multi_select(self, props: Dict[str, Any], name: str) -> List[str]:
        value = props.get(name)
        if value and value.get("type") == "multi_select":
            return [opt.get("name") for opt in value.get("multi_select", []) if opt]
        return []

    def _normalize_title(self, props: Dict[str, Any], name: str) -> str:
        value = props.get(name)
        if value and value.get("type") == "title":
            parts = value.get("title", [])
            return "".join(part.get("plain_text", "") for part in parts)
        return ""

    def _normalize_rich_text(self, props: Dict[str, Any], name: str) -> str:
        value = props.get(name)
        if value and value.get("type") == "rich_text":
            parts = value.get("rich_text", [])
            return "".join(part.get("plain_text", "") for part in parts)
        return ""

    def _normalize_number(
        self, props: Dict[str, Any], name: str, default: Optional[float] = None
    ) -> Optional[float]:
        value = props.get(name)
        if value and value.get("type") == "number":
            return value.get("number")
        return default

    def _normalize_checkbox(
        self, props: Dict[str, Any], name: str, default: bool = False
    ) -> bool:
        value = props.get(name)
        if value and value.get("type") == "checkbox":
            return bool(value.get("checkbox"))
        return default

    def _normalize_relation_ids(self, props: Dict[str, Any], name: str) -> List[str]:
        value = props.get(name)
        if value and value.get("type") == "relation":
            return [rel.get("id") for rel in value.get("relation", []) if rel.get("id")]
        return []

    # ---- Sessions ----------------------------------------------------
    def _sessions_db_id(self, session_db_id: Optional[str]) -> str:
        return session_db_id or self.session_db_id

    def _players_db_id(self, players_db_id: Optional[str]) -> str:
        return players_db_id or self.players_db_id

    def _ideas_db_id(self, ideas_db_id: Optional[str]) -> str:
        return ideas_db_id or self.ideas_db_id

    def _links_db_id(self, links_db_id: Optional[str]) -> str:
        return links_db_id or self.links_db_id

    def _outcomes_db_id(self, outcomes_db_id: Optional[str]) -> str:
        return outcomes_db_id or self.outcomes_db_id

    def _resonance_db_id(self, resonance_db_id: Optional[str]) -> str:
        return resonance_db_id or self.resonance_db_id

    def _statements_db_id(self, statements_db_id: Optional[str]) -> str:
        return statements_db_id or self.statements_db_id

    def _responses_db_id(self, responses_db_id: Optional[str]) -> str:
        return responses_db_id or self.responses_db_id

    def _questions_db_id(self, questions_db_id: Optional[str]) -> str:
        return questions_db_id or self.questions_db_id

    def _moderation_votes_db_id(self, moderation_votes_db_id: Optional[str]) -> str:
        return moderation_votes_db_id or self.moderation_votes_db_id

    def _decisions_db_id(self, decisions_db_id: Optional[str]) -> str:
        return decisions_db_id or self.decisions_db_id

    def _highlights_db_id(self, highlights_db_id: Optional[str]) -> str:
        return highlights_db_id or self.highlights_db_id

    def create_session(
        self, session_code: str, mode: str, session_db_id: Optional[str] = None
    ) -> Dict[str, Any]:
        db_id = self._sessions_db_id(session_db_id)
        db_props = self._db_props(db_id)
        session_code_prop = (
            "session_code"
            if "session_code" in db_props
            else self._prop_name(db_id, "Name", "title")
        )
        session_code_type = (
            db_props.get(session_code_prop, {}).get("type")
            if isinstance(db_props.get(session_code_prop), dict)
            else None
        )
        status_name = self._prop_name(db_id, "status", "select")
        mode_name = self._prop_name(db_id, "mode", "select")

        properties: Dict[str, Any] = {}
        if session_code_type == "rich_text":
            properties.update(self._build_rich_text(session_code_prop, session_code))
        else:
            properties.update(self._build_title(session_code_prop, session_code))
        properties.update(self._build_select(status_name, "Lobby"))
        properties.update(self._build_select(mode_name, mode))
        if self._prop_exists(db_id, "round_index"):
            round_name = self._prop_name(db_id, "round_index", "number")
            properties.update(self._build_number(round_name, 0))

        response = _execute_with_retry(
            self.client.pages.create,
            parent={"database_id": db_id},
            properties=properties,
        )
        _clear_query_cache()
        return self._normalize_session(response, session_db_id=db_id)

    def get_session_by_code(
        self, session_code: str, session_db_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        db_id = self._sessions_db_id(session_db_id)
        db_props = self._db_props(db_id)
        session_code_prop = (
            "session_code"
            if "session_code" in db_props
            else self._prop_name(db_id, "Name", "title")
        )
        session_code_type = (
            db_props.get(session_code_prop, {}).get("type")
            if isinstance(db_props.get(session_code_prop), dict)
            else None
        )
        code_filter = (
            {"property": session_code_prop, "rich_text": {"equals": session_code}}
            if session_code_type == "rich_text"
            else {"property": session_code_prop, "title": {"equals": session_code}}
        )
        response = _cached_query(
            self.client,
            db_id,
            filter=code_filter,
            page_size=1,
        )
        results = response.get("results", [])
        if not results:
            return None
        return self._normalize_session(results[0], session_db_id=db_id)

    def update_session(
        self, session_id: str, session_db_id: Optional[str] = None, **fields
    ) -> Dict[str, Any]:
        db_id = self._sessions_db_id(session_db_id)
        props = {}
        if "status" in fields:
            name = self._prop_name(db_id, "status", "select")
            props.update(self._build_select(name, fields["status"]))
        if "mode" in fields:
            name = self._prop_name(db_id, "mode", "select")
            props.update(self._build_select(name, fields["mode"]))
        if "round_index" in fields and self._prop_exists(db_id, "round_index"):
            name = self._prop_name(db_id, "round_index", "number")
            props.update(self._build_number(name, fields["round_index"]))
        if "peer_average_visible" in fields and self._prop_exists(
            db_id, "peer_average_visible"
        ):
            props.update(
                self._build_checkbox(
                    "peer_average_visible", fields["peer_average_visible"]
                )
            )
        if "yellow_active" in fields and self._prop_exists(db_id, "yellow_active"):
            props.update(self._build_checkbox("yellow_active", fields["yellow_active"]))
        if not props:
            session = _execute_with_retry(
                self.client.pages.retrieve, page_id=session_id
            )
        else:
            session = _execute_with_retry(
                self.client.pages.update, page_id=session_id, properties=props
            )
            _clear_query_cache()

        return self._normalize_session(session, session_db_id=db_id)

    def list_sessions(
        self, limit: int = 20, session_db_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        db_id = self._sessions_db_id(session_db_id)
        response = _cached_query(
            self.client,
            db_id,
            page_size=limit,
            sorts=[{"timestamp": "created_time", "direction": "descending"}],
        )
        return [
            self._normalize_session(page, session_db_id=db_id)
            for page in response.get("results", [])
        ]

    def list_active_sessions(
        self, session_db_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        db_id = self._sessions_db_id(session_db_id)
        if not self._prop_exists(db_id, "active"):
            return []
        response = _cached_query(
            self.client,
            db_id,
            filter={"property": "active", "checkbox": {"equals": True}},
            page_size=10,
            sorts=[{"timestamp": "created_time", "direction": "descending"}],
        )
        return [
            self._normalize_session(page, session_db_id=db_id)
            for page in response.get("results", [])
        ]

    def get_active_session(
        self, session_db_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        sessions = self.list_active_sessions(session_db_id=session_db_id)
        return sessions[0] if sessions else None

    def _safe_date_prop(
        self, props: Dict[str, Any], name: Optional[str]
    ) -> Optional[str]:
        if not name or not isinstance(props, dict):
            return None
        value = props.get(name)
        if not isinstance(value, dict):
            return None
        date_val = value.get("date")
        if not isinstance(date_val, dict):
            return None
        return date_val.get("start")

    def _normalize_session(
        self, page: Dict[str, Any], session_db_id: Optional[str] = None
    ) -> Dict[str, Any]:
        db_id = self._sessions_db_id(session_db_id)
        if not isinstance(page, dict):
            return {"id": None, "session_code": "", "status": "Lobby"}
        props = page.get("properties") or {}
        if not isinstance(props, dict):
            props = {}
        db_props = self._db_props(db_id)
        session_code_prop = (
            "session_code"
            if "session_code" in db_props
            else self._prop_name(db_id, "Name", "title")
        )
        session_code_type = (
            db_props.get(session_code_prop, {}).get("type")
            if isinstance(db_props.get(session_code_prop), dict)
            else None
        )
        status_name = self._prop_name(db_id, "status", "select")
        mode_name = self._prop_name(db_id, "mode", "select")
        round_name = (
            self._prop_name(db_id, "round_index", "number")
            if self._prop_exists(db_id, "round_index")
            else None
        )
        active_name = "active" if self._prop_exists(db_id, "active") else None
        start_name = "start" if self._prop_exists(db_id, "start") else None
        end_name = "end" if self._prop_exists(db_id, "end") else None
        notes_name = "notes" if self._prop_exists(db_id, "notes") else None
        peer_avg_name = (
            "peer_average_visible"
            if self._prop_exists(db_id, "peer_average_visible")
            else None
        )
        yellow_name = (
            "yellow_active" if self._prop_exists(db_id, "yellow_active") else None
        )

        return {
            "id": page.get("id"),
            "session_code": (
                self._normalize_rich_text(props, session_code_prop)
                if session_code_type == "rich_text"
                else self._normalize_title(props, session_code_prop)
            ),
            "status": self._normalize_select(props, status_name) or "Lobby",
            "mode": self._normalize_select(props, mode_name) or "Non-linear",
            "round_index": int(self._normalize_number(props, round_name, 0) or 0)
            if round_name
            else 0,
            "active": self._normalize_checkbox(props, active_name, False)
            if active_name
            else False,
            "start": self._safe_date_prop(props, start_name),
            "end": self._safe_date_prop(props, end_name),
            "notes": self._normalize_rich_text(props, notes_name) if notes_name else "",
            "peer_average_visible": self._normalize_checkbox(props, peer_avg_name, True)
            if peer_avg_name
            else True,
            "yellow_active": self._normalize_checkbox(props, yellow_name, False)
            if yellow_name
            else False,
            "created_at": page.get("created_time"),
        }

    def get_session(
        self, session_id: str, session_db_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        if not session_id:
            return None
        try:
            page = _execute_with_retry(self.client.pages.retrieve, page_id=session_id)
        except Exception:
            return None
        return self._normalize_session(page, session_db_id=session_db_id)

    # ---- Players -----------------------------------------------------
    def upsert_player(
        self,
        session_id: str,
        player_id: str,
        nickname: str,
        role: str,
        consent_play: bool,
        consent_research: bool,
        preferred_mode: Optional[str] = None,
        emoji: Optional[str] = None,
        phrase: Optional[str] = None,
        emoji_suffix_4: Optional[str] = None,
        emoji_suffix_6: Optional[str] = None,
        players_db_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        db_id = self._players_db_id(players_db_id)
        player = self._find_player_by_id(player_id, players_db_id=db_id)
        is_new = player is None
        props = self._player_properties(
            session_id,
            player_id,
            nickname,
            role,
            consent_play,
            consent_research,
            include_joined=is_new,
            preferred_mode=preferred_mode,
            emoji=emoji,
            phrase=phrase,
            emoji_suffix_4=emoji_suffix_4,
            emoji_suffix_6=emoji_suffix_6,
            players_db_id=db_id,
        )

        if player:
            if not is_new:
                last_joined_prop = (
                    "last_joined_on"
                    if self._prop_exists(db_id, "last_joined_on")
                    else None
                )
                if last_joined_prop:
                    now_iso = datetime.now(timezone.utc).isoformat()
                    props[last_joined_prop] = {"date": {"start": now_iso}}
            page = _execute_with_retry(
                self.client.pages.update, page_id=player["id"], properties=props
            )
        else:
            page = _execute_with_retry(
                self.client.pages.create,
                parent={"database_id": db_id},
                properties=props,
            )
        _clear_query_cache()
        return self._normalize_player(page, players_db_id=db_id)

    def _find_player_by_id(
        self, player_id: str, players_db_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        if not player_id:
            return None
        db_id = self._players_db_id(players_db_id)
        if self._prop_exists(db_id, "access_key"):
            player_id_name = self._prop_name(db_id, "access_key", "rich_text")
            filter_payload = {
                "property": player_id_name,
                "rich_text": {"equals": player_id},
            }
        else:
            player_id_name = self._prop_name(db_id, "player_id", "rich_text")
            filter_payload = {
                "property": player_id_name,
                "rich_text": {"equals": player_id},
            }
        response = _cached_query(
            self.client,
            db_id,
            filter=filter_payload,
            page_size=1,
        )
        results = response.get("results", [])
        if not results:
            return None
        return self._normalize_player(results[0], players_db_id=db_id)

    def _player_properties(
        self,
        session_id: str,
        player_id: str,
        nickname: str,
        role: str,
        consent_play: bool,
        consent_research: bool,
        include_joined: bool = False,
        preferred_mode: Optional[str] = None,
        emoji: Optional[str] = None,
        phrase: Optional[str] = None,
        emoji_suffix_4: Optional[str] = None,
        emoji_suffix_6: Optional[str] = None,
        players_db_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        db_id = self._players_db_id(players_db_id)
        props: Dict[str, Any] = {}
        if self._prop_exists(db_id, "session"):
            session_prop = self._prop_name(db_id, "session", "relation")
            props.update(self._build_relation(session_prop, [session_id]))

        if self._prop_exists(db_id, "access_key"):
            pid_prop = self._prop_name(db_id, "access_key", "rich_text")
        else:
            pid_prop = self._prop_name(db_id, "player_id", "rich_text")
        props.update(self._build_rich_text(pid_prop, player_id))

        if self._prop_exists(db_id, "nickname"):
            props.update(self._build_rich_text("nickname", nickname))
        elif self._prop_exists(db_id, "Name"):
            props.update(self._build_title("Name", nickname or player_id))

        if self._prop_exists(db_id, "role"):
            role_prop = self._prop_name(db_id, "role", "select")
            props.update(self._build_select(role_prop, role))
        elif self._prop_exists(db_id, "status"):
            props.update(self._build_select("status", role))

        consent_prop = "consented" if self._prop_exists(db_id, "consented") else None
        if consent_prop:
            props.update(self._build_checkbox(consent_prop, consent_play))

        if self._prop_exists(db_id, "consent_research"):
            props.update(self._build_checkbox("consent_research", consent_research))

        if emoji and self._prop_exists(db_id, "emoji"):
            props.update(self._build_rich_text("emoji", emoji))
        if phrase and self._prop_exists(db_id, "phrase"):
            props.update(self._build_rich_text("phrase", phrase))
        if emoji_suffix_4 and self._prop_exists(db_id, "emoji_suffix_4"):
            props.update(self._build_rich_text("emoji_suffix_4", emoji_suffix_4))
        if emoji_suffix_6 and self._prop_exists(db_id, "emoji_suffix_6"):
            props.update(self._build_rich_text("emoji_suffix_6", emoji_suffix_6))

        if self._prop_exists(db_id, "nickname_title"):
            props.update(self._build_title("nickname_title", nickname or player_id))

        if preferred_mode and self._prop_exists(db_id, "preferred_mode"):
            props.update(self._build_select("preferred_mode", preferred_mode))

        now_iso = datetime.now(timezone.utc).isoformat()
        if include_joined and self._prop_exists(db_id, "joined_at"):
            props["joined_at"] = {"date": {"start": now_iso}}
        if self._prop_exists(db_id, "last_joined_on"):
            props["last_joined_on"] = {"date": {"start": now_iso}}

        return props

    def list_players(
        self, session_id: str, players_db_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        db_id = self._players_db_id(players_db_id)
        session_prop = self._prop_name(db_id, "session", "relation")
        response = _cached_query(
            self.client,
            db_id,
            filter={
                "property": session_prop,
                "relation": {"contains": session_id},
            },
            page_size=100,
        )
        return [
            self._normalize_player(page, players_db_id=db_id)
            for page in response.get("results", [])
        ]

    def find_players_by_emoji_suffix(
        self, suffix: str, length: int = 4, players_db_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        db_id = self._players_db_id(players_db_id)
        prop_name = "emoji_suffix_4" if length == 4 else "emoji_suffix_6"
        if not self._prop_exists(db_id, prop_name):
            return []
        response = _cached_query(
            self.client,
            db_id,
            filter={"property": prop_name, "rich_text": {"equals": suffix}},
            page_size=10,
        )
        return [
            self._normalize_player(page, players_db_id=db_id)
            for page in response.get("results", [])
        ]

    def _normalize_player(
        self, page: Dict[str, Any], players_db_id: Optional[str] = None
    ) -> Dict[str, Any]:
        db_id = self._players_db_id(players_db_id)
        props = page.get("properties", {})
        if self._prop_exists(db_id, "access_key"):
            pid_prop = self._prop_name(db_id, "access_key", "rich_text")
        else:
            pid_prop = self._prop_name(db_id, "player_id", "rich_text")
        role_prop = self._prop_name(db_id, "role", "select")
        session_prop = self._prop_name(db_id, "session", "relation")

        nickname_val = ""
        if self._prop_exists(db_id, "nickname"):
            nickname_val = self._normalize_rich_text(props, "nickname")
        elif self._prop_exists(db_id, "nickname_title"):
            nickname_val = self._normalize_title(props, "nickname_title")
        elif self._prop_exists(db_id, "Name"):
            nickname_val = self._normalize_title(props, "Name")

        player = {
            "id": page.get("id"),
            "access_key": self._normalize_rich_text(props, pid_prop),
            "nickname": nickname_val,
            "role": self._normalize_select(props, role_prop) or "Contributor",
            "session_ids": self._normalize_relation_ids(props, session_prop)
            if self._prop_exists(db_id, "session")
            else [],
            "consent_play": False,
            "consent_research": False,
            "created_at": page.get("created_time"),
            "joined_at": page.get("created_time"),
            "last_joined_on": page.get("last_edited_time"),
        }
        if self._prop_exists(db_id, "status"):
            player["status"] = self._normalize_select(props, "status")
        if self._prop_exists(db_id, "consented"):
            player["consent_play"] = self._normalize_checkbox(props, "consented", False)
        if self._prop_exists(db_id, "consent_research"):
            player["consent_research"] = self._normalize_checkbox(
                props, "consent_research", False
            )
        if self._prop_exists(db_id, "joined_at"):
            joined_prop = props.get("joined_at")
            if joined_prop and joined_prop.get("type") == "date":
                date_val = joined_prop.get("date") or {}
                player["joined_at"] = date_val.get("start") or player["joined_at"]
        if self._prop_exists(db_id, "last_joined_on"):
            last_prop = props.get("last_joined_on")
            if last_prop and last_prop.get("type") == "date":
                date_val = last_prop.get("date") or {}
                player["last_joined_on"] = (
                    date_val.get("start") or player["last_joined_on"]
                )
        if self._prop_exists(db_id, "preferred_mode"):
            player["preferred_mode"] = self._normalize_select(props, "preferred_mode")
        return player

    def get_player_by_id(
        self, player_id: str, players_db_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Public accessor for a player lookup by UUID/access key."""
        return self._find_player_by_id(player_id, players_db_id=players_db_id)

    # ---- Ideas -------------------------------------------------------
    def create_idea(
        self,
        session_id: str,
        round_index: int,
        author_player_id: str,
        text: str,
        performative_rules: List[str],
        idea_key: str,
        ideas_db_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        db_id = self._ideas_db_id(ideas_db_id)
        session_prop = self._prop_name(db_id, "session", "relation")
        round_prop = self._prop_name(db_id, "round", "number")
        author_prop = self._prop_name(db_id, "author_player_id", "rich_text")
        text_prop = self._prop_name(db_id, "text", "title")
        rule_prop = self._prop_name(db_id, "performative_rule", "multi_select")
        status_prop = self._prop_name(db_id, "status", "select")
        idea_id_prop = self._prop_name(db_id, "idea_id", "rich_text")
        created_on_prop = (
            "created_on" if self._prop_exists(db_id, "created_on") else None
        )

        properties: Dict[str, Any] = {}
        properties.update(self._build_relation(session_prop, [session_id]))
        properties.update(self._build_number(round_prop, round_index))
        properties.update(self._build_rich_text(author_prop, author_player_id))
        properties.update(self._build_title(text_prop, text))
        properties.update(self._build_multi_select(rule_prop, performative_rules))
        properties.update(self._build_select(status_prop, "Live"))
        properties.update(self._build_rich_text(idea_id_prop, idea_key))
        if created_on_prop:
            now_iso = datetime.now(timezone.utc).isoformat()
            properties[created_on_prop] = {"date": {"start": now_iso}}

        page = _execute_with_retry(
            self.client.pages.create,
            parent={"database_id": db_id},
            properties=properties,
        )
        _clear_query_cache()
        return self._normalize_idea(page, ideas_db_id=db_id)

    def list_ideas(
        self, session_id: str, round_index: int, ideas_db_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        db_id = self._ideas_db_id(ideas_db_id)
        session_prop = self._prop_name(db_id, "session", "relation")
        round_prop = self._prop_name(db_id, "round", "number")
        response = _cached_query(
            self.client,
            db_id,
            filter={
                "and": [
                    {"property": session_prop, "relation": {"contains": session_id}},
                    {"property": round_prop, "number": {"equals": round_index}},
                ]
            },
            sorts=[{"timestamp": "created_time", "direction": "ascending"}],
            page_size=100,
        )
        return [
            self._normalize_idea(page, ideas_db_id=db_id)
            for page in response.get("results", [])
        ]

    def update_idea_alive_flags(
        self,
        idea_id: str,
        containerized: bool,
        artifacts: bool,
        pilot: bool,
        feedback: bool,
        ideas_db_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        db_id = self._ideas_db_id(ideas_db_id)
        props = {}
        for db_name, value in [
            ("alive_containerized", containerized),
            ("alive_artifacts", artifacts),
            ("alive_fieldpilot", pilot),
            ("alive_feedback", feedback),
        ]:
            if self._prop_exists(db_id, db_name):
                props.update(self._build_checkbox(db_name, value))
        page = _execute_with_retry(
            self.client.pages.update, page_id=idea_id, properties=props
        )
        _clear_query_cache()
        return self._normalize_idea(page, ideas_db_id=db_id)

    def update_idea_status(
        self, idea_id: str, status: str, ideas_db_id: Optional[str] = None
    ) -> Dict[str, Any]:
        db_id = self._ideas_db_id(ideas_db_id)
        status_prop = self._prop_name(db_id, "status", "select")
        page = _execute_with_retry(
            self.client.pages.update,
            page_id=idea_id,
            properties=self._build_select(status_prop, status),
        )
        _clear_query_cache()
        return self._normalize_idea(page, ideas_db_id=db_id)

    def _normalize_idea(
        self, page: Dict[str, Any], ideas_db_id: Optional[str] = None
    ) -> Dict[str, Any]:
        db_id = self._ideas_db_id(ideas_db_id)
        props = page.get("properties", {})
        text_prop = self._prop_name(db_id, "text", "title")
        rule_prop = self._prop_name(db_id, "performative_rule", "multi_select")
        round_prop = self._prop_name(db_id, "round", "number")
        session_prop = self._prop_name(db_id, "session", "relation")
        author_prop = self._prop_name(db_id, "author_player_id", "rich_text")
        status_prop = self._prop_name(db_id, "status", "select")
        idea_id_prop = self._prop_name(db_id, "idea_id", "rich_text")

        data = {
            "id": page.get("id"),
            "text": self._normalize_title(props, text_prop),
            "performative_rules": self._normalize_multi_select(props, rule_prop),
            "round": int(self._normalize_number(props, round_prop, 0) or 0),
            "session_ids": self._normalize_relation_ids(props, session_prop),
            "author_player_id": self._normalize_rich_text(props, author_prop),
            "status": self._normalize_select(props, status_prop) or "Draft",
            "created_at": page.get("created_time"),
            "idea_key": self._normalize_rich_text(props, idea_id_prop),
            "alive_containerized": self._normalize_checkbox(
                props, "alive_containerized", False
            ),
            "alive_artifacts": self._normalize_checkbox(
                props, "alive_artifacts", False
            ),
            "alive_fieldpilot": self._normalize_checkbox(
                props, "alive_fieldpilot", False
            ),
            "alive_feedback": self._normalize_checkbox(props, "alive_feedback", False),
        }
        return data

    # ---- Votes -------------------------------------------------------
    def create_vote(
        self,
        idea_id: str,
        voter_player_id: str,
        resonance: int,
        peer_shown: bool,
        prompt_shown: bool,
        resonance_db_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        db_id = self._resonance_db_id(resonance_db_id)
        idea_prop = self._prop_name(db_id, "idea", "relation")
        voter_prop = self._prop_name(db_id, "voter_player_id", "rich_text")
        resonance_prop = self._prop_name(db_id, "resonance", "number")
        peer_prop = self._prop_name(db_id, "peer_shown", "checkbox")
        prompt_prop = self._prop_name(db_id, "prompt_shown", "checkbox")

        props: Dict[str, Any] = {}
        props.update(self._build_relation(idea_prop, [idea_id]))
        props.update(self._build_rich_text(voter_prop, voter_player_id))
        props.update(self._build_number(resonance_prop, resonance))
        props.update(self._build_checkbox(peer_prop, peer_shown))
        props.update(self._build_checkbox(prompt_prop, prompt_shown))

        page = _execute_with_retry(
            self.client.pages.create,
            parent={"database_id": db_id},
            properties=props,
        )
        _clear_query_cache()
        return self._normalize_vote(page, resonance_db_id=db_id)

    def list_votes_for_ideas(
        self, idea_ids: List[str], resonance_db_id: Optional[str] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        votes: Dict[str, List[Dict[str, Any]]] = {iid: [] for iid in idea_ids}
        if not idea_ids:
            return votes
        db_id = self._resonance_db_id(resonance_db_id)
        idea_prop = self._prop_name(db_id, "idea", "relation")
        for idea_id in idea_ids:
            response = _cached_query(
                self.client,
                db_id,
                filter={
                    "property": idea_prop,
                    "relation": {"contains": idea_id},
                },
                page_size=100,
            )
            votes[idea_id] = [
                self._normalize_vote(page, resonance_db_id=db_id)
                for page in response.get("results", [])
            ]
        return votes

    def _normalize_vote(
        self, page: Dict[str, Any], resonance_db_id: Optional[str] = None
    ) -> Dict[str, Any]:
        db_id = self._resonance_db_id(resonance_db_id)
        props = page.get("properties", {})
        voter_prop = self._prop_name(db_id, "voter_player_id", "rich_text")
        resonance_prop = self._prop_name(db_id, "resonance", "number")
        peer_prop = self._prop_name(db_id, "peer_shown", "checkbox")
        prompt_prop = self._prop_name(db_id, "prompt_shown", "checkbox")
        idea_prop = self._prop_name(db_id, "idea", "relation")

        idea_rel = self._normalize_relation_ids(props, idea_prop)

        return {
            "id": page.get("id"),
            "idea_id": idea_rel[0] if idea_rel else None,
            "voter_player_id": self._normalize_rich_text(props, voter_prop),
            "resonance": int(self._normalize_number(props, resonance_prop, 0) or 0),
            "peer_shown": self._normalize_checkbox(props, peer_prop, False),
            "prompt_shown": self._normalize_checkbox(props, prompt_prop, False),
            "created_at": page.get("created_time"),
        }

    # ---- Links -------------------------------------------------------
    def list_links(
        self, session_id: str, round_index: int, links_db_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        db_id = self._links_db_id(links_db_id)
        session_prop = self._prop_name(db_id, "session", "relation")
        round_prop = self._prop_name(db_id, "round", "number")
        response = _cached_query(
            self.client,
            db_id,
            filter={
                "and": [
                    {"property": session_prop, "relation": {"contains": session_id}},
                    {"property": round_prop, "number": {"equals": round_index}},
                ]
            },
            page_size=100,
        )
        return [
            self._normalize_link(page, links_db_id=db_id)
            for page in response.get("results", [])
        ]

    def _normalize_link(
        self, page: Dict[str, Any], links_db_id: Optional[str] = None
    ) -> Dict[str, Any]:
        db_id = self._links_db_id(links_db_id)
        props = page.get("properties", {})
        idea_a_prop = self._prop_name(db_id, "idea_a", "relation")
        idea_b_prop = self._prop_name(db_id, "idea_b", "relation")
        type_prop = self._prop_name(db_id, "link_type", "select")
        weight_prop = self._prop_name(db_id, "weight", "number")

        idea_a = self._normalize_relation_ids(props, idea_a_prop)
        idea_b = self._normalize_relation_ids(props, idea_b_prop)

        return {
            "id": page.get("id"),
            "idea_a": idea_a[0] if idea_a else None,
            "idea_b": idea_b[0] if idea_b else None,
            "link_type": self._normalize_select(props, type_prop) or "",
            "weight": self._normalize_number(props, weight_prop, 1) or 1,
            "created_at": page.get("created_time"),
        }

    # ---- Statements -------------------------------------------------
    def create_statement(
        self,
        session_id: str,
        text: str,
        theme: Optional[str],
        order: Optional[int] = None,
        active: bool = True,
        statements_db_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        db_id = self._statements_db_id(statements_db_id)
        session_prop = self._prop_name(db_id, "session", "relation")
        title_prop = self._prop_name(db_id, "Name", "title")
        theme_prop = self._prop_name(db_id, "theme", "select")
        order_prop = self._prop_name(db_id, "order", "number")
        active_prop = "active" if self._prop_exists(db_id, "active") else None

        props: Dict[str, Any] = {}
        props.update(self._build_relation(session_prop, [session_id]))
        props.update(self._build_title(title_prop, text))
        props.update(self._build_select(theme_prop, theme))
        if order_prop:
            props.update(self._build_number(order_prop, order))
        if active_prop:
            props.update(self._build_checkbox(active_prop, active))

        page = _execute_with_retry(
            self.client.pages.create,
            parent={"database_id": db_id},
            properties=props,
        )
        _clear_query_cache()
        return self._normalize_statement(page, statements_db_id=db_id)

    def list_statements(
        self, session_id: str, statements_db_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        db_id = self._statements_db_id(statements_db_id)
        session_prop = self._prop_name(db_id, "session", "relation")
        filters = [{"property": session_prop, "relation": {"contains": session_id}}]
        if self._prop_exists(db_id, "active"):
            filters.append({"property": "active", "checkbox": {"equals": True}})
        response = _cached_query(
            self.client,
            db_id,
            filter={"and": filters} if len(filters) > 1 else filters[0],
            page_size=200,
            sorts=[
                {
                    "property": self._prop_name(db_id, "order", "number"),
                    "direction": "ascending",
                }
            ],
        )
        return [
            self._normalize_statement(page, statements_db_id=db_id)
            for page in response.get("results", [])
        ]

    def _normalize_statement(
        self, page: Dict[str, Any], statements_db_id: Optional[str] = None
    ) -> Dict[str, Any]:
        db_id = self._statements_db_id(statements_db_id)
        props = page.get("properties", {})
        title_prop = self._prop_name(db_id, "Name", "title")
        theme_prop = self._prop_name(db_id, "theme", "select")
        order_prop = self._prop_name(db_id, "order", "number")
        active_prop = "active" if self._prop_exists(db_id, "active") else None
        return {
            "id": page.get("id"),
            "text": self._normalize_title(props, title_prop),
            "theme": self._normalize_select(props, theme_prop) or "",
            "order": int(self._normalize_number(props, order_prop, 0) or 0),
            "active": self._normalize_checkbox(props, active_prop, True)
            if active_prop
            else True,
        }

    # ---- Responses --------------------------------------------------
    def create_response(
        self,
        session_id: str,
        statement_id: str,
        player_id: str,
        value: int,
        level_label: Optional[str],
        note: str = "",
        responses_db_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        db_id = self._responses_db_id(responses_db_id)
        session_prop = self._prop_name(db_id, "session", "relation")
        statement_prop = self._prop_name(db_id, "statement", "relation")
        player_prop = self._prop_name(db_id, "player", "relation")
        value_prop = self._prop_name(db_id, "value", "number")
        label_prop = self._prop_name(db_id, "level_label", "select")
        note_prop = self._prop_name(db_id, "note", "rich_text")
        created_prop = "created_at" if self._prop_exists(db_id, "created_at") else None
        title_prop = self._prop_name(db_id, "Name", "title")

        props: Dict[str, Any] = {}
        props.update(self._build_relation(session_prop, [session_id]))
        props.update(self._build_relation(statement_prop, [statement_id]))
        props.update(self._build_relation(player_prop, [player_id]))
        props.update(self._build_number(value_prop, value))
        props.update(self._build_select(label_prop, level_label))
        props.update(self._build_rich_text(note_prop, note))
        props.update(self._build_title(title_prop, f"Response {value:+d}"))
        if created_prop:
            now_iso = datetime.now(timezone.utc).isoformat()
            props[created_prop] = {"date": {"start": now_iso}}

        page = _execute_with_retry(
            self.client.pages.create,
            parent={"database_id": db_id},
            properties=props,
        )
        _clear_query_cache()
        return self._normalize_response(page, responses_db_id=db_id)

    def list_responses(
        self,
        session_id: str,
        statement_ids: List[str],
        responses_db_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        db_id = self._responses_db_id(responses_db_id)
        session_prop = self._prop_name(db_id, "session", "relation")
        statement_prop = self._prop_name(db_id, "statement", "relation")
        results: List[Dict[str, Any]] = []
        for statement_id in statement_ids:
            response = _cached_query(
                self.client,
                db_id,
                filter={
                    "and": [
                        {
                            "property": session_prop,
                            "relation": {"contains": session_id},
                        },
                        {
                            "property": statement_prop,
                            "relation": {"contains": statement_id},
                        },
                    ]
                },
                page_size=200,
            )
            results.extend(
                [
                    self._normalize_response(page, responses_db_id=db_id)
                    for page in response.get("results", [])
                ]
            )
        return results

    def _normalize_response(
        self, page: Dict[str, Any], responses_db_id: Optional[str] = None
    ) -> Dict[str, Any]:
        db_id = self._responses_db_id(responses_db_id)
        props = page.get("properties", {})
        value_prop = self._prop_name(db_id, "value", "number")
        label_prop = self._prop_name(db_id, "level_label", "select")
        note_prop = self._prop_name(db_id, "note", "rich_text")
        statement_prop = self._prop_name(db_id, "statement", "relation")
        player_prop = self._prop_name(db_id, "player", "relation")
        session_prop = self._prop_name(db_id, "session", "relation")
        return {
            "id": page.get("id"),
            "value": int(self._normalize_number(props, value_prop, 0) or 0),
            "level_label": self._normalize_select(props, label_prop) or "",
            "note": self._normalize_rich_text(props, note_prop),
            "statement_id": self._normalize_relation_ids(props, statement_prop),
            "player_id": self._normalize_relation_ids(props, player_prop),
            "session_id": self._normalize_relation_ids(props, session_prop),
            "created_at": page.get("created_time"),
        }

    # ---- Questions --------------------------------------------------
    def create_question(
        self,
        session_id: str,
        text: str,
        domain: Optional[List[str]] | Optional[str],
        submitted_by: str,
        status: str = "pending",
        questions_db_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        db_id = self._questions_db_id(questions_db_id)
        session_prop = self._prop_name(db_id, "session", "relation")
        submitted_prop = self._prop_name(db_id, "submitted_by", "relation")
        domain_prop = self._prop_name(db_id, "domain", "select")
        domain_meta = self._db_props(db_id).get(domain_prop, {})
        domain_type = domain_meta.get("type")
        status_prop = self._prop_name(db_id, "status", "select")
        title_prop = self._prop_name(db_id, "Name", "title")
        created_prop = "created_at" if self._prop_exists(db_id, "created_at") else None

        domains: List[str] = []
        if isinstance(domain, list):
            flat: List[str] = []
            for item in domain:
                if isinstance(item, list):
                    flat.extend([str(d) for d in item if d])
                elif item:
                    flat.append(str(item))
            domains = flat
        elif isinstance(domain, str) and domain:
            domains = [domain]

        title_text = text
        if len(domains) > 1:
            title_text = f"{text} [{', '.join(domains)}]"

        props: Dict[str, Any] = {}
        props.update(self._build_relation(session_prop, [session_id]))
        props.update(self._build_relation(submitted_prop, [submitted_by]))
        # Defensive: ensure we never pass a list into a select field.
        if domain_type == "multi_select":
            props.update(self._build_multi_select(domain_prop, domains))
        else:
            primary = domains[0] if domains else None
            props.update(self._build_select(domain_prop, primary))
            if len(domains) > 1:
                multi_prop = None
                if self._prop_exists(db_id, "domains"):
                    multi_prop = "domains"
                elif self._prop_exists(db_id, "domain_multi"):
                    multi_prop = "domain_multi"
                if multi_prop:
                    props.update(self._build_multi_select(multi_prop, domains))
        props.update(self._build_select(status_prop, status))
        props.update(self._build_title(title_prop, title_text))
        if created_prop:
            now_iso = datetime.now(timezone.utc).isoformat()
            props[created_prop] = {"date": {"start": now_iso}}
        if self._prop_exists(db_id, "approve_count"):
            props.update(self._build_number("approve_count", 0))
        if self._prop_exists(db_id, "park_count"):
            props.update(self._build_number("park_count", 0))
        if self._prop_exists(db_id, "rewrite_count"):
            props.update(self._build_number("rewrite_count", 0))

        page = _execute_with_retry(
            self.client.pages.create,
            parent={"database_id": db_id},
            properties=props,
        )
        _clear_query_cache()
        return self._normalize_question(page, questions_db_id=db_id)

    def list_questions(
        self,
        session_id: str,
        status: Optional[str] = None,
        questions_db_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        db_id = self._questions_db_id(questions_db_id)
        session_prop = self._prop_name(db_id, "session", "relation")
        filters: List[Dict[str, Any]] = [
            {"property": session_prop, "relation": {"contains": session_id}}
        ]
        if status:
            status_prop = self._prop_name(db_id, "status", "select")
            filters.append({"property": status_prop, "select": {"equals": status}})
        response = _cached_query(
            self.client,
            db_id,
            filter={"and": filters} if len(filters) > 1 else filters[0],
            page_size=200,
            sorts=[{"timestamp": "created_time", "direction": "descending"}],
        )
        return [
            self._normalize_question(page, questions_db_id=db_id)
            for page in response.get("results", [])
        ]

    def list_listed_questions(
        self, session_id: str, questions_db_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        return self.list_questions(
            session_id, status="responded", questions_db_id=questions_db_id
        )

    def get_question_by_id(
        self, question_id: str, questions_db_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        if not question_id:
            return None
        db_id = self._questions_db_id(questions_db_id)
        try:
            page = _execute_with_retry(self.client.pages.retrieve, page_id=question_id)
        except Exception:
            return None
        return self._normalize_question(page, questions_db_id=db_id)

    def update_question_status(
        self,
        question_id: str,
        status: str,
        questions_db_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        db_id = self._questions_db_id(questions_db_id)
        status_prop = self._prop_name(db_id, "status", "select")
        page = _execute_with_retry(
            self.client.pages.update,
            page_id=question_id,
            properties=self._build_select(status_prop, status),
        )
        _clear_query_cache()
        return self._normalize_question(page, questions_db_id=db_id)

    def increment_question_list(
        self,
        question_id: str,
        questions_db_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        return self.update_question_status(
            question_id, "responded", questions_db_id=questions_db_id
        )

    def increment_question_upvote(
        self,
        question_id: str,
        questions_db_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        return self.increment_question_list(
            question_id, questions_db_id=questions_db_id
        )

    def _normalize_question(
        self, page: Dict[str, Any], questions_db_id: Optional[str] = None
    ) -> Dict[str, Any]:
        db_id = self._questions_db_id(questions_db_id)
        props = page.get("properties", {})
        title_prop = self._prop_name(db_id, "Name", "title")
        domain_prop = self._prop_name(db_id, "domain", "select")
        status_prop = self._prop_name(db_id, "status", "select")
        submitted_prop = self._prop_name(db_id, "submitted_by", "relation")
        session_prop = self._prop_name(db_id, "session", "relation")
        return {
            "id": page.get("id"),
            "text": self._normalize_title(props, title_prop),
            "domain": self._normalize_select(props, domain_prop) or "",
            "status": self._normalize_select(props, status_prop) or "",
            "submitted_by": self._normalize_relation_ids(props, submitted_prop),
            "session_id": self._normalize_relation_ids(props, session_prop),
            "approve_count": int(self._normalize_number(props, "approve_count", 0) or 0)
            if self._prop_exists(db_id, "approve_count")
            else 0,
            "park_count": int(self._normalize_number(props, "park_count", 0) or 0)
            if self._prop_exists(db_id, "park_count")
            else 0,
            "rewrite_count": int(self._normalize_number(props, "rewrite_count", 0) or 0)
            if self._prop_exists(db_id, "rewrite_count")
            else 0,
            "created_at": page.get("created_time"),
        }

    # ---- Moderation Votes ------------------------------------------
    def create_moderation_vote(
        self,
        session_id: str,
        question_id: str,
        voter_id: str,
        vote: str,
        moderation_votes_db_id: Optional[str] = None,
        questions_db_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        db_id = self._moderation_votes_db_id(moderation_votes_db_id)
        session_prop = self._prop_name(db_id, "session", "relation")
        question_prop = self._prop_name(db_id, "question", "relation")
        voter_prop = self._prop_name(db_id, "voter", "relation")
        vote_prop = self._prop_name(db_id, "vote", "select")
        created_prop = "created_at" if self._prop_exists(db_id, "created_at") else None
        title_prop = self._prop_name(db_id, "Name", "title")

        props: Dict[str, Any] = {}
        props.update(self._build_relation(session_prop, [session_id]))
        props.update(self._build_relation(question_prop, [question_id]))
        props.update(self._build_relation(voter_prop, [voter_id]))
        props.update(self._build_select(vote_prop, vote))
        props.update(self._build_title(title_prop, f"{vote.title()} vote"))
        if created_prop:
            now_iso = datetime.now(timezone.utc).isoformat()
            props[created_prop] = {"date": {"start": now_iso}}

        page = _execute_with_retry(
            self.client.pages.create,
            parent={"database_id": db_id},
            properties=props,
        )
        _clear_query_cache()
        tally = self.tally_moderation_votes(question_id, moderation_votes_db_id=db_id)
        self.update_question_counts(
            question_id,
            approve_count=tally["approve"],
            park_count=tally["park"],
            rewrite_count=tally["rewrite"],
            questions_db_id=questions_db_id,
        )
        if tally["approve"] >= 3:
            self.update_question_status(
                question_id, "approved", questions_db_id=questions_db_id
            )
        return self._normalize_moderation_vote(page, moderation_votes_db_id=db_id)

    def list_moderation_votes(
        self,
        question_id: str,
        moderation_votes_db_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        db_id = self._moderation_votes_db_id(moderation_votes_db_id)
        question_prop = self._prop_name(db_id, "question", "relation")
        response = _cached_query(
            self.client,
            db_id,
            filter={"property": question_prop, "relation": {"contains": question_id}},
            page_size=200,
        )
        return [
            self._normalize_moderation_vote(page, moderation_votes_db_id=db_id)
            for page in response.get("results", [])
        ]

    def tally_moderation_votes(
        self,
        question_id: str,
        moderation_votes_db_id: Optional[str] = None,
    ) -> Dict[str, int]:
        votes = self.list_moderation_votes(
            question_id, moderation_votes_db_id=moderation_votes_db_id
        )
        counts = {"approve": 0, "park": 0, "rewrite": 0}
        for vote in votes:
            key = vote.get("vote", "").lower()
            if key in counts:
                counts[key] += 1
        return counts

    def update_question_counts(
        self,
        question_id: str,
        approve_count: int,
        park_count: int,
        rewrite_count: int,
        questions_db_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        db_id = self._questions_db_id(questions_db_id)
        props: Dict[str, Any] = {}
        if self._prop_exists(db_id, "approve_count"):
            props.update(self._build_number("approve_count", approve_count))
        if self._prop_exists(db_id, "park_count"):
            props.update(self._build_number("park_count", park_count))
        if self._prop_exists(db_id, "rewrite_count"):
            props.update(self._build_number("rewrite_count", rewrite_count))
        if not props:
            return self.get_question_by_id(question_id, questions_db_id=db_id)
        page = _execute_with_retry(
            self.client.pages.update, page_id=question_id, properties=props
        )
        _clear_query_cache()
        return self._normalize_question(page, questions_db_id=db_id)

    def _normalize_moderation_vote(
        self, page: Dict[str, Any], moderation_votes_db_id: Optional[str] = None
    ) -> Dict[str, Any]:
        db_id = self._moderation_votes_db_id(moderation_votes_db_id)
        props = page.get("properties", {})
        vote_prop = self._prop_name(db_id, "vote", "select")
        question_prop = self._prop_name(db_id, "question", "relation")
        voter_prop = self._prop_name(db_id, "voter", "relation")
        session_prop = self._prop_name(db_id, "session", "relation")
        return {
            "id": page.get("id"),
            "vote": self._normalize_select(props, vote_prop) or "",
            "question_id": self._normalize_relation_ids(props, question_prop),
            "voter_id": self._normalize_relation_ids(props, voter_prop),
            "session_id": self._normalize_relation_ids(props, session_prop),
            "created_at": page.get("created_time"),
        }

    # ---- Decisions --------------------------------------------------
    def create_decision(
        self,
        session_id: str,
        player_id: str,
        decision_type: str,
        payload: str,
        decisions_db_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        db_id = self._decisions_db_id(decisions_db_id)
        session_prop = self._prop_name(db_id, "session", "relation")
        player_prop = self._prop_name(db_id, "player", "relation")
        type_prop = self._prop_name(db_id, "type", "select")
        payload_prop = self._prop_name(db_id, "payload", "rich_text")
        created_prop = "created_at" if self._prop_exists(db_id, "created_at") else None
        title_prop = self._prop_name(db_id, "Name", "title")

        props: Dict[str, Any] = {}
        props.update(self._build_relation(session_prop, [session_id]))
        props.update(self._build_relation(player_prop, [player_id]))
        props.update(self._build_select(type_prop, decision_type))
        props.update(self._build_rich_text_chunks(payload_prop, payload))
        props.update(self._build_title(title_prop, decision_type.title()))
        if created_prop:
            now_iso = datetime.now(timezone.utc).isoformat()
            props[created_prop] = {"date": {"start": now_iso}}

        page = _execute_with_retry(
            self.client.pages.create,
            parent={"database_id": db_id},
            properties=props,
        )
        _clear_query_cache()
        return {
            "id": page.get("id"),
            "type": decision_type,
            "payload": payload,
            "created_at": page.get("created_time"),
        }

    def list_decisions(
        self,
        session_id: str,
        decision_type: Optional[str] = None,
        decisions_db_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        db_id = self._decisions_db_id(decisions_db_id)
        session_prop = self._prop_name(db_id, "session", "relation")
        filters: List[Dict[str, Any]] = [
            {"property": session_prop, "relation": {"contains": session_id}}
        ]
        if decision_type:
            type_prop = self._prop_name(db_id, "type", "select")
            filters.append({"property": type_prop, "select": {"equals": decision_type}})
        response = _cached_query(
            self.client,
            db_id,
            filter={"and": filters} if len(filters) > 1 else filters[0],
            page_size=200,
            sorts=[{"timestamp": "created_time", "direction": "ascending"}],
        )
        return [
            self._normalize_decision(page, decisions_db_id=db_id)
            for page in response.get("results", [])
        ]

    def _normalize_decision(
        self, page: Dict[str, Any], decisions_db_id: Optional[str] = None
    ) -> Dict[str, Any]:
        db_id = self._decisions_db_id(decisions_db_id)
        props = page.get("properties", {})
        type_prop = self._prop_name(db_id, "type", "select")
        payload_prop = self._prop_name(db_id, "payload", "rich_text")
        player_prop = self._prop_name(db_id, "player", "relation")
        session_prop = self._prop_name(db_id, "session", "relation")
        payload_val = props.get(payload_prop) if isinstance(props, dict) else None
        payload_text = ""
        if isinstance(payload_val, dict) and payload_val.get("type") == "rich_text":
            parts = payload_val.get("rich_text", [])
            payload_text = "".join(part.get("plain_text", "") for part in parts)
        return {
            "id": page.get("id"),
            "type": self._normalize_select(props, type_prop) or "",
            "payload": payload_text,
            "player_id": self._normalize_relation_ids(props, player_prop),
            "session_id": self._normalize_relation_ids(props, session_prop),
            "created_at": page.get("created_time"),
        }

    # ---- Highlights ------------------------------------------------
    def upsert_highlight(
        self,
        session_id: str,
        player_id: str,
        text_id: str,
        selected_text: str,
        start_char: int,
        end_char: int,
        anchor_prefix: str = "",
        anchor_suffix: str = "",
        note: str = "",
        emotion: Optional[str] = None,
        reason: Optional[str] = None,
        highlights_db_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        db_id = self._highlights_db_id(highlights_db_id)
        if not db_id:
            raise ValueError("Highlights database id is not configured.")

        existing = self._find_highlight(
            text_id=text_id,
            start_char=start_char,
            end_char=end_char,
            player_id=player_id,
            highlights_db_id=db_id,
        )

        title_prop = self._prop_name(db_id, "Name", "title")
        text_id_prop = self._prop_name(db_id, "text_id", "rich_text")
        selected_prop = self._prop_name(db_id, "selected_text", "rich_text")
        start_prop = self._prop_name(db_id, "start_char", "number")
        end_prop = self._prop_name(db_id, "end_char", "number")
        props: Dict[str, Any] = {}

        props.update(
            self._build_title(title_prop, f"{text_id}:{start_char}-{end_char}")
        )
        props.update(self._build_rich_text(text_id_prop, text_id))
        props.update(self._build_rich_text(selected_prop, selected_text))
        props.update(self._build_number(start_prop, start_char))
        props.update(self._build_number(end_prop, end_char))

        if self._prop_exists(db_id, "anchor_prefix"):
            props.update(self._build_rich_text("anchor_prefix", anchor_prefix))
        if self._prop_exists(db_id, "anchor_suffix"):
            props.update(self._build_rich_text("anchor_suffix", anchor_suffix))
        if self._prop_exists(db_id, "note"):
            props.update(self._build_rich_text("note", note))
        if self._prop_exists(db_id, "emotion"):
            props.update(self._build_select("emotion", emotion))
        if self._prop_exists(db_id, "reason"):
            props.update(self._build_select("reason", reason))
        if self._prop_exists(db_id, "session"):
            props.update(self._build_relation("session", [session_id]))
        if self._prop_exists(db_id, "player"):
            props.update(self._build_relation("player", [player_id]))
        if self._prop_exists(db_id, "created_at") and not existing:
            now_iso = datetime.now(timezone.utc).isoformat()
            props["created_at"] = {"date": {"start": now_iso}}
        if self._prop_exists(db_id, "updated_at"):
            now_iso = datetime.now(timezone.utc).isoformat()
            props["updated_at"] = {"date": {"start": now_iso}}

        if existing:
            page = _execute_with_retry(
                self.client.pages.update, page_id=existing["id"], properties=props
            )
        else:
            page = _execute_with_retry(
                self.client.pages.create,
                parent={"database_id": db_id},
                properties=props,
            )
        _clear_query_cache()
        return self._normalize_highlight(page, highlights_db_id=db_id)

    def _find_highlight(
        self,
        text_id: str,
        start_char: int,
        end_char: int,
        player_id: str,
        highlights_db_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        db_id = self._highlights_db_id(highlights_db_id)
        text_id_prop = self._prop_name(db_id, "text_id", "rich_text")
        start_prop = self._prop_name(db_id, "start_char", "number")
        end_prop = self._prop_name(db_id, "end_char", "number")
        filters: list[Dict[str, Any]] = [
            {"property": text_id_prop, "rich_text": {"equals": text_id}},
            {"property": start_prop, "number": {"equals": int(start_char)}},
            {"property": end_prop, "number": {"equals": int(end_char)}},
        ]
        if player_id:
            if self._prop_exists(db_id, "player"):
                filters.append(
                    {"property": "player", "relation": {"contains": player_id}}
                )
            elif self._prop_exists(db_id, "player_id"):
                filters.append(
                    {"property": "player_id", "rich_text": {"equals": player_id}}
                )

        response = _cached_query(
            self.client,
            db_id,
            filter={"and": filters},
            page_size=1,
        )
        results = response.get("results", [])
        if not results:
            return None
        return self._normalize_highlight(results[0], highlights_db_id=db_id)

    def list_highlights(
        self,
        session_id: Optional[str] = None,
        text_id: Optional[str] = None,
        highlights_db_id: Optional[str] = None,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        db_id = self._highlights_db_id(highlights_db_id)
        if not db_id:
            return []
        filters: list[Dict[str, Any]] = []
        if text_id:
            text_prop = self._prop_name(db_id, "text_id", "rich_text")
            filters.append({"property": text_prop, "rich_text": {"equals": text_id}})
        if session_id and self._prop_exists(db_id, "session"):
            filters.append(
                {"property": "session", "relation": {"contains": session_id}}
            )

        filter_payload: Optional[Dict[str, Any]] = None
        if len(filters) == 1:
            filter_payload = filters[0]
        elif len(filters) > 1:
            filter_payload = {"and": filters}

        query_kwargs: Dict[str, Any] = {
            "page_size": limit,
            "sorts": [{"timestamp": "created_time", "direction": "descending"}],
        }
        if filter_payload:
            query_kwargs["filter"] = filter_payload

        response = _cached_query(self.client, db_id, **query_kwargs)
        return [
            self._normalize_highlight(page, highlights_db_id=db_id)
            for page in response.get("results", [])
        ]

    def _normalize_highlight(
        self, page: Dict[str, Any], highlights_db_id: Optional[str] = None
    ) -> Dict[str, Any]:
        db_id = self._highlights_db_id(highlights_db_id)
        props = page.get("properties", {})
        text_id_prop = self._prop_name(db_id, "text_id", "rich_text")
        selected_prop = self._prop_name(db_id, "selected_text", "rich_text")
        start_prop = self._prop_name(db_id, "start_char", "number")
        end_prop = self._prop_name(db_id, "end_char", "number")
        session_ids = (
            self._normalize_relation_ids(props, "session")
            if self._prop_exists(db_id, "session")
            else []
        )
        player_ids = (
            self._normalize_relation_ids(props, "player")
            if self._prop_exists(db_id, "player")
            else []
        )
        return {
            "id": page.get("id"),
            "text_id": self._normalize_rich_text(props, text_id_prop),
            "selected_text": self._normalize_rich_text(props, selected_prop),
            "start_char": int(self._normalize_number(props, start_prop, 0) or 0),
            "end_char": int(self._normalize_number(props, end_prop, 0) or 0),
            "anchor_prefix": self._normalize_rich_text(props, "anchor_prefix")
            if self._prop_exists(db_id, "anchor_prefix")
            else "",
            "anchor_suffix": self._normalize_rich_text(props, "anchor_suffix")
            if self._prop_exists(db_id, "anchor_suffix")
            else "",
            "note": self._normalize_rich_text(props, "note")
            if self._prop_exists(db_id, "note")
            else "",
            "emotion": self._normalize_select(props, "emotion")
            if self._prop_exists(db_id, "emotion")
            else "",
            "reason": self._normalize_select(props, "reason")
            if self._prop_exists(db_id, "reason")
            else "",
            "session_id": session_ids,
            "player_id": player_ids,
            "created_at": page.get("created_time"),
        }


def init_notion_repo(
    session_db_id: Optional[str] = None,
    players_db_id: Optional[str] = None,
    ideas_db_id: Optional[str] = None,
    links_db_id: Optional[str] = None,
    outcomes_db_id: Optional[str] = None,
    resonance_db_id: Optional[str] = None,
    statements_db_id: Optional[str] = None,
    responses_db_id: Optional[str] = None,
    questions_db_id: Optional[str] = None,
    moderation_votes_db_id: Optional[str] = None,
    decisions_db_id: Optional[str] = None,
    highlights_db_id: Optional[str] = None,
) -> Optional[NotionRepo]:
    def _resolve_db_id(explicit_value: Optional[str], env_key: str) -> str:
        if explicit_value:
            return _clean_notion_id(explicit_value)
        return _clean_notion_id(os.getenv(env_key, ""))

    api_key = str(os.getenv("NOTION_TOKEN", "")).strip()
    if not api_key:
        st.warning("Clé API absente ; fonctions désactivées.")
        return None

    client = None
    try:
        from notion_client import Client

        notion_version = None
        if notion_version:
            client = Client(auth=api_key, notion_version=notion_version)
        else:
            client = Client(auth=api_key)
    except Exception as exc:  # pragma: no cover
        st.warning(f"Échec d'initialisation du SDK : {exc}")

    if client is None and NotionConnection is not None:
        try:
            conn = st.connection(
                "notion",
                type=NotionConnection,
                notion_api_key=api_key,
            )
            client = getattr(conn, "client", None) or getattr(conn, "_client", None)
        except Exception as exc:
            st.warning(f"Échec de connexion Streamlit-Notion : {exc}")

    if client is None:  # pragma: no cover
        st.error("Impossible d'initialiser le client Notion.")
        return None
    _ensure_base_url(client)
    _debug_client("notion.init", client)

    resolved_session_db_id = _resolve_db_id(
        session_db_id, "AFF_SESSIONS_DB_ID"
    ) or _clean_notion_id(SESSIONS_DB_ID)
    resolved_players_db_id = _resolve_db_id(
        players_db_id, "AFF_PLAYERS_DB_ID"
    ) or _clean_notion_id(PLAYERS_DB_ID)
    resolved_statements_db_id = _resolve_db_id(statements_db_id, "AFF_STATEMENTS_DB_ID")
    resolved_responses_db_id = _resolve_db_id(responses_db_id, "AFF_RESPONSES_DB_ID")
    resolved_questions_db_id = _resolve_db_id(questions_db_id, "AFF_QUESTIONS_DB_ID")
    resolved_moderation_votes_db_id = _resolve_db_id(
        moderation_votes_db_id, "AFF_VOTES_DB_ID"
    )
    resolved_decisions_db_id = _resolve_db_id(decisions_db_id, "AFF_DECISIONS_DB_ID")
    resolved_highlights_db_id = _clean_notion_id(highlights_db_id or "")

    return NotionRepo(
        client,
        session_db_id=resolved_session_db_id,
        players_db_id=resolved_players_db_id,
        ideas_db_id=ideas_db_id or "",
        links_db_id=links_db_id or "",
        outcomes_db_id=outcomes_db_id or "",
        resonance_db_id=resonance_db_id or "",
        statements_db_id=resolved_statements_db_id,
        responses_db_id=resolved_responses_db_id,
        questions_db_id=resolved_questions_db_id,
        moderation_votes_db_id=resolved_moderation_votes_db_id,
        decisions_db_id=resolved_decisions_db_id,
        highlights_db_id=resolved_highlights_db_id,
    )
