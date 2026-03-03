from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Optional

import streamlit as st
import yaml
from yaml.loader import SafeLoader

from infra.key_auth import AuthenticateWithKey
from infra.notion_repo import NotionRepo, init_notion_repo

CONFIG_PATH = Path(__file__).resolve().parents[1] / "config.yaml"


@st.cache_data(show_spinner=False)
def load_config() -> Dict[str, Any]:
    if not CONFIG_PATH.exists():
        return {}
    with CONFIG_PATH.open("r", encoding="utf-8") as fh:
        return yaml.load(fh, Loader=SafeLoader) or {}


def _pick_id(key: str) -> str:
    return str(os.getenv(key, "")).strip()


@st.cache_resource(show_spinner=False)
def get_notion_repo() -> Optional[NotionRepo]:
    repo = init_notion_repo(
        session_db_id=_pick_id("AFF_SESSIONS_DB_ID"),
        players_db_id=_pick_id("AFF_PLAYERS_DB_ID"),
        statements_db_id=_pick_id("AFF_STATEMENTS_DB_ID"),
        responses_db_id=_pick_id("AFF_RESPONSES_DB_ID"),
        questions_db_id=_pick_id("AFF_QUESTIONS_DB_ID"),
        moderation_votes_db_id=_pick_id("AFF_VOTES_DB_ID"),
        decisions_db_id=_pick_id("AFF_DECISIONS_DB_ID"),
        highlights_db_id="",
    )
    if repo and (not hasattr(repo, "list_decisions") or not hasattr(repo, "upsert_highlight")):
        reset_notion_repo_cache()
        repo = init_notion_repo(
            session_db_id=_pick_id("AFF_SESSIONS_DB_ID"),
            players_db_id=_pick_id("AFF_PLAYERS_DB_ID"),
            statements_db_id=_pick_id("AFF_STATEMENTS_DB_ID"),
            responses_db_id=_pick_id("AFF_RESPONSES_DB_ID"),
            questions_db_id=_pick_id("AFF_QUESTIONS_DB_ID"),
            moderation_votes_db_id=_pick_id("AFF_VOTES_DB_ID"),
            decisions_db_id=_pick_id("AFF_DECISIONS_DB_ID"),
            highlights_db_id="",
        )
    return repo


def reset_notion_repo_cache() -> None:
    try:
        get_notion_repo.clear()
    except Exception:
        pass


def get_authenticator(repo: Optional[NotionRepo]) -> AuthenticateWithKey:
    auth_cfg = get_auth_runtime_config()
    config = load_config()
    credentials = config.get("credentials", {})
    return AuthenticateWithKey(
        credentials,
        auth_cfg["cookie_name"],
        auth_cfg["cookie_key"],
        auth_cfg["cookie_expiry_days"],
        notion_repo=repo,
        default_session_code=auth_cfg["default_session_code"],
    )


def get_active_session(repo: Optional[NotionRepo]) -> Optional[Dict[str, Any]]:
    if not repo:
        return None
    active = repo.get_active_session()
    if active:
        return active
    default_code = get_auth_runtime_config()["default_session_code"]
    return repo.get_session_by_code(default_code)


def get_auth_runtime_config() -> Dict[str, Any]:
    cookie_cfg = st.secrets.get("cookie", {})

    cookie_name = str(cookie_cfg.get("name", "")).strip()
    cookie_key = str(cookie_cfg.get("key", "")).strip()
    expiry_raw = cookie_cfg.get("expiry_days")
    default_session_code = "GLOBAL-SESSION"

    try:
        cookie_expiry_days = float(expiry_raw)
    except Exception:
        cookie_expiry_days = -1.0

    if not cookie_name or not cookie_key or cookie_expiry_days <= 0:
        st.error(
            "Secrets de cookie d'authentification manquants. "
            "Définissez st.secrets['cookie'] avec les clés 'name', 'key', 'expiry_days'."
        )
        st.stop()

    return {
        "cookie_name": cookie_name,
        "cookie_key": cookie_key,
        "cookie_expiry_days": cookie_expiry_days,
        "default_session_code": default_session_code,
        "source": "st.secrets['cookie']",
    }
