from __future__ import annotations

from pathlib import Path
import re
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
    secrets_cfg = st.secrets.get("notion", {})
    candidates = [key, key.lower()]
    for candidate in candidates:
        if candidate in secrets_cfg and secrets_cfg[candidate]:
            return str(secrets_cfg[candidate])
        if candidate in st.secrets and st.secrets[candidate]:
            return str(st.secrets[candidate])
    return ""


@st.cache_resource(show_spinner=True)
def _build_notion_repo_cached(
    session_db_id: str,
    players_db_id: str,
    statements_db_id: str,
    responses_db_id: str,
    questions_db_id: str,
    moderation_votes_db_id: str,
    decisions_db_id: str,
) -> Optional[NotionRepo]:
    return init_notion_repo(
        session_db_id=session_db_id,
        players_db_id=players_db_id,
        statements_db_id=statements_db_id,
        responses_db_id=responses_db_id,
        questions_db_id=questions_db_id,
        moderation_votes_db_id=moderation_votes_db_id,
        decisions_db_id=decisions_db_id,
        highlights_db_id="",
    )


def get_notion_repo() -> Optional[NotionRepo]:
    session_db_id = _pick_id("AFF_SESSIONS_DB_ID")
    players_db_id = _pick_id("AFF_PLAYERS_DB_ID")
    statements_db_id = _pick_id("AFF_STATEMENTS_DB_ID")
    responses_db_id = _pick_id("AFF_RESPONSES_DB_ID")
    questions_db_id = _pick_id("AFF_QUESTIONS_DB_ID")
    moderation_votes_db_id = _pick_id("AFF_VOTES_DB_ID")
    decisions_db_id = _pick_id("AFF_DECISIONS_DB_ID")

    missing: list[str] = []
    if not session_db_id:
        missing.append("AFF_SESSIONS_DB_ID / aff_sessions_db_id")
    if not players_db_id:
        missing.append("AFF_PLAYERS_DB_ID / aff_players_db_id")
    if missing:
        st.error("Secrets Notion manquants: " + ", ".join(missing))
        return None

    repo = _build_notion_repo_cached(
        session_db_id=session_db_id,
        players_db_id=players_db_id,
        statements_db_id=statements_db_id,
        responses_db_id=responses_db_id,
        questions_db_id=questions_db_id,
        moderation_votes_db_id=moderation_votes_db_id,
        decisions_db_id=decisions_db_id,
    )
    if repo and (not repo.session_db_id or not repo.players_db_id):
        _build_notion_repo_cached.clear()
        repo = _build_notion_repo_cached(
            session_db_id=session_db_id,
            players_db_id=players_db_id,
            statements_db_id=statements_db_id,
            responses_db_id=responses_db_id,
            questions_db_id=questions_db_id,
            moderation_votes_db_id=moderation_votes_db_id,
            decisions_db_id=decisions_db_id,
        )
    return repo


def reset_notion_repo_cache() -> None:
    try:
        _build_notion_repo_cached.clear()
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
    sanitized_cookie_name = re.sub(r"[^A-Za-z0-9._-]", "_", cookie_name)
    if sanitized_cookie_name != cookie_name:
        st.warning(
            "Nom de cookie ajusté automatiquement pour compatibilité navigateur."
        )
        cookie_name = sanitized_cookie_name
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
