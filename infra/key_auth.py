from __future__ import annotations

from typing import Callable, Dict, Optional, Tuple, List

import streamlit as st
from streamlit_authenticator.controllers import CookieController
from streamlit_authenticator.utilities import RegisterError, Validator

from infra.key_codec import (
    generate_hex_key,
    hex_to_emoji,
    hex_to_phrase,
    normalize_access_key,
    split_emoji_symbols,
)
from infra.notion_repo import NotionRepo


class AccessKeyAuthenticationModel:
    """Minimal authentication backend that treats Notion player IDs as access keys."""

    def __init__(
        self,
        credentials: Dict,
        notion_repo: Optional[NotionRepo],
        default_session_code: str,
    ) -> None:
        self.credentials = credentials
        self.repo = notion_repo
        self.default_session_code = default_session_code
        self.webapp_name = credentials.get("webapp", "idea-resonance")
        if "authentication_status" not in st.session_state:
            st.session_state["authentication_status"] = None
        if "username" not in st.session_state:
            st.session_state["username"] = None
        if "name" not in st.session_state:
            st.session_state["name"] = None
        if "logout" not in st.session_state:
            st.session_state["logout"] = None
        self.session_meta = self._ensure_session()

    def _ensure_session(self) -> Optional[Dict]:
        if not self.repo or not self.default_session_code:
            return None
        session = self.repo.get_session_by_code(self.default_session_code)
        if not session:
            session = self.repo.create_session(self.default_session_code, "Non-linear")
        return session

    @property
    def session_id(self) -> Optional[str]:
        if self.session_meta:
            return self.session_meta.get("id")
        return None

    def login(
        self,
        access_key: str,
        callback: Optional[Callable[[Dict], None]] = None,
    ) -> bool:
        if not access_key:
            st.session_state["authentication_status"] = None
            return False
        try:
            normalized_key = normalize_access_key(access_key)
        except ValueError:
            return self._login_with_emoji_suffix(access_key, callback=callback)
        record = self._valid_access_key(normalized_key)
        if record:
            st.session_state["username"] = normalized_key
            st.session_state["name"] = record.get("nickname") or access_key
            st.session_state["authentication_status"] = True
            if callback:
                payload = self._build_payload(record, normalized_key)
                callback(payload)
            return True
        st.session_state["authentication_status"] = False
        return False

    def logout(self) -> None:
        st.session_state["logout"] = True
        st.session_state["name"] = None
        st.session_state["username"] = None
        st.session_state["authentication_status"] = None

    def register_user(
        self,
        metadata: Optional[Dict[str, str]] = None,
        callback: Optional[Callable[[Dict], None]] = None,
    ) -> Tuple[Optional[str], Optional[str], Optional[Dict]]:
        if not self.repo or not self.session_id:
            raise RegisterError(
                "Notion repository not configured; cannot mint new access keys."
            )
        access_key = generate_hex_key()
        if self._valid_access_key(access_key):
            raise RegisterError("Collision detected; retry generating a new key.")

        nickname = ""
        role = "Seeker"
        preferred_mode = None
        if metadata:
            nickname = metadata.get("name") or metadata.get("nickname") or ""
            role = metadata.get("role") or role
            preferred_mode = metadata.get("mode")

        emoji = hex_to_emoji(access_key)
        symbols = split_emoji_symbols(emoji)
        suffix4 = "".join(symbols[-4:]) if len(symbols) >= 4 else ""
        suffix6 = "".join(symbols[-6:]) if len(symbols) >= 6 else ""
        player = self.repo.upsert_player(
            session_id=self.session_id,
            player_id=access_key,
            nickname=nickname,
            role=role,
            consent_play=False,
            consent_research=False,
            preferred_mode=preferred_mode,
            emoji=emoji,
            phrase=hex_to_phrase(access_key),
            emoji_suffix_4=suffix4,
            emoji_suffix_6=suffix6,
        )
        payload: Dict[str, object] = {
            "player": player,
            "access_key": access_key,
            "metadata": metadata or {},
            "emoji": hex_to_emoji(access_key),
            "phrase": hex_to_phrase(access_key),
        }
        if callback:
            callback(payload)
        return access_key, access_key, payload

    def _valid_access_key(self, access_key: str) -> Optional[Dict]:
        if not self.repo:
            return None
        return self.repo.get_player_by_id(access_key)

    def _login_with_emoji_suffix(
        self,
        access_key: str,
        callback: Optional[Callable[[Dict], None]] = None,
    ) -> bool:
        symbols = split_emoji_symbols(access_key.strip())
        if not symbols:
            st.error("Access key format not recognized.")
            st.session_state["authentication_status"] = False
            return False
        if len(symbols) < 4:
            st.warning("Add at least 4 emoji symbols to continue.")
            st.session_state["authentication_status"] = None
            return False
        if not self.repo:
            st.session_state["authentication_status"] = False
            return False
        suffix4 = "".join(symbols[-4:])
        matches = self.repo.find_players_by_emoji_suffix(suffix4, length=4)
        if len(matches) == 1:
            record = matches[0]
            normalized_key = record.get("access_key") or ""
            st.session_state["username"] = normalized_key
            st.session_state["name"] = record.get("nickname") or "Collaborator"
            st.session_state["authentication_status"] = True
            if callback:
                payload = self._build_payload(record, normalized_key)
                callback(payload)
            return True
        if len(matches) > 1 and len(symbols) < 6:
            st.warning("Multiple matches. Add two more emoji symbols.")
            st.session_state["authentication_status"] = None
            return False
        if len(symbols) >= 6:
            suffix6 = "".join(symbols[-6:])
            matches = self.repo.find_players_by_emoji_suffix(suffix6, length=6)
            if len(matches) == 1:
                record = matches[0]
                normalized_key = record.get("access_key") or ""
                st.session_state["username"] = normalized_key
                st.session_state["name"] = record.get("nickname") or "Collaborator"
                st.session_state["authentication_status"] = True
                if callback:
                    payload = self._build_payload(record, normalized_key)
                    callback(payload)
                return True
        st.error("Key invalid or ambiguous. Try full hash or more emoji.")
        st.session_state["authentication_status"] = False
        return False

    def _build_payload(self, player: Dict, canonical_key: str) -> Dict[str, object]:
        sessions: List[Dict[str, object]] = []
        if self.repo:
            for session_id in player.get("session_ids", []):
                session = self.repo.get_session(session_id)
                if session:
                    sessions.append(
                        {
                            "session_code": session["session_code"],
                            "status": session.get("status", "—"),
                            "round_index": session.get("round_index", 0),
                        }
                    )
        return {
            "player": player,
            "sessions": sessions,
            "emoji": hex_to_emoji(canonical_key),
            "phrase": hex_to_phrase(canonical_key),
            "access_key": canonical_key,
        }


class AuthenticateWithKey:
    """Streamlit-Authenticator inspired interface that grants access via Notion-backed keys."""

    def __init__(
        self,
        credentials: Dict,
        cookie_name: str,
        cookie_key: str,
        cookie_expiry_days: float,
        notion_repo: Optional[NotionRepo],
        default_session_code: str,
        validator: Optional[Validator] = None,
    ) -> None:
        self.cookie_controller = CookieController(
            cookie_name,
            cookie_key,
            cookie_expiry_days,
        )
        self.validator = validator or Validator()
        self.auth_model = AccessKeyAuthenticationModel(
            credentials,
            notion_repo,
            default_session_code,
        )

    def login(
        self,
        location: str = "main",
        key: str = "Access key login",
        callback: Optional[Callable[[Dict], None]] = None,
    ):
        container = st.sidebar if location == "sidebar" else st
        token = self.cookie_controller.get_cookie()
        if token and not st.session_state.get("authentication_status"):
            self.auth_model.login(token.get("username", ""), callback=callback)

        input_key = f"{key}-access-input"
        prefill_key = str(st.session_state.pop("login_access_key_prefill", "")).strip()
        if prefill_key:
            st.session_state[input_key] = prefill_key
            st.session_state["login_access_key_prefill_notice"] = (
                "Your 4-emoji key has been prefilled."
            )
        notice = str(st.session_state.pop("login_access_key_prefill_notice", "")).strip()
        if notice:
            container.info(notice)

        with container.form(key):
            access_key = st.text_input("Access key", key=input_key).strip()
            submit = st.form_submit_button("Open with key 🔑")
        if submit:
            success = self.auth_model.login(access_key, callback=callback)
            if success:
                self.cookie_controller.set_cookie()
                st.toast("Access granted.")
            else:
                st.error("Access key invalid.")

        return (
            st.session_state.get("name"),
            st.session_state.get("authentication_status"),
            st.session_state.get("username"),
        )

    def logout(self, button_name: str = "Logout", location: str = "sidebar") -> None:
        container = st.sidebar if location == "sidebar" else st
        if container.button(button_name):
            self.cookie_controller.delete_cookie()
            self.auth_model.logout()
            st.success("Logged out.")

    def register_user(
        self,
        metadata: Optional[Dict[str, str]] = None,
        callback: Optional[Callable[[Dict], None]] = None,
    ) -> Tuple[Optional[str], Optional[str], Optional[Dict]]:
        return self.auth_model.register_user(metadata=metadata, callback=callback)
