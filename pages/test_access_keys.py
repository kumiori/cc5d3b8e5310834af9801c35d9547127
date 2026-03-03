from __future__ import annotations

import inspect
import os
from pathlib import Path
from typing import Dict, Optional, Tuple
from datetime import datetime

import streamlit as st
import yaml
from importlib.metadata import PackageNotFoundError, version
from yaml.loader import SafeLoader


from infra.key_auth import AuthenticateWithKey
from infra.credentials_pdf import build_credentials_pdf
from infra.key_codec import (
    hex_to_emoji,
    hex_to_phrase,
    normalize_access_key,
)
from infra.notion_repo import NotionRepo, init_notion_repo
from infra.app_context import get_auth_runtime_config

CONFIG_PATH = Path(__file__).resolve().parents[1] / "config.yaml"
DEFAULT_SESSION_CODE = st.secrets.get("notion", {}).get(
    "default_session_code", "GLOBAL-SESSION"
)


def _pkg_version(name: str) -> str:
    try:
        return version(name)
    except PackageNotFoundError:
        return "not installed"
    except Exception as exc:  # pragma: no cover
        return f"error: {exc}"


@st.cache_data(show_spinner=False)
def load_config(path: str) -> Dict:
    if not Path(path).exists():
        return {}
    with open(path, "r", encoding="utf-8") as file:
        return yaml.load(file, Loader=SafeLoader) or {}


def _resolve_id_with_source(
    notion_keys: list[str]
) -> Tuple[str, str]:
    for key in notion_keys:
        value = os.getenv(key)
        if value:
            return str(value), f"env:{key}"
    return "", "<absent>"


def ensure_shared_state() -> None:
    ss = st.session_state
    ss.setdefault("player_id", "")
    ss.setdefault("nickname", "")
    ss.setdefault("mode", "Non-linear")
    ss.setdefault("session_code", DEFAULT_SESSION_CODE)
    ss.setdefault("access_payload", None)


ensure_shared_state()

config = load_config(str(CONFIG_PATH))

resolved_session_db_id, session_source = _resolve_id_with_source(
    notion_keys=["AFF_SESSIONS_DB_ID"],
)
resolved_players_db_id, players_source = _resolve_id_with_source(
    notion_keys=["AFF_PLAYERS_DB_ID"],
)

notion_repo = init_notion_repo(
    session_db_id=resolved_session_db_id,
    players_db_id=resolved_players_db_id,
)


def remember_access(payload: Dict) -> None:
    st.session_state.access_payload = payload
    player = payload.get("player") or {}
    st.session_state.player_id = player.get("player_id") or st.session_state.player_id
    st.session_state.nickname = player.get("nickname") or st.session_state.nickname
    st.session_state.mode = player.get("preferred_mode") or st.session_state.mode
    st.session_state.session_code = DEFAULT_SESSION_CODE
    session_id = getattr(authenticator.auth_model, "session_id", None)
    if session_id:
        st.session_state.session_id = session_id


st.set_page_config(page_title="Clés d'accès · Les Affranchis", page_icon="🔑")
st.title("🔑 Clés d'accès · Les Affranchis")
st.caption("Créez ou saisissez une clé d'accès pour rejoindre la session.")

with st.expander("Debug : connexion Notion", expanded=True):
    st.write("IDs résolus")
    st.code(
        (
            f"AFF_SESSIONS_DB_ID={resolved_session_db_id or '<absent>'}\n"
            f"AFF_SESSIONS_DB_ID source={session_source}\n"
            f"AFF_PLAYERS_DB_ID={resolved_players_db_id or '<absent>'}\n"
            f"AFF_PLAYERS_DB_ID source={players_source}"
        )
    )
    st.write("Versions des paquets Python")
    st.code(
        (
            f"streamlit={_pkg_version('streamlit')}\n"
            f"notion-client={_pkg_version('notion-client')}\n"
            f"streamlit-notion={_pkg_version('streamlit-notion')}\n"
            f"streamlit-authenticator={_pkg_version('streamlit-authenticator')}"
        )
    )
    if notion_repo:
        client = notion_repo.client
        databases_endpoint = getattr(client, "databases", None)
        query_method = getattr(databases_endpoint, "query", None)
        data_sources_endpoint = getattr(client, "data_sources", None)
        ds_query_method = getattr(data_sources_endpoint, "query", None)
        st.code(
            (
                f"client_type={type(client).__module__}.{type(client).__name__}\n"
                f"databases_endpoint_type={type(databases_endpoint).__module__}.{type(databases_endpoint).__name__ if databases_endpoint else 'None'}\n"
                f"has_databases_query={bool(query_method)}\n"
                f"query_signature={inspect.signature(query_method) if query_method else '<missing>'}\n"
                f"data_sources_endpoint_type={type(data_sources_endpoint).__module__}.{type(data_sources_endpoint).__name__ if data_sources_endpoint else 'None'}\n"
                f"has_data_sources_query={bool(ds_query_method)}\n"
                f"data_sources_query_signature={inspect.signature(ds_query_method) if ds_query_method else '<missing>'}"
            )
        )
    else:
        st.error("Le dépôt Notion n'a pas pu être initialisé.")

if not resolved_session_db_id or not resolved_players_db_id:
    st.error("IDs Notion requis manquants. Définissez AFF_SESSIONS_DB_ID et AFF_PLAYERS_DB_ID.")
    st.stop()

try:
    auth_cfg = get_auth_runtime_config()
    authenticator = AuthenticateWithKey(
        config["credentials"],
        auth_cfg["cookie_name"],
        auth_cfg["cookie_key"],
        auth_cfg["cookie_expiry_days"],
        notion_repo=notion_repo,
        default_session_code=auth_cfg["default_session_code"],
    )
except Exception as exc:
    st.error(f"Échec d'initialisation de l'authentification : {type(exc).__name__}: {exc}")
    st.exception(exc)
    st.stop()

name, authentication_status, username = authenticator.login(
    key="access-key-login", callback=remember_access
)

if authentication_status:
    authenticator.logout(button_name="Se déconnecter", location="sidebar")
    st.success(
        f"Accès accordé pour **{name or username}** — vous pouvez continuer."
    )
    player = (st.session_state.access_payload or {}).get("player", {})
    st.json(player)
    st.write(
        "Emoji :",
        (st.session_state.access_payload or {}).get("emoji", "—"),
        " · Phrase :",
        (st.session_state.access_payload or {}).get("phrase", "—"),
    )
elif authentication_status is False:
    st.error("Clé invalide. Vérifiez l'orthographe ou créez-en une nouvelle ci-dessous.")
else:
    st.info("Saisissez une clé d'accès ci-dessus pour déverrouiller la session.")

st.divider()

st.subheader("Besoin d'une clé ?")
st.write("Faites une demande ci-dessous ; la clé sera créée et stockée automatiquement dans Notion.")

with st.form("access_request_form"):
    req_name = st.text_input("Nom souhaité ou collectif")
    req_email = st.text_input("Email de contact")
    req_intent = st.text_area("Que voulez-vous explorer avec cette clé ?", height=120)
    req_mode = st.selectbox("Mode préféré", ["Non-linear", "Linear"])
    req_submit = st.form_submit_button("Créer la clé")
if req_submit:
    with st.status("🔄 Création de clé en cours...", expanded=True) as status_box:
        status_box.write("1/5 · Vérification des informations saisies.")
        if not req_email.strip():
            status_box.update(label="⚠️ Email manquant", state="error")
            st.error("Un email est requis pour vous recontacter.")
        elif not notion_repo:
            status_box.update(label="❌ Notion indisponible", state="error")
            st.error("Le dépôt Notion est indisponible ; création impossible pour le moment.")
        else:
            metadata = {
                "name": req_name.strip(),
                "email": req_email.strip(),
                "intent": req_intent.strip(),
                "mode": req_mode,
            }
            try:
                status_box.write("2/5 · Vérification de la session active.")
                with st.spinner("⏳ Création de la clé sur Notion..."):
                    status_box.write("3/5 · Génération de la clé et enregistrement Notion.")
                    access_key, _, payload = authenticator.register_user(metadata=metadata)
                status_box.write("4/5 · Construction des projections (emoji, phrase).")
            except Exception as exc:
                status_box.update(label="❌ Échec de création", state="error")
                st.error(f"Échec de création de la clé : {exc}")
            else:
                st.success("Clé créée. Conservez les détails ci-dessous.")
                st.code(access_key)
                st.write("Projection emoji :", payload.get("emoji"))
                st.write("Phrase secrète :", payload.get("phrase"))
                player_payload = payload.get("player") if isinstance(payload, dict) else {}
                nickname = ""
                role = "guest"
                if isinstance(player_payload, dict):
                    nickname = str(player_payload.get("nickname", "") or "")
                    role = str(player_payload.get("role", "guest") or "guest")
                status_box.write("5/5 · Génération de la carte PDF en couleur.")
                pdf_bytes = build_credentials_pdf(
                    access_key=str(access_key),
                    emoji=str(payload.get("emoji", "")),
                    phrase=str(payload.get("phrase", "")),
                    nickname=nickname,
                    role=role,
                    title="Carte d'acces cuisine",
                )
                filename = f"affranchis-cle-{datetime.now().strftime('%Y%m%d-%H%M%S')}.pdf"
                st.download_button(
                    "Télécharger la carte PDF",
                    data=pdf_bytes,
                    file_name=filename,
                    mime="application/pdf",
                    use_container_width=True,
                )
                status_box.update(label="✅ Clé prête et carte générée", state="complete")
                st.info(
                    "Les clés sont enregistrées dans Notion et utilisables immédiatement dans le formulaire ci-dessus."
                )

st.divider()

st.subheader("Valider une clé existante")
existing_key = st.text_input("Collez une clé d'accès / chaîne emoji / phrase secrète")
if st.button("Valider la clé", disabled=not existing_key.strip()):
    if not notion_repo:
        st.error("Le dépôt Notion est indisponible ; validation impossible.")
    else:
        try:
            canonical = normalize_access_key(existing_key)
        except ValueError as exc:
            st.error(str(exc))
        else:
            player = notion_repo.get_player_by_id(canonical)
            if not player:
                st.error("Clé introuvable.")
            else:
                st.success("Clé valide et bien stockée dans Notion.")
                st.json(player)
                st.write("Emoji :", hex_to_emoji(canonical))
                st.write("Phrase :", hex_to_phrase(canonical))
