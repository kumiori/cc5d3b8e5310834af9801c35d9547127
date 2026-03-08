from __future__ import annotations

import hashlib
from datetime import datetime

import streamlit as st

from infra.app_context import (
    get_active_session,
    get_authenticator,
    get_auth_runtime_config,
    get_notion_repo,
)
from infra.credentials_pdf import build_credentials_pdf
from infra.app_state import (
    ensure_auth,
    ensure_session_state,
    remember_access,
    set_session,
    mint_anon_token,
)
from infra.key_codec import split_emoji_symbols

from ui import (
    apply_theme,
    heading,
    microcopy,
    set_page,
    sidebar_debug_state,
    cracks_globe_block,
    display_centered_prompt,
    render_info_block,
)


def main() -> None:
    set_page()
    apply_theme()
    ensure_session_state()
    sidebar_debug_state()
    # heading("Welcome")
    display_centered_prompt("Les Affranchi·e·s.")
    st.markdown(
        """
### Les Affranchi·e·s est un collectif créé en 2024 pour réunir des personnes qui se disent _affranchies du pouvoir_ et des _rapports de pouvoir dominants_, et qui s’efforcent de les traverser avec vertu, détachement et discernement.        """
    )
    st.divider()

    render_info_block(
        left_title="L'Idée",
        left_subtitle="\n",
        right_content="Est de créer un réseau d’entraide : lorsqu’on reconnaît un·e Affranchi·e, on sait qu’il ou elle est digne de confiance, fiable, précisément parce qu’il ou elle n’est pas mû·e par des dynamiques de domination.",
    )

    st.markdown(
        """### Nous organisons des rencontres, des evenements, des discussions, des projets collectifs, et nous nous soutenons les un·e·s les autres dans nos entreprises respectives.
        """
    )
    repo = get_notion_repo()
    authenticator = get_authenticator(repo)
    auth_cfg = get_auth_runtime_config()
    key_hash_prefix = hashlib.sha256(
        auth_cfg["cookie_key"].encode("utf-8")
    ).hexdigest()[:12]

    with st.sidebar.expander("Debug: Auth cookie", expanded=False):
        st.code(
            (
                f"cookie_source={auth_cfg['source']}\n"
                f"cookie_name={auth_cfg['cookie_name']}\n"
                f"cookie_expiry_days={auth_cfg['cookie_expiry_days']}\n"
                f"cookie_key_len={len(auth_cfg['cookie_key'])}\n"
                f"cookie_key_sha256_prefix={key_hash_prefix}\n"
                f"default_session_code={auth_cfg['default_session_code']}"
            )
        )

    name, authentication_status, _ = ensure_auth(
        authenticator, callback=remember_access, key="access-key-login", location="main"
    )
    if authentication_status:
        authenticator.logout(button_name="Logout", location="sidebar")

    if authentication_status:
        st.info(
            "Nous avons préparé un cookie pour vous pendant 30 minutes. Cela vous permet de rester connecté pendant que vous naviguez."
        )
        session = get_active_session(repo)
        if session:
            set_session(session.get("id", ""), session.get("session_code", "Session"))
        salt = st.secrets.get("anon_salt", "iceicebaby")
        anon_token = mint_anon_token(
            st.session_state.get("session_id", ""),
            st.session_state.get("player_access_key", ""),
            salt,
        )
        st.session_state["anon_token"] = anon_token
        st.success(f"Accès accordé pour {name or 'collaborateur'}.")
        if st.button("Entre", type="secondary", use_container_width=True):
            st.switch_page("pages/02_Home.py")
    elif authentication_status is False:
        st.error("Clé invalide ou ambiguë. Essaye la chaine complète ou plus d'émojis.")


if __name__ == "__main__":
    with open("assets/styles.css", "r") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
        st.write(f.read())

    main()
