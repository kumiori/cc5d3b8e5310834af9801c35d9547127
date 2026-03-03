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

    heading("Welcome")
    microcopy("Enter your access token, emoji tail, or passphrase.")

    display_centered_prompt("Les Affranchi·e·s.")
    st.markdown(
        """
### Les A. est un collectif créé en 2024 pour réunir des personnes qui se disent _affranchies du pouvoir_ et des _rapports de pouvoir dominants_, et qui s’efforcent de les traverser avec vertu, détachement et discernement.        """
    )
    st.divider()

    render_info_block(
        left_title="L'Idée",
        left_subtitle="\n",
        right_content="est de créer un réseau d’entraide : lorsqu’on reconnaît un·e Affranchi·e, on sait qu’il ou elle est digne de confiance, fiable, précisément parce qu’il ou elle n’est pas mû·e par des dynamiques de domination.",
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

    with st.expander("Mint access token"):
        st.caption("Create a single access token for a new collaborator.")
        mint_role = st.selectbox(
            "Role", ["Player", "Organiser"], index=0, key="mint-role"
        )
        mint_name = st.text_input("Display name (optional)", key="mint-name")
        if st.button("Mint token", type="secondary", use_container_width=True):
            with st.status(
                "🔄 Création du jeton en cours...", expanded=True
            ) as status_box:
                status_box.write("1/5 · Vérification des paramètres.")
                try:
                    status_box.write("2/5 · Vérification de la session active.")
                    with st.spinner("⏳ Création du jeton sur Notion..."):
                        status_box.write(
                            "3/5 · Génération de la clé et enregistrement Notion."
                        )
                        access_key, _, payload = authenticator.register_user(
                            metadata={"name": mint_name, "role": mint_role}
                        )
                    status_box.write(
                        "4/5 · Construction des projections (emoji, phrase)."
                    )
                except Exception as exc:
                    status_box.update(
                        label="❌ Échec pendant la création du jeton", state="error"
                    )
                    st.error(f"Minting failed: {exc}")
                else:
                    st.success("Token minted.")
                    st.code(access_key or "", language="text")
                    emoji_value = str(payload.get("emoji", ""))
                    emoji_symbols = split_emoji_symbols(emoji_value)
                    suffix_4 = (
                        "".join(emoji_symbols[-4:]) if len(emoji_symbols) >= 4 else ""
                    )
                    suffix_6 = (
                        "".join(emoji_symbols[-6:]) if len(emoji_symbols) >= 6 else ""
                    )
                    st.write("Emoji:", emoji_value or "—")
                    st.write("Phrase:", payload.get("phrase", "—"))
                    st.write("Emoji suffix 4:", suffix_4 or "—")
                    st.write("Emoji suffix 6:", suffix_6 or "—")
                    status_box.write("5/5 · Génération de la carte PDF en couleur.")
                    pdf_bytes = build_credentials_pdf(
                        access_key=str(access_key or ""),
                        emoji=emoji_value,
                        phrase=str(payload.get("phrase", "")),
                        nickname=str(mint_name or ""),
                        role=str(mint_role or "guest"),
                        title="Carte d'acces",
                    )
                    filename = (
                        f"affranchis-cle-{datetime.now().strftime('%Y%m%d-%H%M%S')}.pdf"
                    )
                    st.download_button(
                        "Télécharger la carte PDF",
                        data=pdf_bytes,
                        file_name=filename,
                        mime="application/pdf",
                        use_container_width=True,
                    )
                    status_box.update(
                        label="✅ Jeton prêt et carte générée", state="complete"
                    )

    if authentication_status:
        st.info(
            "We baked a cookie for you for 30 minutes. This keeps you signed in while you navigate."
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
        st.success(f"Access granted for {name or 'collaborator'}.")
        if st.button("Enter lobby", type="secondary", use_container_width=True):
            st.switch_page("pages/02_Home.py")
    elif authentication_status is False:
        st.error("Key invalid or ambiguous. Try full hash or more emoji.")


if __name__ == "__main__":
    with open("assets/styles.css", "r") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
        st.write(f.read())

    main()
