from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

import streamlit as st

from infra.app_context import get_authenticator, get_notion_repo
from infra.app_state import ensure_session_state
from infra.credentials_pdf import build_credentials_pdf
from infra.key_codec import split_emoji_symbols
from ui import (
    apply_theme,
    display_centered_prompt,
    microcopy,
    set_page,
    sidebar_debug_state,
)

MINT_RESULT_KEY = "splash_mint_result"
OPEN_MINT_KEY = "focus_mint_token"


def _build_mint_result(
    access_key: str, payload: Dict[str, Any], mint_name: str
) -> Dict[str, Any]:
    emoji_value = str(payload.get("emoji", ""))
    phrase_value = str(payload.get("phrase", ""))
    emoji_symbols = split_emoji_symbols(emoji_value)
    suffix4 = "".join(emoji_symbols[-4:]) if len(emoji_symbols) >= 4 else emoji_value
    suffix6 = "".join(emoji_symbols[-6:]) if len(emoji_symbols) >= 6 else emoji_value
    pdf_bytes = build_credentials_pdf(
        access_key=access_key,
        emoji=emoji_value,
        phrase=phrase_value,
        nickname=str(mint_name or ""),
        role="Player",
        title="Access Card",
    )
    filename = f"affranchis-key-{datetime.now().strftime('%Y%m%d-%H%M%S')}.pdf"
    return {
        "access_key": access_key,
        "emoji": emoji_value,
        "phrase": phrase_value,
        "emoji4": suffix4,
        "emoji6": suffix6,
        "pdf_bytes": pdf_bytes,
        "filename": filename,
    }


def _render_access_cta() -> None:
    display_centered_prompt("Access")
    if st.button("Voir l’Idée", use_container_width=False, key="splash-go-idee"):
        st.switch_page("app.py")
    st.markdown("### This session ...")
    st.markdown("## Each player enters using a unique access key.")

    col_login, col_create = st.columns(2)

    with col_login:
        st.markdown("## I already have an access key")
        st.caption("And I am ready to participate.")
        if st.button(
            "🔑 Go to Login",
            type="secondary",
            use_container_width=True,
            key="splash-go-login",
        ):
            st.switch_page("pages/02_Login.py")

    with col_create:
        st.markdown("## Create a new access key")
        st.caption("Generate your personal token to participate.")
        if st.button(
            "✨ Create Access Key",
            type="primary",
            use_container_width=True,
            key="splash-create-key",
        ):
            st.session_state[OPEN_MINT_KEY] = True
            st.rerun()


def _render_mint_panel(authenticator: Any) -> None:
    st.session_state.setdefault(MINT_RESULT_KEY, None)
    open_mint = bool(st.session_state.pop(OPEN_MINT_KEY, False))

    with st.expander("Mint access token", expanded=open_mint):
        st.caption(
            "All sessions are designed to be anonymous. Your access key is personal and "
            "must be stored securely. If you add an email, it is used only to send a credentials reminder."
        )
        st.markdown("### Now login with your access key to join the lobby")

        with st.form("splash-mint-token-form"):
            mint_name = st.text_input("Name or nickname", key="splash-mint-name")
            mint_intent = st.text_input(
                "What is your motivation? (optional)",
                key="splash-mint-intent",
                max_chars=120,
            )
            mint_email = st.text_input(
                "Email (optional, for credentials reminder)",
                key="splash-mint-email",
            )
            mint_submit = st.form_submit_button(
                "Create Access Key",
                type="primary",
                use_container_width=True,
            )

        if not mint_submit:
            return

        with st.status("Minting access key...", expanded=True) as status:
            try:
                status.write("1/3 · Creating key in database.")
                access_key, _, payload = authenticator.register_user(
                    metadata={
                        "name": mint_name,
                        "intent": mint_intent,
                        "email": mint_email,
                        "role": "Player",
                    }
                )
                status.write("2/3 · Preparing access card PDF.")
                st.session_state[MINT_RESULT_KEY] = _build_mint_result(
                    access_key=str(access_key or ""),
                    payload=payload,
                    mint_name=mint_name,
                )
                status.write("3/3 · Ready.")
                status.update(label="Minting complete", state="complete")
            except Exception as exc:
                status.update(label="Minting failed", state="error")
                st.error(f"Minting failed: {exc}")


def _render_mint_result() -> None:
    mint_result = st.session_state.get(MINT_RESULT_KEY)
    if not mint_result:
        return

    st.success("Token minted.")
    st.markdown("### Your handy access key (emoji-4)")
    st.markdown(
        f"<div style='font-size:4.1rem;line-height:1.2;text-align:center'>{mint_result.get('emoji4', '—')}</div>",
        unsafe_allow_html=True,
    )
    st.caption(
        "A 22-emojis string unique key has been generated. In most cases, its last 4 emoji are sufficient to log in. "
        "Create a story around them to remember, or store the full key securely."
    )

    with st.expander("Show full credentials", expanded=False):
        st.code(f"Access key: {mint_result.get('access_key', '')}", language="text")
        st.write("Emoji:", mint_result.get("emoji", "—"))
        st.write("Phrase:", mint_result.get("phrase", "—"))
        st.write("Emoji suffix 4:", mint_result.get("emoji4", "—"))
        st.write("Emoji suffix 6:", mint_result.get("emoji6", "—"))

    st.download_button(
        "Download Access Card PDF",
        data=mint_result.get("pdf_bytes", b""),
        file_name=mint_result.get("filename", "affranchis-key.pdf"),
        mime="application/pdf",
        use_container_width=True,
        key="splash-mint-download-pdf",
    )

    if st.button(
        "Go to Login with emoji-4", type="secondary", use_container_width=True
    ):
        st.session_state["login_access_key_prefill"] = str(
            mint_result.get("emoji4", "")
        ).strip()
        st.session_state["login_access_key_prefill_notice"] = (
            "Going to login. Reloading with your 4-emoji key prefilled."
        )
        st.info("Going to login. Reloading now...")
        st.switch_page("pages/02_Login.py")


def main() -> None:
    set_page()
    apply_theme()
    ensure_session_state()
    sidebar_debug_state()

    microcopy("This is the entry point for access.")
    repo = get_notion_repo()
    authenticator = get_authenticator(repo)

    _render_access_cta()
    _render_mint_panel(authenticator)
    _render_mint_result()


if __name__ == "__main__":
    main()
