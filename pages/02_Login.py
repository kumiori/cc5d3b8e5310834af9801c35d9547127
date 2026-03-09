from __future__ import annotations

from datetime import datetime
import time
from typing import Any, Dict

import streamlit as st
import streamlit.components.v1 as components

from infra.app_context import get_authenticator, get_notion_repo
from infra.app_state import ensure_session_state, remember_access
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
ACCESS_SHOW_LOGIN_FORM = "access_show_login_form"


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


def _stream_chunk(text: str, *, punctuation_pause: float = 0.08):
    words = text.split(" ")
    for idx, word in enumerate(words):
        token = word
        if idx < len(words) - 1:
            token += " "
        yield token
        delay = 0.035
        if token.endswith((".", "!", "?")):
            delay += punctuation_pause
        elif token.endswith((",", ";", ":")):
            delay += punctuation_pause * 0.6
        time.sleep(delay)


def _stream_access_intro(skip: bool = False) -> None:
    if skip:
        st.markdown("## Accès")
        st.markdown("Chaque participant·e entre avec une clé personnelle.")
        st.markdown("Si tu en as déjà une, connecte-toi. Sinon, crée-la ici.")
        return
    st.write_stream(_stream_chunk("### Accès", punctuation_pause=0.14))
    time.sleep(0.48)
    st.write_stream(
        _stream_chunk("Chaque participant·e entre avec une clé personnelle.")
    )
    time.sleep(0.4)
    st.write_stream(
        _stream_chunk("Si tu en as déjà une, connecte-toi. Sinon, crée-la ici.")
    )


def _render_access_cta() -> None:
    st.session_state.setdefault("access_intro_played", False)
    if not st.session_state["access_intro_played"]:
        if st.button("Afficher tout", key="access-intro-skip"):
            st.session_state["access_intro_played"] = True
            st.rerun()
        _stream_access_intro(skip=False)
        st.session_state["access_intro_played"] = True
    else:
        _stream_access_intro(skip=True)

    col_login, col_create = st.columns(2)

    with col_login:
        st.markdown("## J’ai déjà une clé")
        st.caption("Je suis prêt·e à entrer.")
        if st.button(
            "Se connecter",
            type="secondary",
            use_container_width=True,
            key="splash-go-login",
        ):
            st.session_state[ACCESS_SHOW_LOGIN_FORM] = True

    with col_create:
        st.markdown("## Créer une clé")
        st.caption("Générer mon accès personnel.")
        if st.button(
            "Créer ma clé",
            type="primary",
            use_container_width=True,
            key="splash-create-key",
        ):
            st.session_state[OPEN_MINT_KEY] = True
            st.rerun()

    with st.expander("Qu’est-ce qu’une clé ?", expanded=False):
        st.write(
            "La clé permet de reconnaître un·e affranchi·e sur cette forme-plate, "
            "sans exposer une identité publique."
        )


def _render_login_panel(authenticator: Any) -> None:
    if st.session_state.get(MINT_RESULT_KEY):
        return
    if not bool(st.session_state.get(ACCESS_SHOW_LOGIN_FORM, False)):
        return
    st.markdown("### Connexion")
    _, authentication_status, _ = authenticator.login(
        location="main",
        key="access-login-form",
        callback=remember_access,
    )
    if authentication_status:
        st.success("Accès validé.")
        if st.button(
            "Continuer",
            type="primary",
            use_container_width=True,
            key="access-login-continue-btn",
        ):
            st.switch_page("pages/03_Cuisine.py")


def _render_mint_panel(authenticator: Any) -> None:
    st.session_state.setdefault(MINT_RESULT_KEY, None)
    open_mint = bool(st.session_state.pop(OPEN_MINT_KEY, False))

    with st.expander("Détails de la clé", expanded=open_mint):
        st.caption(
            "La clé est personnelle. Si tu indiques un email, il sert uniquement au rappel."
        )
        st.markdown("### Crée ta clé d’accès")

        with st.form("splash-mint-token-form"):
            mint_name = st.text_input("Nom ou pseudo", key="splash-mint-name")
            # mint_intent = st.text_input(
            #     "What is your motivation? (optional)",
            #     key="splash-mint-intent",
            #     max_chars=120,
            # )
            mint_intent = ""
            mint_email = st.text_input(
                "Email (optionnel, pour retrouver la clé)",
                key="splash-mint-email",
            )
            mint_submit = st.form_submit_button(
                "Créer ma clé",
                type="primary",
                use_container_width=True,
            )

        if not mint_submit:
            return

        with st.spinner("Création de la clé en cours..."):
            try:
                access_key, _, payload = authenticator.register_user(
                    metadata={
                        "name": mint_name,
                        "intent": mint_intent,
                        "email": mint_email,
                        "role": "Player",
                    }
                )
                st.session_state[MINT_RESULT_KEY] = _build_mint_result(
                    access_key=str(access_key or ""),
                    payload=payload,
                    mint_name=mint_name,
                )
                st.session_state["access_key_created_stream"] = False
                st.session_state["access_mint_key_copied"] = False
                st.rerun()
            except Exception as exc:
                st.error(f"Minting failed: {exc}")


def _render_mint_result(authenticator: Any) -> None:
    mint_result = st.session_state.get(MINT_RESULT_KEY)
    if not mint_result:
        return

    st.success(
        f"La clée est prète, cela a pris {st.session_state.get('mint_duration', 'un certain temps')}."
    )
    st.session_state.setdefault("access_key_created_stream", False)
    if not st.session_state["access_key_created_stream"]:
        st.write_stream(_stream_chunk("Clé créée", punctuation_pause=0.14))
        time.sleep(0.4)
        st.write_stream(_stream_chunk("Tu peux maintenant entrer dans la session."))
        time.sleep(0.36)
        st.write_stream(
            _stream_chunk(
                "Copie-la, garde cette clé, et entre dans la plateforme. Elle te permettra de revenir et de poursuivre."
            )
        )
        st.session_state["access_key_created_stream"] = True
    else:
        st.markdown("### Clé créée")
        st.markdown("Tu peux maintenant entrer dans la session.")
        st.markdown("Garde cette clé. Elle te permettra de revenir et de poursuivre.")

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

    # with st.container(border=True,):
    #     st.markdown("#### Clé d’accès")
    #     st.code(str(mint_result.get("access_key", "")), language="text")

    st.session_state.setdefault("access_mint_key_copied", False)
    key_copied = bool(st.session_state.get("access_mint_key_copied", False))

    col_continue, col_copy = st.columns(2)
    with col_continue:
        if st.button(
            "Continuer",
            type="primary" if key_copied else "secondary",
            use_container_width=True,
            key="access-mint-continue-btn",
        ):
            full_key = str(mint_result.get("access_key", "")).strip()
            auto_login_ok = False
            if full_key:
                try:
                    auto_login_ok = bool(
                        authenticator.auth_model.login(
                            full_key, callback=remember_access
                        )
                    )
                    if auto_login_ok:
                        authenticator.cookie_controller.set_cookie()
                except Exception:
                    auto_login_ok = False

            if auto_login_ok:
                st.switch_page("pages/03_Cuisine.py")
            else:
                st.session_state["login_access_key_prefill"] = str(
                    mint_result.get("emoji4", "")
                ).strip()
                st.session_state["login_access_key_prefill_notice"] = (
                    "Connexion automatique indisponible. Ta clé emoji-4 est préremplie."
                )
                st.session_state[ACCESS_SHOW_LOGIN_FORM] = True
                st.error("Impossible de te connecter automatiquement. Entre la clé.")
                st.rerun()
    with col_copy:
        if st.button(
            "Copier la clé pour sauvegarder",
            use_container_width=True,
            key="access-mint-copy-btn",
            type="secondary" if key_copied else "primary",
        ):
            key_text = (
                str(mint_result.get("access_key", ""))
                .replace("\\", "\\\\")
                .replace("'", "\\'")
            )
            components.html(
                f"<script>navigator.clipboard.writeText('{key_text}');</script>",
                height=0,
            )
            st.session_state["access_mint_key_copied"] = True
            st.toast("Clé copiée.")


def main() -> None:
    set_page()
    apply_theme()
    ensure_session_state()
    sidebar_debug_state()

    microcopy("Entrée d’accès.")
    repo = get_notion_repo()
    authenticator = get_authenticator(repo)

    _render_access_cta()
    _render_login_panel(authenticator)
    _render_mint_panel(authenticator)
    _render_mint_result(authenticator)


if __name__ == "__main__":
    main()
