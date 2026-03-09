from __future__ import annotations

import time

import streamlit as st

from infra.app_state import ensure_session_state

from ui import (
    apply_theme,
    set_page,
    sidebar_debug_state,
)


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


def _render_streamed_intro(skip: bool = False) -> None:
    if skip:
        st.markdown("## Les Affranchi·e·s")
        st.markdown(
            "Un collectif pour se reconnaître, se relier, et agir sans logique de domination."
        )
        st.markdown(
            "Cette forme-plate te permet d’entrer dans une session et d’y prendre part."
        )
        return

    st.write_stream(_stream_chunk("### Les Affranchi·e·s", punctuation_pause=0.15))
    time.sleep(0.52)
    st.write_stream(
        _stream_chunk(
            "Un collectif pour se reconnaître, se relier, et agir au-delà des logiques de domination."
        )
    )
    time.sleep(0.42)
    st.write_stream(
        _stream_chunk(
            "Cette forme-plate te permet d’entrer dans une session et d’y prendre part."
        )
    )


def main() -> None:
    set_page()
    apply_theme()
    ensure_session_state()
    sidebar_debug_state()

    st.session_state.setdefault("splash_intro_played", False)
    if not st.session_state["splash_intro_played"]:
        if st.button("Afficher tout", key="landing-skip-stream"):
            st.session_state["splash_intro_played"] = True
            st.rerun()
        _render_streamed_intro()
        st.session_state["splash_intro_played"] = True
    else:
        _render_streamed_intro(skip=True)

    st.divider()
    if st.button("Je continue", type="primary", use_container_width=True):
        st.switch_page("pages/01_Login.py")


if __name__ == "__main__":
    main()
