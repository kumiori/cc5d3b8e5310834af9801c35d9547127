from __future__ import annotations

import streamlit as st

from infra.app_context import get_authenticator, get_notion_repo
from infra.app_state import (
    ensure_auth,
    ensure_session_context,
    ensure_session_state,
    remember_access,
    require_login,
)
from ui import (
    apply_theme,
    heading,
    microcopy,
    set_page,
    sidebar_debug_state,
    display_centered_prompt,
    cracks_globe_block,
)


def main() -> None:
    set_page()
    apply_theme()
    ensure_session_state()
    repo = get_notion_repo()
    authenticator = get_authenticator(repo)
    ensure_auth(authenticator, callback=remember_access, key="home-login")
    ensure_session_context(repo)
    require_login()
    sidebar_debug_state()

    heading("Salon de session")
    session_title = st.session_state.get("session_title") or "Session active"
    microcopy(session_title)
    cracks_globe_block(
        [],
        height=260,
        key="home-header-cracks",
        auto_rotate_speed=1.8,
    )

    st.write("Actions rapides")
    if st.button("Préferences cuisine", use_container_width=True):
        st.switch_page("pages/01_Cuisine.py")
    if st.button(
        "Décisions (après dinêr, si affinités)", disabled=True, use_container_width=True
    ):
        st.switch_page("pages/05_Decisions.py")
    if st.button(
        "Coordination (après dinêr, si affinités)",
        disabled=True,
        use_container_width=True,
    ):
        st.switch_page("pages/06_Coordination.py")
    if st.button(
        "Tableau de bord (après dinêr, si affinités)",
        disabled=True,
        use_container_width=True,
    ):
        st.switch_page("pages/03_Resonance.py")

    st.button("Pose une question", disabled=True, use_container_width=True)
    if st.session_state.get("authentication_status"):
        authenticator.logout(button_name="Se déconnecter", location="sidebar")
    if repo and st.session_state.get("session_id"):
        questions = repo.list_questions(
            st.session_state["session_id"], status="approved"
        )
        st.caption(f"Questions publiées : {len(questions)}")


if __name__ == "__main__":
    main()
