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
from services.presence import count_active_users
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
    active_12h = count_active_users(
        window_minutes=12 * 60, session_id=st.session_state.get("session_id", "")
    )
    st.metric("Affranchi·e·s actif·ve·s", active_12h, help="ces 12 dernières heures")
    cracks_globe_block(
        [],
        height=260,
        key="home-header-cracks",
        auto_rotate_speed=1.8,
    )
    # st.caption("La sphère sur la planète.")
    st.write("Navigation :")
    if st.button("L'Idée", use_container_width=True):
        st.switch_page("app.py")
    if st.button("Administration", use_container_width=True):
        st.switch_page("pages/07_Admin.py")
    if st.button("Préferences cuisine", use_container_width=True):
        st.switch_page("pages/03_Cuisine.py")
    if st.button("Mon espace participant", use_container_width=True):
        st.switch_page("pages/09_Participant.py")
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
        "Overview cuisine",
        use_container_width=True,
    ):
        st.switch_page("pages/08_Overview.py")

    st.button("Pose une question", disabled=True, use_container_width=True)
    if st.session_state.get("authentication_status"):
        authenticator.logout(button_name="Se déconnecter", location="sidebar")
    if repo and st.session_state.get("session_id"):
        questions = repo.list_questions(
            st.session_state["session_id"], status="approved"
        )
        st.caption(f"Préferences partagées : {len(questions)}")


if __name__ == "__main__":
    main()
