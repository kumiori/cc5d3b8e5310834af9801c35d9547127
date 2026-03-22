from __future__ import annotations

import streamlit as st

from config import settings


def _visibility(internal: bool) -> str:
    """Hide internal/testing pages from navigation in production only."""
    if settings.is_production and internal:
        return "hidden"
    return "visible"


def main() -> None:
    """Application router based on explicit Streamlit navigation."""
    pages = {
        "Entrée": [
            st.Page(
                "pages/00_Idee.py",
                title="L'Idée",
                icon="🪶",
                default=True,
                url_path="idee",
            ),
            st.Page("pages/01_Splash.py", title="Splash", icon="✨", url_path="splash"),
            st.Page("pages/02_Login.py", title="Login", icon="🔑", url_path="login"),
            st.Page(
                "pages/10_Access.py",
                title="Access (legacy)",
                icon="🧪",
                url_path="access-legacy",
                visibility=_visibility(True),
            ),
        ],
        "Session": [
            st.Page("pages/04_Home.py", title="Lobby", icon="🏠", url_path="home"),
            st.Page("pages/03_Cuisine.py", title="Cuisine", icon="🍲", url_path="cuisine"),
            st.Page(
                "pages/09_Participant.py",
                title="Participant",
                icon="🧾",
                url_path="participant",
            ),
            st.Page(
                "pages/08_Overview.py",
                title="Overview",
                icon="📊",
                url_path="overview",
            ),
        ],
        "Ops": [
            st.Page("pages/07_Admin.py", title="Admin", icon="🛠️", url_path="admin"),
            st.Page(
                "pages/test_key_recovery.py",
                title="Test · Recovery",
                icon="🧪",
                url_path="test-key-recovery",
                visibility=_visibility(True),
            ),
            st.Page(
                "pages/test_txs.py",
                title="Test · Transactions",
                icon="🧪",
                url_path="test-txs",
                visibility=_visibility(True),
            ),
            st.Page(
                "pages/test_access_keys.py",
                title="Test · Access keys",
                icon="🧪",
                url_path="test-access-keys",
                visibility=_visibility(True),
            ),
            st.Page(
                "pages/40_affranchis_cuisine_test.py",
                title="Test · Cuisine legacy",
                icon="🧪",
                url_path="test-cuisine-legacy",
                visibility=_visibility(True),
            ),
        ],
    }
    router = st.navigation(pages, position="sidebar", expanded=False)
    router.run()


if __name__ == "__main__":
    main()
