from __future__ import annotations

from ui import (
    apply_theme,
    display_centered_prompt,
    microcopy,
    render_info_block,
    set_page,
    sidebar_debug_state,
)

import streamlit as st


def main() -> None:
    set_page()
    apply_theme()
    sidebar_debug_state()

    display_centered_prompt("Les Affranchi·e·s")
    st.markdown(
        """
### Les Affranchi·e·s est un collectif créé en 2024 pour réunir des personnes qui se disent _affranchies du pouvoir_ et des _rapports de pouvoir dominants_, et qui s’efforcent de les traverser avec vertu, détachement et discernement.
"""
    )
    st.divider()

    render_info_block(
        left_title="L'Idée",
        left_subtitle="",
        right_content=(
            "Créer un réseau d’entraide : lorsqu’on reconnaît un·e Affranchi·e, "
            "on sait qu’il ou elle est digne de confiance, fiable, et non mû·e "
            "par des dynamiques de domination."
        ),
    )

    st.markdown(
        """
### Nous organisons des rencontres, des événements, des discussions, des projets collectifs, et nous nous soutenons les un·e·s les autres dans nos entreprises respectives.
"""
    )
    microcopy("Une forme-plate pour se reconnaître, se relier, et agir.")

    col_back, col_enter = st.columns(2)
    with col_back:
        if st.button("Retour", use_container_width=True):
            st.switch_page("app.py")
    with col_enter:
        if st.button("Entrer", type="primary", use_container_width=True):
            st.switch_page("pages/01_Login.py")


if __name__ == "__main__":
    main()
