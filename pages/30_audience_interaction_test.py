from __future__ import annotations

import re
import uuid
import os
from collections import Counter
from typing import Any, Optional

import pandas as pd
import streamlit as st

from infra.app_context import get_active_session, get_notion_repo
from infra.app_state import ensure_session_state
from models.catalog import QUESTION_CATALOG
from models.questions import Question
from repositories.base import InteractionRepository
from repositories.interaction_repo import (
    NotionInteractionRepository,
    SQLiteInteractionRepository,
)
from services.selection import question_count_for_depth, select_questions
from ui import apply_theme, heading, microcopy, set_page, sidebar_debug_state


TEXT_OPTIONS = [
    "cryosphere_v0",
    "thresholds_v0",
    "living-ice_v0",
]

TEXT_SNIPPETS = {
    "cryosphere_v0": "Ice systems store memory across generations; their fracture patterns expose social time scales.",
    "thresholds_v0": "Under threshold dynamics, incremental pressure can trigger discontinuous shifts in shared futures.",
    "living-ice_v0": "Glaciers can be approached as living political mirrors that reorganize responsibility across distance.",
}


def _ensure_local_state() -> None:
    ensure_session_state()
    st.session_state.setdefault("interaction_device_id", uuid.uuid4().hex[:16])
    st.session_state.setdefault("interaction_answers", {})
    st.session_state.setdefault("interaction_index", 0)
    st.session_state.setdefault("interaction_submitted", False)
    st.session_state.setdefault("interaction_seed_sig", "")


def debug(msg: str) -> None:
    st.markdown(msg)


def _actor_identity() -> tuple[str, Optional[str]]:
    player_page_id = st.session_state.get("player_page_id", "")
    if player_page_id:
        return player_page_id, player_page_id
    device_id = st.session_state.get("interaction_device_id", "")
    return device_id, None


def _build_repository() -> tuple[InteractionRepository, str]:
    sqlite_repo = SQLiteInteractionRepository("data/interaction_v0.sqlite")
    repo = get_notion_repo()
    db_id = os.getenv("AFF_RESPONSES_DB_ID", "")
    if not repo or not db_id:
        return sqlite_repo, "SQLite fallback"
    try:
        return NotionInteractionRepository(repo, str(db_id)), "Notion"
    except Exception as exc:
        st.warning(f"Stockage Notion indisponible, bascule vers SQLite : {exc}")
        return sqlite_repo, "SQLite fallback"


def _resolve_sessions() -> tuple[list[dict[str, str]], str]:
    repo = get_notion_repo()
    if not repo:
        sid = st.session_state.get("session_id") or "local-session"
        return [{"id": sid, "label": sid}], sid

    sessions = repo.list_sessions(limit=100)
    active = get_active_session(repo)
    active_id = (active or {}).get("id") or st.session_state.get("session_id", "")
    options = []
    for session in sessions:
        sid = session.get("id")
        code = session.get("session_code") or "Session"
        if sid:
            options.append({"id": sid, "label": code})
    if not options and active_id:
        options.append({"id": active_id, "label": "Active Session"})
    if not options:
        options.append({"id": "local-session", "label": "local-session"})
    return options, active_id or options[0]["id"]


def _is_answer_valid(question: Question, value: Any) -> bool:
    if question.qtype == "single":
        return isinstance(value, str) and value.strip() != ""
    if question.qtype == "multi":
        return isinstance(value, list) and len(value) > 0
    if question.qtype == "text":
        txt = str(value or "").strip()
        return 0 < len(txt) <= 24
    return False


def _normalize_answer_for_storage(question: Question, value: Any) -> Any:
    if question.qtype == "text":
        return str(value or "").strip()
    if question.qtype == "multi":
        return [str(v) for v in (value or [])]
    return str(value or "")


def _normalize_word_token(text: str) -> str:
    lowered = text.strip().lower()
    lowered = re.sub(r"[^a-z\- ]+", "", lowered)
    lowered = re.sub(r"\s+", " ", lowered).strip()
    return lowered


def _answered_count(questions: list[Question], answers: dict[str, Any]) -> int:
    return sum(1 for q in questions if _is_answer_valid(q, answers.get(q.id)))


def _render_question(question: Question, answers: dict[str, Any]) -> None:
    with st.container(border=True):
        st.markdown(f"### {question.prompt}")
        st.caption(f"Catégorie : {question.category}")

        widget_key = f"interaction_widget_{question.id}"
        if question.qtype == "single":
            options = ["Choisir une option"] + (question.options or [])
            existing = answers.get(question.id)
            idx = options.index(existing) if existing in options else 0
            selected = st.radio(" ", options, index=idx, key=widget_key, label_visibility="collapsed")
            answers[question.id] = "" if selected == "Choisir une option" else selected

        elif question.qtype == "multi":
            existing = answers.get(question.id) if isinstance(answers.get(question.id), list) else []
            selected = st.multiselect(
                "Choisissez jusqu'à 2",
                question.options or [],
                default=existing,
                max_selections=question.max_select or 2,
                key=widget_key,
            )
            answers[question.id] = selected

        else:
            existing = str(answers.get(question.id, ""))
            text = st.text_input(
                "Un mot (max 24 caractères)",
                value=existing,
                max_chars=24,
                key=widget_key,
                help="Le mot sera mis en minuscules pour les statistiques agrégées.",
            )
            answers[question.id] = text


def _render_aggregates(rows: list[dict[str, Any]], questions: list[Question]) -> None:
    st.subheader("Résultats agrégés (session en cours)")
    st.caption("Comptages anonymisés des réponses envoyées.")

    rows_by_item: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        rows_by_item.setdefault(str(row.get("item_id", "")), []).append(row)

    for question in questions:
        st.markdown(f"#### {question.prompt}")
        subset = rows_by_item.get(question.id, [])
        if not subset:
            st.caption("Aucune réponse pour le moment.")
            continue

        if question.qtype == "single":
            counter = Counter()
            for row in subset:
                value = row.get("value")
                if isinstance(value, str) and value:
                    counter[value] += 1
            if not counter:
                st.caption("Aucune réponse pour le moment.")
                continue
            df = pd.DataFrame(
                {"option": list(counter.keys()), "count": list(counter.values())}
            ).sort_values("count", ascending=False)
            st.bar_chart(df.set_index("option"))

        elif question.qtype == "multi":
            counter = Counter()
            for row in subset:
                value = row.get("value")
                if isinstance(value, list):
                    for item in value:
                        if item:
                            counter[str(item)] += 1
            if not counter:
                st.caption("Aucune réponse pour le moment.")
                continue
            df = pd.DataFrame(
                {"option": list(counter.keys()), "count": list(counter.values())}
            ).sort_values("count", ascending=False)
            st.bar_chart(df.set_index("option"))

        else:
            counter = Counter()
            for row in subset:
                value = row.get("value")
                if isinstance(value, str):
                    normalized = _normalize_word_token(value)
                    if normalized:
                        counter[normalized] += 1
            if not counter:
                st.caption("Aucune réponse pour le moment.")
                continue
            top_words = counter.most_common(10)
            st.table(pd.DataFrame(top_words, columns=["mot", "total"]))


def main() -> None:
    st.set_page_config(page_title="TEST · Interaction", page_icon="🧪")
    set_page()
    apply_theme()
    sidebar_debug_state()
    _ensure_local_state()

    heading("TEST · Interaction")
    microcopy("Cela prend environ 2 minutes. Les réponses sont agrégées après l'envoi.")
    debug("🔄 Initialisation de la page de test d'interaction.")

    sessions, default_session_id = _resolve_sessions()
    debug(f"🧭 Sessions chargées : **{len(sessions)}** option(s) disponibles.")
    session_ids = [s["id"] for s in sessions]
    default_idx = session_ids.index(default_session_id) if default_session_id in session_ids else 0
    selected_session_id = st.selectbox(
        "SESSION_ID",
        session_ids,
        index=default_idx,
        format_func=lambda sid: next((s["label"] for s in sessions if s["id"] == sid), sid),
    )
    text_id = st.selectbox("TEXT_ID (optionnel)", TEXT_OPTIONS, index=0)
    st.caption(TEXT_SNIPPETS.get(text_id, ""))
    depth = st.slider("Profondeur des questions", min_value=1, max_value=10, value=5, step=1)

    actor_id, player_id = _actor_identity()
    debug("👤 Identité interaction prête (participant ou appareil).")
    seed_key = f"{selected_session_id}:{actor_id}"
    selected_questions = select_questions(depth=depth, seed_key=seed_key, catalog=QUESTION_CATALOG)
    debug(f"🧩 Questions sélectionnées : **{len(selected_questions)}**.")
    answers: dict[str, Any] = st.session_state["interaction_answers"]

    signature = f"{selected_session_id}|{actor_id}|{depth}|{text_id}|{','.join(q.id for q in selected_questions)}"
    if st.session_state["interaction_seed_sig"] != signature:
        debug("♻️ Contexte modifié : recalcul de la sélection et réinitialisation de la navigation.")
        st.session_state["interaction_seed_sig"] = signature
        st.session_state["interaction_index"] = 0
        st.session_state["interaction_submitted"] = False
        allowed_ids = {q.id for q in selected_questions}
        st.session_state["interaction_answers"] = {
            k: v for k, v in answers.items() if k in allowed_ids
        }
        answers = st.session_state["interaction_answers"]

    total = len(selected_questions)
    answered = _answered_count(selected_questions, answers)
    st.progress(answered / total if total else 0.0)
    st.caption(f"Progression : {answered} / {total} répondues")
    st.caption(
        f"La profondeur {depth} affiche {question_count_for_depth(depth)} questions. "
        f"La sélection reste stable pour cette session et cette personne."
    )

    index = min(st.session_state["interaction_index"], max(total - 1, 0))
    st.session_state["interaction_index"] = index
    question = selected_questions[index]
    _render_question(question, answers)

    st.markdown(
        """
<style>
.interaction-footer {
  position: sticky;
  bottom: 0;
  padding: 0.75rem 0;
  background: var(--background-color);
  border-top: 1px solid rgba(120, 120, 120, 0.3);
  z-index: 20;
}
</style>
        """,
        unsafe_allow_html=True,
    )
    st.markdown('<div class="interaction-footer">', unsafe_allow_html=True)
    back_col, next_col, submit_col = st.columns(3)
    with back_col:
        if st.button("Retour", use_container_width=True, disabled=index == 0):
            st.session_state["interaction_index"] = max(0, index - 1)
            st.rerun()
    with next_col:
        next_disabled = index >= total - 1 or not _is_answer_valid(question, answers.get(question.id))
        if st.button("Suivant", use_container_width=True, disabled=next_disabled):
            st.session_state["interaction_index"] = min(total - 1, index + 1)
            st.rerun()
    with submit_col:
        submit_disabled = index != total - 1 or answered < total
        do_submit = st.button("Envoyer", type="primary", use_container_width=True, disabled=submit_disabled)
    st.markdown("</div>", unsafe_allow_html=True)

    repository, backend_name = _build_repository()
    st.caption(f"Stockage : {backend_name}")
    debug(f"🗄️ Backend actif : **{backend_name}**.")

    if do_submit:
        with st.status("⏳ Envoi des réponses en cours...", expanded=True) as submit_status:
            submit_status.write("1/3 · Validation des réponses.")
            for q in selected_questions:
                normalized = _normalize_answer_for_storage(q, answers.get(q.id))
                if not _is_answer_valid(q, normalized):
                    submit_status.update(label="❌ Validation échouée", state="error")
                    st.error("Veuillez compléter toutes les questions avant l'envoi.")
                    st.stop()

            submit_status.write("2/3 · Enregistrement des réponses dans le stockage.")
            for idx, q in enumerate(selected_questions, start=1):
                repository.save_response(
                    session_id=selected_session_id,
                    player_id=player_id,
                    question_id=q.id,
                    value=_normalize_answer_for_storage(q, answers.get(q.id)),
                    text_id=text_id,
                    device_id=actor_id,
                )
                submit_status.write(f"   - Réponse {idx}/{len(selected_questions)} enregistrée.")

            submit_status.write("3/3 · Finalisation et mise à jour de l'affichage.")
            st.session_state["interaction_submitted"] = True
            submit_status.update(label="✅ Envoi terminé", state="complete")
            st.success("Merci. Vos réponses sont enregistrées. Les agrégats sont visibles.")
            st.rerun()

    if st.session_state.get("interaction_submitted"):
        debug("📊 Chargement des agrégats après envoi.")
        rows = repository.get_responses(selected_session_id)
        selected_ids = {q.id for q in selected_questions}
        filtered_rows = [row for row in rows if row.get("item_id") in selected_ids]
        _render_aggregates(filtered_rows, selected_questions)
    else:
        st.info("Les agrégats s'affichent après l'envoi pour limiter l'influence sociale.")

    with st.expander("Debug", expanded=False):
        st.code(
            (
                f"session_id={selected_session_id}\n"
                f"text_id={text_id}\n"
                f"actor_id={actor_id}\n"
                f"player_id={player_id or '<anonymous>'}\n"
                f"questions={','.join(q.id for q in selected_questions)}"
            )
        )


if __name__ == "__main__":
    main()
