from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import streamlit as st

from infra.app_context import get_authenticator, get_notion_repo
from infra.app_state import (
    ensure_session_context,
    ensure_session_state,
    remember_access,
)
from repositories.session_repo import SessionRepository
from services.admin_data import (
    clear_admin_caches,
    get_contact_preferences,
    get_players,
    get_sessions,
    now_iso,
    build_players_dashboard_rows,
)
from services.admin_logging import log_admin_event
from services.admin_metrics import compute_activity_metrics, compute_contact_metrics
from services.duplicate_detection import (
    build_duplicate_activity_snapshot,
    detect_duplicate_candidates,
    duplicate_rule_text,
    log_duplicate_merge_invite,
    mark_candidate_unrelated,
)
from ui import apply_theme, set_page, sidebar_auth_controls, sidebar_technical_debug

PAGE_ID = "07_Admin"


def begin_timed_task(task_name: str) -> float:
    """Start timing an admin task and register initial trace metadata."""
    started = time.perf_counter()
    st.session_state.setdefault("_admin_timing_traces", [])
    st.session_state["_admin_last_task"] = task_name
    st.session_state["_admin_last_task_started_at"] = now_iso()
    return started


def end_timed_task(start_ts: float, task_name: str) -> None:
    """Close timed task and push elapsed measurement to sidebar traces."""
    elapsed_ms = (time.perf_counter() - start_ts) * 1000.0
    traces = st.session_state.setdefault("_admin_timing_traces", [])
    traces.insert(
        0,
        {
            "task": task_name,
            "elapsed_ms": round(elapsed_ms, 1),
            "finished_at": datetime.now(timezone.utc).isoformat(),
        },
    )
    st.session_state["_admin_timing_traces"] = traces[:20]


def render_sidebar_operational_context(
    *,
    session_label: str,
    player_summary: Dict[str, Any],
    system_summary: Dict[str, Any],
) -> None:
    """Render compact operational context and timing traces in sidebar."""
    with st.sidebar:
        st.markdown("### Admin · Contexte")
        st.caption(f"Session ciblée: {session_label or '—'}")
        st.caption(
            f"Joueurs: {player_summary.get('total_players', 0)} · "
            f"Contact: {player_summary.get('with_contact_preference', 0)} · "
            f"Sans contact: {player_summary.get('no_contact_requested', 0)}"
        )
        st.caption(
            f"Actifs 12h: {player_summary.get('active_12h', 0)} · "
            f"Scans doublons: {system_summary.get('duplicate_scan_runs', 0)}"
        )
        with st.expander(
            "Timing traces",
            expanded=False,
            key="admin-timing-traces-expander",
            on_change="rerun",
        ):
            traces = st.session_state.get("_admin_timing_traces", [])
            if not traces:
                st.caption("Aucune trace pour le moment.")
            else:
                st.table(traces)


def render_admin_header() -> None:
    """Render admin page title and subtitle."""
    st.title("Administration")
    st.caption("Pilotage des sessions, des joueurs et maintenance opérationnelle.")


def _refresh_button() -> None:
    """Render explicit refresh action and invalidate admin caches."""
    if st.button("Rafraîchir les données", use_container_width=True):
        clear_admin_caches()
        st.session_state["_admin_players_loaded"] = False
        st.session_state["_admin_dup_candidates"] = []
        log_admin_event(event_type="admin_refresh", page=PAGE_ID)
        st.rerun()


def render_sessions_panel(repo: Any) -> None:
    """Render session list and inline actions for activation/metadata updates."""
    st.subheader("Session management")
    start = begin_timed_task("sessions.load")
    try:
        sessions = get_sessions(repo, limit=200)
    finally:
        end_timed_task(start, "sessions.load")

    if not sessions:
        st.info("Aucune session trouvée.")
        return

    session_rows: List[Dict[str, Any]] = []
    for item in sessions:
        session_rows.append(
            {
                "session_code": item.get("session_code"),
                "status": item.get("status"),
                "active": item.get("active"),
                "session_order": item.get("session_order"),
                "question_count": item.get("question_count", 0),
                "session_id": item.get("id"),
            }
        )
    st.dataframe(session_rows, use_container_width=True, hide_index=True)

    selected_id = st.selectbox(
        "Session à modifier",
        options=[row["id"] for row in sessions if row.get("id")],
        format_func=lambda sid: next(
            (
                f"{row.get('session_code') or 'Session'} · {row.get('status') or ''}"
                for row in sessions
                if row.get("id") == sid
            ),
            sid,
        ),
        key="admin_session_select",
    )
    selected = next((row for row in sessions if row.get("id") == selected_id), {})
    status = st.text_input(
        "Status", value=str(selected.get("status") or ""), key="admin_session_status"
    )
    mode = st.text_input(
        "Mode", value=str(selected.get("mode") or ""), key="admin_session_mode"
    )
    session_order = st.number_input(
        "Ordre de session",
        min_value=0,
        value=int(selected.get("session_order") or 0),
        step=1,
        key="admin_session_order",
    )
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("Activer", use_container_width=True, key="admin_activate_session"):
            begin = begin_timed_task("sessions.activate")
            try:
                SessionRepository(repo).update_session_active(selected_id, True)
                clear_admin_caches()
                log_admin_event(
                    event_type="session_activate",
                    page=PAGE_ID,
                    session_id=selected_id,
                )
                st.success("Session activée.")
            except Exception as exc:
                log_admin_event(
                    event_type="session_activate",
                    page=PAGE_ID,
                    session_id=selected_id,
                    status="error",
                    metadata={"error": str(exc)},
                )
                st.error(f"Échec d’activation: {exc}")
            finally:
                end_timed_task(begin, "sessions.activate")
    with col2:
        if st.button("Désactiver", use_container_width=True, key="admin_deactivate_session"):
            begin = begin_timed_task("sessions.deactivate")
            try:
                SessionRepository(repo).update_session_active(selected_id, False)
                clear_admin_caches()
                log_admin_event(
                    event_type="session_deactivate",
                    page=PAGE_ID,
                    session_id=selected_id,
                )
                st.success("Session désactivée.")
            except Exception as exc:
                log_admin_event(
                    event_type="session_deactivate",
                    page=PAGE_ID,
                    session_id=selected_id,
                    status="error",
                    metadata={"error": str(exc)},
                )
                st.error(f"Échec de désactivation: {exc}")
            finally:
                end_timed_task(begin, "sessions.deactivate")
    with col3:
        if st.button("Mettre à jour metadata", use_container_width=True, key="admin_update_session_meta"):
            begin = begin_timed_task("sessions.update_metadata")
            try:
                repo.update_session(
                    selected_id,
                    status=status,
                    mode=mode,
                    round_index=int(session_order),
                )
                clear_admin_caches()
                log_admin_event(
                    event_type="session_update_metadata",
                    page=PAGE_ID,
                    session_id=selected_id,
                    metadata={"status": status, "mode": mode, "session_order": int(session_order)},
                )
                st.success("Metadata de session mise à jour.")
            except Exception as exc:
                log_admin_event(
                    event_type="session_update_metadata",
                    page=PAGE_ID,
                    session_id=selected_id,
                    status="error",
                    metadata={"error": str(exc)},
                )
                st.error(f"Échec de mise à jour: {exc}")
            finally:
                end_timed_task(begin, "sessions.update_metadata")


def render_players_dashboard(repo: Any, session_id: str) -> Dict[str, int]:
    """Render player metrics/table and return computed summary counters."""
    st.subheader("Players dashboard")
    force_refresh = bool(st.session_state.get("_admin_players_force_refresh", False))
    with st.spinner("Chargement joueurs + préférences contact..."):
        start = begin_timed_task("players.load")
        try:
            players = get_players(repo, limit=500, force_refresh=force_refresh)
            contact_preferences = get_contact_preferences(
                repo, session_id=session_id, force_refresh=force_refresh
            )
            rows, player_metrics = build_players_dashboard_rows(players, contact_preferences)
        finally:
            end_timed_task(start, "players.load")
    st.session_state["_admin_players_force_refresh"] = False

    activity_metrics = compute_activity_metrics(players)
    contact_metrics = compute_contact_metrics(rows)
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total players", player_metrics.get("total_players", 0))
    m2.metric("With contact preference", player_metrics.get("with_contact_preference", 0))
    m3.metric("No-contact requested", player_metrics.get("no_contact_requested", 0))
    m4.metric("Active in last 12h", activity_metrics.get("active_12h", 0))

    st.caption(
        "Data source: players + responses filtered by item_id == CONTACT_METHOD "
        f"({len(contact_preferences)} signaux)."
    )
    st.dataframe(rows, use_container_width=True, hide_index=True)
    return {
        **player_metrics,
        **activity_metrics,
        **contact_metrics,
    }


def render_duplicate_players_panel(repo: Any) -> Dict[str, Any]:
    """Render manual duplicate scan controls and candidate results."""
    st.subheader("Potential duplicate players")
    st.caption("Contrôle manuel uniquement. Aucun merge/suppression automatique.")
    st.info(duplicate_rule_text())

    if st.button("Run duplicate-player scan", key="admin_run_duplicate_scan", use_container_width=True):
        with st.spinner("Analyse des identités en cours..."):
            start = begin_timed_task("duplicates.scan")
            try:
                players = get_players(repo, limit=2000, force_refresh=True)
                candidates = detect_duplicate_candidates(players)
                st.session_state["_admin_dup_candidates"] = candidates
                st.session_state["_admin_duplicate_scan_runs"] = (
                    int(st.session_state.get("_admin_duplicate_scan_runs", 0)) + 1
                )
                snapshot = build_duplicate_activity_snapshot(players)
                log_admin_event(
                    event_type="duplicate_scan_run",
                    page=PAGE_ID,
                    metadata=snapshot,
                )
            finally:
                end_timed_task(start, "duplicates.scan")

    candidates: List[Dict[str, Any]] = st.session_state.get("_admin_dup_candidates", [])
    if not candidates:
        st.caption("Aucun scan récent ou aucun doublon potentiel détecté.")
        return {
            "duplicate_scan_runs": int(st.session_state.get("_admin_duplicate_scan_runs", 0)),
            "candidate_groups": 0,
        }

    st.warning(f"{len(candidates)} groupe(s) candidat(s) détecté(s).")
    for idx, candidate in enumerate(candidates, start=1):
        with st.expander(f"Groupe {idx} · {len(candidate.get('player_ids', []))} profils", expanded=False):
            st.write("Raisons:", ", ".join(candidate.get("reasons", [])))
            st.write("Match keys:", ", ".join(candidate.get("match_keys", [])))
            table_rows = []
            for player in candidate.get("players", []):
                table_rows.append(
                    {
                        "id": player.get("id"),
                        "nickname": player.get("nickname"),
                        "email": player.get("email"),
                        "access_key": player.get("access_key"),
                        "last_activity": player.get("last_activity") or player.get("last_joined_on"),
                    }
                )
            st.dataframe(table_rows, hide_index=True, use_container_width=True)
            c1, c2 = st.columns(2)
            with c1:
                if st.button(
                    "Marquer non lié",
                    key=f"admin_dup_unrelated_{candidate.get('candidate_key')}",
                    use_container_width=True,
                ):
                    mark_candidate_unrelated(str(candidate.get("candidate_key") or ""))
                    st.success("Groupe marqué comme non lié.")
            with c2:
                if st.button(
                    "Journaliser invitation de fusion",
                    key=f"admin_dup_invite_{candidate.get('candidate_key')}",
                    use_container_width=True,
                ):
                    log_duplicate_merge_invite(
                        actor_player_id=str(st.session_state.get("player_page_id") or ""),
                        session_id=str(st.session_state.get("session_id") or ""),
                        candidate_ids=[str(pid) for pid in candidate.get("player_ids", [])],
                        reasons=[str(r) for r in candidate.get("reasons", [])],
                        match_keys=[str(mk) for mk in candidate.get("match_keys", [])],
                    )
                    st.success("Action journalisée.")
    return {
        "duplicate_scan_runs": int(st.session_state.get("_admin_duplicate_scan_runs", 0)),
        "candidate_groups": len(candidates),
    }


def _render_optional_admin_controls(repo: Any) -> None:
    """Render optional maintenance controls for admins."""
    with st.expander("Optional admin controls", expanded=False):
        st.caption("Schema/status checks and maintenance actions.")
        st.json(
            {
                "session_db_id": getattr(repo, "session_db_id", ""),
                "players_db_id": getattr(repo, "players_db_id", ""),
                "responses_db_id": getattr(repo, "responses_db_id", ""),
                "questions_db_id": getattr(repo, "questions_db_id", ""),
            }
        )
        if st.button("Clear admin caches", key="admin_clear_caches_btn"):
            clear_admin_caches()
            st.success("Caches admin vidés.")
        if st.button("Retour lobby", key="admin_back_home", use_container_width=True):
            st.switch_page("pages/04_Home.py")


def main() -> None:
    """Entrypoint for admin dashboard page."""
    set_page()
    apply_theme()
    ensure_session_state()
    repo = get_notion_repo()
    if not repo:
        st.error("Notion repository unavailable.")
        return
    authenticator = get_authenticator(repo)
    authentication_status = sidebar_auth_controls(
        authenticator,
        callback=remember_access,
        key_prefix="admin-auth",
    )
    ensure_session_context(repo)
    sidebar_technical_debug(
        page_label="07_Admin",
        repo=repo,
        extra={
            "timing_trace_count": len(st.session_state.get("_admin_timing_traces", [])),
            "duplicate_scan_runs": int(st.session_state.get("_admin_duplicate_scan_runs", 0)),
        },
    )
    if not authentication_status:
        st.warning("Please log in first.")
        st.stop()

    render_admin_header()
    _refresh_button()
    render_sessions_panel(repo)

    session_id = str(st.session_state.get("session_id") or "")
    player_summary = render_players_dashboard(repo, session_id)
    system_summary = render_duplicate_players_panel(repo)
    _render_optional_admin_controls(repo)

    render_sidebar_operational_context(
        session_label=str(st.session_state.get("session_title") or "—"),
        player_summary=player_summary,
        system_summary=system_summary,
    )


if __name__ == "__main__":
    main()
