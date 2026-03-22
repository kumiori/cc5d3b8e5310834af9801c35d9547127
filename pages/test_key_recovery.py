from __future__ import annotations

from typing import Any, Dict, List, Optional

import streamlit as st

from infra.app_context import get_authenticator, get_notion_repo
from infra.app_state import ensure_session_state, remember_access
from infra.notion_repo import _execute_with_retry, _resolve_data_source_id, get_database_schema
from services.notion_value_utils import (
    find_prop,
    read_relation_first,
    read_rich_text,
    read_title,
)
from ui import apply_theme, set_page, sidebar_technical_debug


def _normalize(text: str) -> str:
    """Normalize user-entered text for matching."""
    return " ".join(str(text or "").strip().lower().split())


def _mask_email(email: str) -> str:
    """Mask email to preserve privacy in candidate list."""
    raw = str(email or "").strip()
    if "@" not in raw:
        return "—"
    local, domain = raw.split("@", 1)
    if not local:
        return f"***@{domain}"
    return f"{local[:1]}***@{domain}"


def _mask_key_suffix(access_key: str, size: int = 4) -> str:
    """Return masked key suffix hint."""
    key = str(access_key or "").strip()
    if not key:
        return "—"
    return key[-size:]


def _load_sessions_map(repo: Any) -> Dict[str, str]:
    """Load session id -> session code map for display/disambiguation."""
    out: Dict[str, str] = {}
    try:
        for row in repo.list_sessions(limit=300):
            sid = str(row.get("id") or "")
            if sid:
                out[sid] = str(row.get("session_code") or sid[:8])
    except Exception:
        pass
    return out


def _load_players_for_recovery(repo: Any, *, limit: int = 500) -> List[Dict[str, Any]]:
    """Load player rows with fields useful for recovery scoring."""
    players_db_id = str(getattr(repo, "players_db_id", "") or "")
    if not players_db_id:
        return []
    ds_id = _resolve_data_source_id(repo.client, players_db_id)
    if not ds_id:
        return []
    schema = get_database_schema(repo.client, players_db_id)
    name_rt_prop = find_prop(schema, "nickname", "rich_text")
    name_title_prop = find_prop(schema, "Name", "title")
    key_prop = find_prop(schema, "access_key", "rich_text")
    email_prop = find_prop(schema, "email", "email")
    email_rt_prop = find_prop(schema, "email", "rich_text")
    session_prop = find_prop(schema, "session", "relation")

    query: Dict[str, Any] = {"data_source_id": ds_id, "page_size": min(100, max(1, limit))}
    out: List[Dict[str, Any]] = []
    while True:
        payload = _execute_with_retry(repo.client.data_sources.query, **query)
        for page in payload.get("results", []):
            props = page.get("properties", {}) if isinstance(page, dict) else {}
            email_val = ""
            email_raw = props.get(email_prop)
            if isinstance(email_raw, dict) and email_raw.get("type") == "email":
                email_val = str(email_raw.get("email") or "")
            if not email_val:
                email_val = read_rich_text(props, email_rt_prop)
            session_ids: List[str] = []
            rel_first = read_relation_first(props, session_prop)
            if rel_first:
                session_ids.append(rel_first)
            rel_raw = props.get(session_prop)
            if isinstance(rel_raw, dict) and rel_raw.get("type") == "relation":
                for item in rel_raw.get("relation", []):
                    if isinstance(item, dict) and item.get("id"):
                        sid = str(item.get("id") or "")
                        if sid and sid not in session_ids:
                            session_ids.append(sid)
            out.append(
                {
                    "id": str(page.get("id") or ""),
                    "nickname": read_rich_text(props, name_rt_prop)
                    or read_title(props, name_title_prop)
                    or "participant",
                    "email": email_val.strip(),
                    "access_key": read_rich_text(props, key_prop).strip(),
                    "session_ids": session_ids,
                }
            )
        if not payload.get("has_more") or len(out) >= limit:
            break
        query["start_cursor"] = payload.get("next_cursor")
    return out[:limit]


def _score_candidate(
    candidate: Dict[str, Any],
    *,
    query_text: str,
    session_id: str,
    key_suffix: str,
) -> Dict[str, Any]:
    """Compute confidence score and reasons for one recovery candidate."""
    q = _normalize(query_text)
    email = _normalize(candidate.get("email") or "")
    name = _normalize(candidate.get("nickname") or "")
    suffix = str(key_suffix or "").strip()
    candidate_key = str(candidate.get("access_key") or "")
    score = 0
    reasons: List[str] = []
    if q and email and q == email:
        score += 120
        reasons.append("email exact")
    if q and name and q == name:
        score += 90
        reasons.append("nom exact")
    elif q and name and q in name:
        score += 55
        reasons.append("nom partiel")
    if suffix and candidate_key and candidate_key.endswith(suffix):
        score += 70
        reasons.append("fin de clé")
    if session_id and session_id in set(candidate.get("session_ids", [])):
        score += 25
        reasons.append("session")
    return {"score": score, "reasons": reasons}


def _find_candidates(
    players: List[Dict[str, Any]],
    *,
    query_text: str,
    session_id: str,
    key_suffix: str,
) -> List[Dict[str, Any]]:
    """Return ranked candidate list for recovery lookup."""
    scored: List[Dict[str, Any]] = []
    for row in players:
        details = _score_candidate(
            row,
            query_text=query_text,
            session_id=session_id,
            key_suffix=key_suffix,
        )
        if details["score"] <= 0:
            continue
        scored.append({**row, **details})
    scored.sort(key=lambda x: (int(x.get("score", 0)), str(x.get("nickname") or "")), reverse=True)
    return scored[:10]


def main() -> None:
    """Render a test access-key recovery flow with privacy-safe matching."""
    set_page()
    apply_theme()
    ensure_session_state()

    repo = get_notion_repo()
    if not repo:
        st.error("Connexion Notion indisponible.")
        st.stop()

    authenticator = get_authenticator(repo)
    authenticator.login(location="hidden", key="recovery-cookie-check", callback=remember_access)
    is_logged = bool(st.session_state.get("authentication_status"))
    with st.sidebar:
        if is_logged:
            st.caption("Tu es déjà connecté·e.")
            authenticator.logout(button_name="Se déconnecter", location="sidebar")
        else:
            st.caption("Pas connecté·e.")
            authenticator.login(location="sidebar", key="recovery-login-form", callback=remember_access)
    sidebar_technical_debug(
        page_label="test_key_recovery",
        repo=repo,
        extra={"is_logged_in": is_logged},
    )

    st.title("Test · Récupération de clé")
    st.caption("Tu peux retrouver ta clé sans exposer les données des autres participant·e·s.")

    if is_logged and st.session_state.get("player_access_key"):
        st.success("Clé locale trouvée sur cet appareil. Tu peux restaurer l’accès immédiatement.")
        st.code(str(st.session_state.get("player_access_key") or ""), language="text")
        c1, c2 = st.columns(2)
        with c1:
            if st.button("Restaurer cet accès", type="primary", use_container_width=True):
                st.switch_page("pages/04_Home.py")
        with c2:
            if st.button("Continuer vers la cuisine", use_container_width=True):
                st.switch_page("pages/03_Cuisine.py")
        st.markdown("---")

    st.subheader("Clé oubliée ?")
    st.write("Indique le nom ou l’e-mail utilisé pour la session.")

    sessions_map = _load_sessions_map(repo)
    session_options = [""] + list(sessions_map.keys())
    session_id = st.selectbox(
        "Session (optionnel)",
        options=session_options,
        format_func=lambda sid: "Toutes les sessions" if not sid else sessions_map.get(sid, sid[:8]),
    )
    query_text = st.text_input("Nom ou e-mail")
    key_suffix = st.text_input("Fin de clé (optionnel)", max_chars=8)
    run_search = st.button("Rechercher", type="primary", use_container_width=True)

    st.session_state.setdefault("_recovery_candidates", [])
    st.session_state.setdefault("_recovery_selected_id", "")

    if run_search:
        if not query_text.strip() and not key_suffix.strip():
            st.warning("Ajoute au moins un nom, un e-mail ou une fin de clé.")
        else:
            with st.spinner("Recherche des correspondances probables..."):
                players = _load_players_for_recovery(repo, limit=800)
                candidates = _find_candidates(
                    players,
                    query_text=query_text,
                    session_id=session_id,
                    key_suffix=key_suffix,
                )
            st.session_state["_recovery_candidates"] = candidates
            st.session_state["_recovery_selected_id"] = ""

    candidates = st.session_state.get("_recovery_candidates", [])
    if candidates:
        st.markdown("### Accès possibles")
        st.caption("Les résultats sont masqués. Choisis la ligne qui te correspond.")
        for idx, row in enumerate(candidates):
            sid = (row.get("session_ids") or [""])[0]
            session_label = sessions_map.get(sid, "session inconnue") if sid else "session inconnue"
            line = (
                f"**{row.get('nickname', 'participant')}** · "
                f"session {session_label} · "
                f"e-mail {_mask_email(str(row.get('email') or ''))} · "
                f"clé finissant par `{_mask_key_suffix(str(row.get('access_key') or ''))}`"
            )
            st.markdown(line)
            st.caption(
                f"Confiance: {row.get('score', 0)} · "
                f"indices: {', '.join(row.get('reasons', [])) or '—'}"
            )
            if st.button("C’est moi", key=f"recovery-select-{idx}", use_container_width=True):
                st.session_state["_recovery_selected_id"] = str(row.get("id") or "")

    selected_id = str(st.session_state.get("_recovery_selected_id") or "")
    if selected_id:
        selected = next((row for row in candidates if str(row.get("id") or "") == selected_id), None)
        if selected:
            st.markdown("### Confirmation")
            st.write("Pour confirmer, indique une information complémentaire.")
            confirm_suffix = st.text_input("Fin de clé")
            confirm_email = st.text_input("E-mail complet (si utilisé)")
            if st.button("Confirmer et récupérer", type="primary", use_container_width=True):
                suffix_ok = bool(confirm_suffix.strip()) and str(selected.get("access_key") or "").endswith(confirm_suffix.strip())
                email_ok = bool(confirm_email.strip()) and _normalize(confirm_email) == _normalize(selected.get("email") or "")
                high_confidence = int(selected.get("score", 0)) >= 120
                if high_confidence or suffix_ok or email_ok:
                    access_key = str(selected.get("access_key") or "").strip()
                    st.success("Accès retrouvé.")
                    st.code(access_key, language="text")
                    st.session_state["login_access_key_prefill"] = access_key
                    st.session_state["login_access_key_prefill_notice"] = "Ta clé est préremplie. Tu peux te connecter."
                    c1, c2 = st.columns(2)
                    with c1:
                        if st.button("Continuer", type="primary", use_container_width=True):
                            st.switch_page("pages/02_Login.py")
                    with c2:
                        st.caption("Copie la clé puis continue vers la connexion.")
                else:
                    st.warning("Je n’ai pas assez de certitude. Vérifie la fin de clé ou l’e-mail.")

    with st.expander("Aide manuelle", expanded=False):
        st.write("Si tu ne retrouves pas ta clé ici, laisse un message à l’équipe organisatrice.")
        st.text_area("Message de récupération", key="recovery-help-message")
        st.caption("Ce bloc est un prototype : pas encore envoyé automatiquement.")


if __name__ == "__main__":
    main()
