from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import streamlit as st
import streamlit.components.v1 as components

from infra.app_context import get_authenticator, get_notion_repo
from infra.app_state import (
    ensure_session_context,
    ensure_session_state,
    remember_access,
)
from infra.notion_repo import (
    _execute_with_retry,
    _resolve_data_source_id,
    get_database_schema,
)
from services.notion_value_utils import (
    as_list_labels,
    find_exact_prop,
    parse_json_text,
    parse_number,
    read_multiselect_names,
    read_number,
    read_relation_first,
    read_rich_text,
    read_title,
)
from services.sumup_client import SumUpClient
from services.audio_storage import get_audio_storage_adapter
from ui import apply_theme, set_page, sidebar_auth_controls, sidebar_technical_debug


def _now_iso() -> str:
    """Return current UTC timestamp as ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


def _relation_first_in(props: Dict[str, Any], names: List[str]) -> str:
    """Return first relation id found among candidate relation properties."""
    for name in names:
        rel_id = read_relation_first(props, name)
        if rel_id:
            return rel_id
    return ""


def _question_ids(questions: List[Dict[str, Any]], key: str) -> List[str]:
    """Return candidate question ids by semantic key using multilingual aliases."""
    aliases = {
        "diet": ["régime", "regime", "preference", "préférence"],
        "allergens": ["allergène", "allergen"],
        "hard_no": ["ingrédients non", "ingredient", "exclusion"],
        "texture": ["texture"],
        "cravings": ["envies", "craving"],
        "contribution": ["contribution", "economic_contribution"],
    }
    terms = aliases.get(key, [key])
    out: List[str] = []
    for q in questions:
        text = str(q.get("text") or "").lower()
        if any(term in text for term in terms):
            qid = str(q.get("id") or "")
            if qid:
                out.append(qid)
    return out


def _load_player_page(repo: Any, player_page_id: str) -> Dict[str, Any]:
    """Load participant profile fields from players database."""
    players_db_id = str(getattr(repo, "players_db_id", "") or "")
    schema = get_database_schema(repo.client, players_db_id) if players_db_id else {}
    page = _execute_with_retry(repo.client.pages.retrieve, page_id=player_page_id)
    props = page.get("properties", {}) if isinstance(page, dict) else {}
    diet_prop = find_exact_prop(schema, ["diet"], "multi_select")
    allergens_prop = find_exact_prop(schema, ["allergens"], "multi_select")
    hard_no_prop = find_exact_prop(schema, ["hard_no"], "multi_select")
    notes_prop = find_exact_prop(schema, ["notes_public"], "rich_text")
    nickname_prop = find_exact_prop(schema, ["nickname"], "rich_text")
    title_prop = find_exact_prop(schema, ["Name"], "title")
    access_prop = find_exact_prop(schema, ["access_key", "player_id"], "rich_text")
    session_prop = find_exact_prop(schema, ["session"], "relation")

    session_ids = []
    if session_prop:
        val = props.get(session_prop)
        if isinstance(val, dict) and val.get("type") == "relation":
            session_ids = [
                str(r.get("id"))
                for r in val.get("relation", [])
                if isinstance(r, dict) and r.get("id")
            ]

    return {
        "id": player_page_id,
        "nickname": read_rich_text(props, nickname_prop)
        or read_title(props, title_prop)
        or player_page_id[:8],
        "access_key": read_rich_text(props, access_prop),
        "diet": read_multiselect_names(props, diet_prop),
        "allergens": read_multiselect_names(props, allergens_prop),
        "hard_no": read_multiselect_names(props, hard_no_prop),
        "comment_text": read_rich_text(props, notes_prop),
        "comment_prop": notes_prop,
        "session_ids": session_ids,
        "schema": schema,
    }


def _load_player_responses(
    repo: Any, session_id: str, player_page_id: str
) -> List[Dict[str, Any]]:
    """Load participant responses for a specific session."""
    responses_db_id = str(getattr(repo, "responses_db_id", "") or "")
    if not responses_db_id:
        return []
    ds_id = _resolve_data_source_id(repo.client, responses_db_id)
    if not ds_id:
        return []
    schema = get_database_schema(repo.client, responses_db_id)
    session_prop = find_exact_prop(schema, ["session"], "relation")
    player_prop = find_exact_prop(schema, ["player"], "relation")
    question_rel_prop = find_exact_prop(schema, ["question", "statement"], "relation")
    question_id_prop = find_exact_prop(schema, ["question_id"], "rich_text")
    item_id_prop = find_exact_prop(schema, ["item_id"], "rich_text")
    value_prop = find_exact_prop(schema, ["value"], "rich_text")
    value_number_prop = find_exact_prop(schema, ["value_number"], "number")
    title_prop = find_exact_prop(schema, ["Name"], "title")

    filters = []
    if session_prop:
        filters.append({"property": session_prop, "relation": {"contains": session_id}})
    if player_prop:
        filters.append(
            {"property": player_prop, "relation": {"contains": player_page_id}}
        )

    query: Dict[str, Any] = {
        "data_source_id": ds_id,
        "sorts": [{"timestamp": "created_time", "direction": "descending"}],
        "page_size": 100,
    }
    if len(filters) == 1:
        query["filter"] = filters[0]
    elif len(filters) > 1:
        query["filter"] = {"and": filters}

    out: List[Dict[str, Any]] = []
    while True:
        payload = _execute_with_retry(repo.client.data_sources.query, **query)
        for page in payload.get("results", []):
            props = page.get("properties", {}) if isinstance(page, dict) else {}
            title_key = read_title(props, title_prop)
            qid = (
                _relation_first_in(props, [question_rel_prop])
                or read_rich_text(props, question_id_prop)
                or read_rich_text(props, item_id_prop)
            )
            if not qid and title_key:
                mq = re.search(r"Q:([0-9a-fA-F-]{16,40})", title_key)
                if mq:
                    qid = mq.group(1)
            pid = _relation_first_in(props, [player_prop])
            if not pid and title_key:
                mp = re.search(r"P:([0-9a-fA-F-]{16,40})", title_key)
                if mp:
                    pid = mp.group(1)
            if pid and pid != player_page_id:
                continue
            out.append(
                {
                    "id": str(page.get("id") or ""),
                    "question_id": str(qid or ""),
                    "player_id": pid,
                    "value": parse_json_text(read_rich_text(props, value_prop)),
                    "value_number": read_number(props, value_number_prop),
                    "created_at": str(page.get("created_time") or ""),
                    "title_key": title_key,
                }
            )
        if not payload.get("has_more"):
            break
        query["start_cursor"] = payload.get("next_cursor")
    return out


def _latest_by_qid(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Keep latest response row by question id."""
    out: Dict[str, Dict[str, Any]] = {}
    for row in sorted(rows, key=lambda x: str(x.get("created_at") or ""), reverse=True):
        qid = str(row.get("question_id") or "")
        if qid and qid not in out:
            out[qid] = row
    return out


def _resolve_participant_state(
    repo: Any, session_id: str, player_page_id: str
) -> Dict[str, Any]:
    """Resolve canonical participant state from profile plus latest responses."""
    player = _load_player_page(repo, player_page_id)
    questions = repo.list_questions(session_id)
    responses = _load_player_responses(repo, session_id, player_page_id)
    latest = _latest_by_qid(responses)

    qids = {
        "diet": _question_ids(questions, "diet"),
        "allergens": _question_ids(questions, "allergens"),
        "hard_no": _question_ids(questions, "hard_no"),
        "texture": _question_ids(questions, "texture"),
        "cravings": _question_ids(questions, "cravings"),
        "contribution": _question_ids(questions, "contribution"),
    }

    def _latest_list_for(keys: List[str]) -> List[str]:
        for key in keys:
            row = latest.get(key)
            if row:
                vals = as_list_labels(row.get("value"))
                if vals:
                    return vals
        return []

    def _latest_str_for(keys: List[str]) -> str:
        for key in keys:
            row = latest.get(key)
            if row:
                value = row.get("value")
                if isinstance(value, str) and value.strip():
                    return value.strip()
        return ""

    def _latest_number_for(keys: List[str]) -> Optional[float]:
        for key in keys:
            row = latest.get(key)
            if not row:
                continue
            n = parse_number(row.get("value_number"))
            if n is None:
                n = parse_number(row.get("value"))
            if n is not None:
                return n
        return None

    last_updated = ""
    if responses:
        last_updated = max(str(r.get("created_at") or "") for r in responses)

    contribution = _latest_number_for(qids["contribution"])
    return {
        "player": player,
        "questions": questions,
        "responses": responses,
        "latest_by_qid": latest,
        "qids": qids,
        "resolved": {
            "participation": session_id in set(player.get("session_ids", [])),
            "diet": _latest_list_for(qids["diet"]) or list(player.get("diet", [])),
            "allergens": _latest_list_for(qids["allergens"])
            or list(player.get("allergens", [])),
            "hard_no": _latest_list_for(qids["hard_no"])
            or list(player.get("hard_no", [])),
            "texture": _latest_str_for(qids["texture"]),
            "cravings": _latest_list_for(qids["cravings"]),
            "contribution_eur": contribution,
            "comment_text": str(player.get("comment_text") or ""),
            "updated_at": last_updated,
        },
    }


def _upsert_response(
    repo: Any, session_id: str, player_id: str, question_id: str, value: Any
) -> None:
    """Create or update a response row for one participant/question."""
    responses_db_id = str(getattr(repo, "responses_db_id", "") or "")
    if not responses_db_id:
        raise RuntimeError("Responses database id missing.")
    schema = get_database_schema(repo.client, responses_db_id)
    ds_id = _resolve_data_source_id(repo.client, responses_db_id)
    if not ds_id:
        raise RuntimeError("Responses data source id missing.")

    title_prop = find_exact_prop(schema, ["Name"], "title")
    session_prop = find_exact_prop(schema, ["session"], "relation")
    player_prop = find_exact_prop(schema, ["player"], "relation")
    question_rel_prop = find_exact_prop(schema, ["question", "statement"], "relation")
    question_id_prop = find_exact_prop(schema, ["question_id"], "rich_text")
    value_prop = find_exact_prop(schema, ["value"], "rich_text")
    value_number_prop = find_exact_prop(schema, ["value_number"], "number")
    created_prop = find_exact_prop(schema, ["created_at"], "date")
    logical_key = f"Q:{question_id} P:{player_id}"

    filters: List[Dict[str, Any]] = []
    if title_prop:
        filters.append({"property": title_prop, "title": {"equals": logical_key}})
    if session_prop:
        filters.append({"property": session_prop, "relation": {"contains": session_id}})
    if player_prop:
        filters.append({"property": player_prop, "relation": {"contains": player_id}})
    if question_rel_prop:
        filters.append(
            {"property": question_rel_prop, "relation": {"contains": question_id}}
        )
    elif question_id_prop:
        filters.append(
            {"property": question_id_prop, "rich_text": {"equals": question_id}}
        )

    query: Dict[str, Any] = {"data_source_id": ds_id, "page_size": 1}
    if len(filters) == 1:
        query["filter"] = filters[0]
    elif len(filters) > 1:
        query["filter"] = {"and": filters}
    payload = _execute_with_retry(repo.client.data_sources.query, **query)
    existing = payload.get("results", [])
    target_page_id = existing[0].get("id") if existing else None

    props: Dict[str, Any] = {}
    if title_prop:
        props[title_prop] = {
            "title": [{"type": "text", "text": {"content": logical_key}}]
        }
    if session_prop:
        props[session_prop] = {"relation": [{"id": session_id}]}
    if player_prop:
        props[player_prop] = {"relation": [{"id": player_id}]}
    if question_rel_prop:
        props[question_rel_prop] = {"relation": [{"id": question_id}]}
    if question_id_prop:
        props[question_id_prop] = {
            "rich_text": [{"type": "text", "text": {"content": question_id}}]
        }
    if value_prop:
        props[value_prop] = {
            "rich_text": [
                {
                    "type": "text",
                    "text": {"content": json.dumps(value, ensure_ascii=False)},
                }
            ]
        }
    if value_number_prop and isinstance(value, (int, float)):
        props[value_number_prop] = {"number": float(value)}
    if created_prop:
        props[created_prop] = {"date": {"start": _now_iso()}}

    if target_page_id:
        _execute_with_retry(
            repo.client.pages.update, page_id=target_page_id, properties=props
        )
    else:
        _execute_with_retry(
            repo.client.pages.create,
            parent={"database_id": responses_db_id},
            properties=props,
        )


def _save_comment(repo: Any, player_id: str, comment: str) -> None:
    """Save participant public note on the player profile page."""
    players_db_id = str(getattr(repo, "players_db_id", "") or "")
    if not players_db_id:
        raise RuntimeError("Players database id missing.")
    schema = get_database_schema(repo.client, players_db_id)
    notes_prop = find_exact_prop(schema, ["notes_public"], "rich_text")
    if not notes_prop:
        raise RuntimeError("Players schema has no notes_public rich_text field.")
    _execute_with_retry(
        repo.client.pages.update,
        page_id=player_id,
        properties={
            notes_prop: {
                "rich_text": [{"type": "text", "text": {"content": comment.strip()}}]
            }
        },
    )


def _save_audio_note(
    repo: Any,
    session_id: str,
    player_id: str,
    file_name: str,
    mime_type: str,
    size_bytes: int,
    *,
    storage_provider: str,
    storage_path: str,
    public_url: str,
    created_at: str = "",
) -> None:
    """Persist audio-note metadata in the decisions repository."""
    payload = {
        "record_type": "audio_note",
        "participant_key": st.session_state.get("player_access_key", ""),
        "player_id": player_id,
        "session_id": session_id,
        "audio_note_id": f"audio-{int(datetime.now().timestamp())}",
        "audio_url": public_url,
        "storage_provider": storage_provider,
        "storage_path": storage_path,
        "public_url": public_url,
        "mime_type": mime_type,
        "size_bytes": size_bytes,
        "file_name": file_name,
        "created_at": created_at or _now_iso(),
    }
    repo.create_decision(
        session_id=session_id,
        player_id=player_id,
        decision_type=_decision_type_for_storage(repo),
        payload=json.dumps(payload, ensure_ascii=False),
    )


def _load_audio_notes(
    repo: Any, session_id: str, player_id: str
) -> List[Dict[str, Any]]:
    """Load audio-note decision records for one participant."""
    rows = repo.list_decisions(session_id, decision_type=None)
    out: List[Dict[str, Any]] = []
    for row in rows:
        pids = row.get("player_id") or []
        if player_id not in pids:
            continue
        payload_raw = str(row.get("payload") or "")
        try:
            payload = json.loads(payload_raw) if payload_raw else {}
        except Exception:
            payload = {}
        if str(payload.get("record_type") or "") != "audio_note":
            continue
        out.append(
            {
                "id": row.get("id"),
                "created_at": row.get("created_at"),
                "payload": payload,
            }
        )
    out.sort(key=lambda x: str(x.get("created_at") or ""), reverse=True)
    return out


def _persist_payment_attempt(
    repo: Any,
    *,
    session_id: str,
    player_id: str,
    amount_eur: float,
    currency: str,
    checkout_id: str,
    status: str,
    payment_url: str = "",
    transaction_id: str = "",
    payload: Optional[Dict[str, Any]] = None,
) -> None:
    """Persist one SumUp checkout/payment attempt as immutable history."""
    body = {
        "record_type": "payment_attempt",
        "participant_key": st.session_state.get("player_access_key", ""),
        "player_id": player_id,
        "session_id": session_id,
        "contribution_amount_eur": float(amount_eur),
        "currency": currency,
        "sumup_checkout_id": checkout_id,
        "sumup_transaction_id": transaction_id,
        "sumup_status": status,
        "payment_link_url": payment_url,
        "created_at": _now_iso(),
        "updated_at": _now_iso(),
        "sumup_response_payload": payload or {},
    }
    repo.create_decision(
        session_id=session_id,
        player_id=player_id,
        decision_type=_decision_type_for_storage(repo),
        payload=json.dumps(body, ensure_ascii=False),
    )


def _load_payment_attempts(
    repo: Any, session_id: str, player_id: str
) -> List[Dict[str, Any]]:
    """Load payment attempts for one participant ordered newest-first."""
    rows = repo.list_decisions(session_id, decision_type=None)
    out: List[Dict[str, Any]] = []
    for row in rows:
        pids = row.get("player_id") or []
        if player_id not in pids:
            continue
        payload_raw = str(row.get("payload") or "")
        try:
            payload = json.loads(payload_raw) if payload_raw else {}
        except Exception:
            payload = {}
        if str(payload.get("record_type") or "") != "payment_attempt":
            continue
        out.append(
            {
                "id": row.get("id"),
                "created_at": row.get("created_at"),
                "payload": payload,
            }
        )
    out.sort(key=lambda x: str(x.get("created_at") or ""), reverse=True)
    return out


def _status_fr(raw_status: str) -> str:
    """Map internal payment status to short French labels."""
    mapping = {
        "not_started": "Non démarré",
        "checkout_created": "Lien généré",
        "pending": "En attente de paiement",
        "paid": "Paiement confirmé",
        "failed": "Paiement échoué",
        "expired": "Expiré",
        "cancelled": "Annulé",
    }
    key = str(raw_status or "").strip().lower()
    return mapping.get(key, key or "Non démarré")


def _audio_path_from_payload(payload: Dict[str, Any]) -> str:
    """Resolve the most suitable local/reference path from audio payload."""
    return str(
        payload.get("storage_path")
        or payload.get("public_url")
        or payload.get("audio_url")
        or ""
    )


def _decision_type_for_storage(repo: Any) -> str:
    """Pick a valid option from current Notion decisions.type select options."""
    db_id = str(getattr(repo, "decisions_db_id", "") or "")
    if not db_id:
        return "description_status"
    try:
        schema = get_database_schema(repo.client, db_id)
        type_meta = schema.get("type")
        if isinstance(type_meta, dict) and type_meta.get("type") == "select":
            options = (type_meta.get("select") or {}).get("options") or []
            names = [
                str(opt.get("name") or "")
                for opt in options
                if isinstance(opt, dict) and opt.get("name")
            ]
            for preferred in [
                "description_status",
                "journey_A",
                "journey_B",
                "structure_choice",
            ]:
                if preferred in names:
                    return preferred
            if names:
                return names[0]
    except Exception:
        pass
    return "description_status"


@st.dialog("Paiement SumUp")
def _sumup_payment_dialog(checkout_id: str) -> None:
    """Render SumUp card dialog for an existing checkout id."""
    st.caption("Finalise le paiement dans ce module sécurisé SumUp.")
    js_code = f"""
        <div id="sumup-card" style="min-height: 520px;"></div>
        <script type="text/javascript" src="https://gateway.sumup.com/gateway/ecom/card/v2/sdk.js"></script>
        <script type="text/javascript">
            const mountCard = () => {{
                if (!window.SumUpCard) return;
                window.SumUpCard.mount({{
                    id: "sumup-card",
                    checkoutId: "{checkout_id}",
                    donateSubmitButton: false,
                    showInstallments: false,
                    onResponse: function (type, body) {{
                        console.log("SumUp onResponse type:", type);
                        console.log("SumUp onResponse body:", body);
                    }},
                }});
            }};
            if (document.readyState === "loading") {{
                document.addEventListener("DOMContentLoaded", mountCard);
            }} else {{
                mountCard();
            }}
        </script>
    """
    components.html(js_code, height=620, scrolling=True)


def main() -> None:
    """Render participant page with resolved preferences, audio, and payments."""
    set_page()
    apply_theme()
    ensure_session_state()
    repo = get_notion_repo()
    if not repo:
        st.error("Connexion Notion indisponible.")
        st.stop()
    authenticator = get_authenticator(repo)
    authentication_status = sidebar_auth_controls(
        authenticator,
        callback=remember_access,
        key_prefix="participant-auth",
    )
    ensure_session_context(repo)
    sidebar_technical_debug(
        page_label="09_Participant",
        repo=repo,
        extra={
            "audio_save_enabled": True,
            "sumup_dialog_enabled": True,
        },
    )
    if not authentication_status:
        st.warning("Please log in first.")
        st.stop()

    player_page_id = str(st.session_state.get("player_page_id") or "").strip()
    if not player_page_id:
        st.error("Clé participant absente. Reconnecte-toi.")
        st.stop()
    session_id = str(st.session_state.get("session_id") or "").strip()
    session_label = str(st.session_state.get("session_title") or "Session")
    if not session_id:
        st.error("Session introuvable.")
        st.stop()

    state = _resolve_participant_state(repo, session_id, player_page_id)
    resolved = state["resolved"]
    player = state["player"]
    questions = state["questions"]

    st.title("Espace Affranchi·e")
    st.caption(
        f"Session: {session_label} · Affranchi·e reconnu·e: {player.get('nickname')}"
    )
    st.caption(
        "Tu peux mettre à jour tes préférences et activer ta contribution ci-dessous."
    )

    st.subheader("Récapitulatif de mes préférences")
    st.markdown(
        f"""
        - Participation: **{"Oui" if resolved.get("participation") else "Non"}**
        - Allergènes: **{", ".join(resolved.get("allergens", [])) or "aucun"}**
        - Ingrédients exclus: **{", ".join(resolved.get("hard_no", [])) or "aucun"}**
        - Texture: **{resolved.get("texture") or "—"}**
        - Envies: **{", ".join(resolved.get("cravings", [])) or "—"}**
        - Contribution proposée: **{resolved.get("contribution_eur") if resolved.get("contribution_eur") is not None else "—"} EUR**
        - Dernière mise à jour: **{resolved.get("updated_at") or "—"}**
        """
    )
    if st.button("Modifier mes préférences", use_container_width=True):
        st.switch_page("pages/03_Cuisine.py")

    st.subheader("Commentaire / feedback")
    comment_input = st.text_area(
        "Message pour l’équipe (optionnel)",
        value=str(resolved.get("comment_text") or ""),
        height=120,
        key="participant_comment_text",
    )
    if st.button("Enregistrer le commentaire", use_container_width=True):
        try:
            _save_comment(repo, player_page_id, comment_input)
            st.success("Commentaire enregistré.")
        except Exception as exc:
            st.error(f"Échec enregistrement commentaire: {exc}")

    st.subheader("Note audio (optionnel)")
    audio_notes = _load_audio_notes(repo, session_id, player_page_id)
    if audio_notes:
        latest_audio = audio_notes[0].get("payload", {})
        st.caption(
            "Note active: "
            f"{latest_audio.get('file_name', 'audio')} · "
            f"{latest_audio.get('mime_type', '')} · "
            f"{latest_audio.get('size_bytes', 0)} bytes"
        )
        audio_path = _audio_path_from_payload(latest_audio)
        if audio_path and Path(audio_path).exists():
            st.audio(audio_path)
        with st.expander("Historique des notes audio", expanded=False):
            for idx, note in enumerate(audio_notes, start=1):
                payload = note.get("payload", {})
                path = _audio_path_from_payload(payload)
                provider = str(payload.get("storage_provider") or "unknown")
                st.markdown(
                    f"{idx}. **{payload.get('file_name', 'audio')}** · "
                    f"{payload.get('mime_type', '')} · "
                    f"{payload.get('size_bytes', 0)} bytes · "
                    f"{note.get('created_at', '')} · "
                    f"provider: `{provider}`"
                )
                if path and Path(path).exists():
                    st.audio(path)
                else:
                    st.caption(
                        "Fichier audio non disponible localement (référence conservée)."
                    )
    recorded_audio = st.audio_input("Enregistrer une note audio")
    if recorded_audio is not None and st.button(
        "Enregistrer la note audio", use_container_width=True
    ):
        audio_bytes = recorded_audio.getvalue()
        try:
            adapter = get_audio_storage_adapter()
            stored = adapter.store(
                session_id=session_id,
                player_id=player_page_id,
                file_name=recorded_audio.name,
                mime_type=str(recorded_audio.type or ""),
                content=audio_bytes,
            )
            _save_audio_note(
                repo,
                session_id=session_id,
                player_id=player_page_id,
                file_name=stored.file_name,
                mime_type=stored.mime_type,
                size_bytes=stored.size_bytes,
                storage_provider=stored.storage_provider,
                storage_path=stored.storage_path,
                public_url=stored.public_url,
                created_at=stored.created_at,
            )
            st.success("Note audio enregistrée.")
        except Exception as exc:
            st.error(f"Échec enregistrement audio: {exc}")

    st.subheader("Contribution")
    current_contribution = parse_number(resolved.get("contribution_eur"))
    if current_contribution is None:
        current_contribution = 0.0
    proposed = st.number_input(
        "Montant proposé (EUR)",
        min_value=0.0,
        max_value=100000.0,
        value=float(current_contribution),
        step=1.0,
        key="participant_contribution_amount",
    )

    contribution_qids = _question_ids(questions, "contribution")
    contribution_qid = contribution_qids[0] if contribution_qids else ""
    c1, c2 = st.columns(2)
    with c1:
        if st.button("Mettre à jour ma contribution", use_container_width=True):
            if not contribution_qid:
                st.error("Question contribution introuvable pour cette session.")
            else:
                try:
                    _upsert_response(
                        repo,
                        session_id,
                        player_page_id,
                        contribution_qid,
                        float(proposed),
                    )
                    st.success("Contribution mise à jour.")
                except Exception as exc:
                    st.error(f"Échec mise à jour contribution: {exc}")
    with c2:
        if st.button(
            "Activer la contribution (SumUp)", type="primary", use_container_width=True
        ):
            if proposed <= 0:
                st.error("Le montant doit être supérieur à 0 pour activer le paiement.")
            else:
                client = SumUpClient.from_secrets()
                if not client.is_configured():
                    st.error("Configuration SumUp manquante dans les secrets.")
                else:
                    ref = f"aff-{session_id[:8]}-{player_page_id[:8]}-{int(datetime.now().timestamp())}"
                    metadata = {
                        "participant_key": st.session_state.get(
                            "player_access_key", ""
                        ),
                        "player_id": player_page_id,
                        "session_id": session_id,
                    }
                    result = client.create_checkout(
                        amount=float(proposed),
                        currency="EUR",
                        checkout_reference=ref,
                        description=f"Contribution participant {player.get('nickname')}",
                        metadata=metadata,
                    )
                    if result.get("ok"):
                        payload = result.get("json") or {}
                        checkout_id = str(payload.get("id") or "")
                        links = payload.get("links") or []
                        pay_url = ""
                        if isinstance(links, list):
                            for link in links:
                                if isinstance(link, dict) and str(
                                    link.get("href") or ""
                                ):
                                    pay_url = str(link.get("href") or "")
                                    break
                        try:
                            _persist_payment_attempt(
                                repo,
                                session_id=session_id,
                                player_id=player_page_id,
                                amount_eur=float(proposed),
                                currency="EUR",
                                checkout_id=checkout_id,
                                status="checkout_created",
                                payment_url=pay_url,
                                payload=payload,
                            )
                            st.success("Lien de paiement créé et enregistré.")
                            if checkout_id:
                                _sumup_payment_dialog(checkout_id)
                        except Exception as exc:
                            st.error(
                                f"Checkout créé mais persistence locale échouée: {exc}"
                            )
                    else:
                        st.error(
                            f"Échec création checkout SumUp: {result.get('error')}"
                        )

    attempts = _load_payment_attempts(repo, session_id, player_page_id)
    latest_attempt = attempts[0]["payload"] if attempts else {}
    latest_status = str(latest_attempt.get("sumup_status") or "not_started")
    st.caption(f"Statut paiement actuel: **{_status_fr(latest_status)}**")

    if attempts:
        st.markdown("#### Tentatives de paiement")
        for i, attempt in enumerate(attempts):
            payload = attempt.get("payload", {})
            checkout_id = str(payload.get("sumup_checkout_id") or "")
            amount = payload.get("contribution_amount_eur")
            status = str(payload.get("sumup_status") or "")
            with st.expander(
                f"Tentative {i + 1} · {amount} EUR · {status} · {checkout_id[:12]}",
                expanded=False,
            ):
                st.json(payload, expanded=False)
                r1, r2 = st.columns(2)
                with r1:
                    if st.button(
                        "Ouvrir paiement",
                        key=f"pay-open-{attempt['id']}",
                        use_container_width=True,
                        disabled=not bool(checkout_id),
                    ):
                        _sumup_payment_dialog(checkout_id)
                with r2:
                    if st.button(
                        "Vérifier statut",
                        key=f"pay-verify-{attempt['id']}",
                        use_container_width=True,
                        disabled=not bool(checkout_id),
                    ):
                        client = SumUpClient.from_secrets()
                        result = client.checkout_details(checkout_id)
                        if result.get("ok"):
                            payload_now = result.get("json") or {}
                            status_now = str(
                                payload_now.get("status") or "pending"
                            ).lower()
                            transaction_id = str(
                                payload_now.get("transaction_id") or ""
                            )
                            try:
                                _persist_payment_attempt(
                                    repo,
                                    session_id=session_id,
                                    player_id=player_page_id,
                                    amount_eur=float(amount or 0.0),
                                    currency=str(payload_now.get("currency") or "EUR"),
                                    checkout_id=checkout_id,
                                    status=status_now,
                                    payment_url=str(
                                        payload.get("payment_link_url") or ""
                                    ),
                                    transaction_id=transaction_id,
                                    payload=payload_now,
                                )
                                st.success(
                                    f"Statut mis à jour: {_status_fr(status_now)}"
                                )
                            except Exception as exc:
                                st.error(
                                    f"Statut récupéré mais persistence échouée: {exc}"
                                )
                        else:
                            st.error(f"Vérification échouée: {result.get('error')}")

    if st.button("Retour au lobby", use_container_width=True):
        st.switch_page("pages/04_Home.py")


if __name__ == "__main__":
    main()
