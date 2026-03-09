from __future__ import annotations

import json
import logging
import re
import traceback
from collections import Counter
from datetime import datetime, timezone
from time import perf_counter, sleep, time
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st
from notion_client import Client

from infra.app_context import get_authenticator, get_notion_repo
from infra.app_state import (
    ensure_auth,
    ensure_session_state,
    remember_access,
)
from infra.notion_repo import get_database_schema
from lib.notion_options import ensure_multiselect_option
from services.presence import count_active_users, touch_player_presence


QUESTION_CATALOG: List[Dict[str, Any]] = [
    {
        "key": "diet",
        "prompt": "Régime",
        "kind": "constraint",
        "qtype": "multi",
        "required": True,
        "order": 1,
    },
    {
        "key": "allergens",
        "prompt": "Allergènes",
        "kind": "constraint",
        "qtype": "multi",
        "required": True,
        "order": 2,
    },
    {
        "key": "hard_no",
        "prompt": "Ingrédients non",
        "kind": "constraint",
        "qtype": "multi",
        "required": True,
        "order": 3,
    },
    {
        "key": "spice",
        "prompt": "Piquant",
        "kind": "preference",
        "qtype": "single",
        "required": True,
        "order": 4,
    },
    {
        "key": "texture",
        "prompt": "Texture",
        "kind": "preference",
        "qtype": "single",
        "required": True,
        "order": 5,
    },
    {
        "key": "cravings",
        "prompt": "Envies du moment",
        "kind": "craving",
        "qtype": "multi",
        "required": True,
        "max_select": 2,
        "order": 6,
    },
    {
        "key": "contribution",
        "prompt": "Contribution",
        "kind": "preference",
        "qtype": "single",
        "required": True,
        "order": 7,
    },
]

NECESSARY_KEYS = ["allergens", "contribution"]
EXTENDED_ONLY_KEYS = ["diet", "hard_no", "spice", "texture", "cravings"]

CRAVINGS_OPTIONS = [
    "frais",
    "réconfortant",
    "lumineux",
    "fumé",
    "umami",
    "herbacé",
    "croquant",
    "crémeux",
    "chaleur épicée",
    "surprise",
]

TEXTURE_OPTIONS = ["croquant", "crémeux", "les deux", "autre/aucune"]
CONDIMENT_CHOICES = ["j'évite", "ok", "j'aime"]


LOGGER = logging.getLogger("affranchis.cuisine")
if not LOGGER.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")
    )
    LOGGER.addHandler(_handler)
    LOGGER.setLevel(logging.INFO)
    LOGGER.propagate = False


# ---------- helpers ----------
def debug(msg: str) -> None:
    st.markdown(msg)


def _cache_store() -> Dict[str, Any]:
    return st.session_state.setdefault("aff_read_cache", {})


def _cache_bust_token() -> int:
    return int(st.session_state.get("aff_cache_bust", 0))


def _bust_cache() -> None:
    st.session_state["aff_cache_bust"] = _cache_bust_token() + 1


def cached_read(cache_key: str, loader):
    token = _cache_bust_token()
    key = f"{cache_key}|v{token}"
    cache = _cache_store()
    if key in cache:
        return cache[key]
    value = loader()
    cache[key] = value
    return value


def timed_call(label: str, fn):
    started = perf_counter()
    try:
        return fn()
    finally:
        elapsed_ms = (perf_counter() - started) * 1000
        LOGGER.info("perf.%s_ms=%.1f", label, elapsed_ms)


def _stream_chunk(text: str, *, punctuation_pause: float = 0.08):
    words = text.split(" ")
    for idx, word in enumerate(words):
        token = word
        if idx < len(words) - 1:
            token += " "
        yield token
        delay = 0.032
        if token.endswith((".", "!", "?")):
            delay += punctuation_pause
        elif token.endswith((",", ";", ":")):
            delay += punctuation_pause * 0.6
        sleep(delay)


def run_progress_expander(steps: List[Dict[str, str]], runner):
    placeholder = st.empty()

    def push(title: str, _msg: str = "") -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {title}"
        if _msg:
            line += f" · {_msg}"
        LOGGER.info("progress %s", line)
        with placeholder.container():
            with st.expander(title, expanded=False):
                if _msg:
                    st.caption(_msg)

    if steps:
        push(steps[0]["title"], steps[0].get("msg", ""))
    return runner(push)


def normalize_label(value: str) -> str:
    txt = re.sub(r"\s+", " ", (value or "").strip().lower())
    return txt


def parse_csv_labels(raw: str) -> List[str]:
    tokens = [normalize_label(tok) for tok in str(raw or "").split(",")]
    deduped: List[str] = []
    for tok in tokens:
        if tok and tok not in deduped:
            deduped.append(tok)
    return deduped


def _is_valid_contribution(value: Any) -> bool:
    try:
        amount = float(value)
    except Exception:
        return False
    if amount == 0:
        return True
    return 1 <= amount <= 100000


def short_save_summary(summary: Dict[str, Any]) -> str:
    preferences = ", ".join(summary.get("diet") or []) or "—"
    allergens = ", ".join(summary.get("allergens") or []) or "none"
    hard_no = ", ".join(summary.get("hard_no") or []) or "none"
    contrib = summary.get("contribution")
    contrib_txt = "—" if contrib is None else str(contrib)
    cravings = ", ".join(summary.get("cravings") or []) or "—"
    return (
        f"My preferences: {preferences} · Allergens: {allergens} · No ingredients: {hard_no} · "
        f"Contribution: {contrib_txt} · Cravings: {cravings}"
    )


def ensure_questionnaire_state(
    session_id: str,
    player_id: str,
    selected_player: Dict[str, Any],
) -> str:
    state_key = f"{session_id}:{player_id}"
    current_key = str(st.session_state.get("aff_form_state_key", ""))
    if current_key != state_key:
        initial_allergens = list(selected_player.get("allergens", []))
        st.session_state["aff_form_state_key"] = state_key
        st.session_state["aff_form_index"] = 0
        st.session_state["aff_form_values"] = {
            "diet": list(selected_player.get("diet", [])),
            "allergens": initial_allergens,
            "allergens_mode": "Known allergens" if initial_allergens else "",
            "hard_no": list(selected_player.get("hard_no", [])),
            "allergen_other": "",
            "allergens_none_known": False,
            "hard_no_other": "",
            "hard_no_none": False,
            "spice": 3,
            "texture": "",
            "pref_ail": "ok",
            "pref_oignon": "ok",
            "pref_coriandre": "ok",
            "cravings": [],
            "contribution_choice": "",
            "contribution_custom": "",
            "contribution_value": None,
            "bio_note": selected_player.get("bio_note", ""),
        }
    return state_key


def _required_answered(question_key: str, values: Dict[str, Any]) -> bool:
    if question_key == "diet":
        return (
            isinstance(values.get(question_key), list)
            and len(values.get(question_key) or []) > 0
        )
    if question_key == "hard_no":
        if bool(values.get("hard_no_none", False)):
            return True
        custom = normalize_label(values.get("hard_no_other", ""))
        return (
            isinstance(values.get("hard_no"), list)
            and len(values.get("hard_no") or []) > 0
        ) or bool(custom)
    if question_key == "allergens":
        mode = str(values.get("allergens_mode", "")).strip()
        if mode == "None known":
            return True
        if mode != "Known allergens":
            return False
        custom = parse_csv_labels(values.get("allergen_other", ""))
        return (
            isinstance(values.get("allergens"), list)
            and len(values.get("allergens") or []) > 0
        ) or bool(custom)
    if question_key == "spice":
        return values.get("spice") is not None
    if question_key == "texture":
        return str(values.get("texture", "")).strip() != ""
    if question_key == "cravings":
        vals = values.get("cravings") or []
        return isinstance(vals, list) and len(vals) > 0
    if question_key == "contribution":
        return _is_valid_contribution(values.get("contribution_value"))
    return True


def now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def secret_required(name: str) -> str:
    notion_cfg = st.secrets.get("notion", {})
    value = str(notion_cfg.get(name, "")).strip()
    if not value:
        value = str(notion_cfg.get(name.lower(), "")).strip()
    if not value:
        value = str(st.secrets.get(name, "")).strip()
    if not value:
        value = str(st.secrets.get(name.lower(), "")).strip()
    if not value:
        raise RuntimeError(
            f"Secret Notion manquant : notion.{name} (or top-level {name})"
        )
    return value


def resolve_data_source_id(client: Client, db_id: str) -> Optional[str]:
    db = client.databases.retrieve(database_id=db_id)
    ds_list = db.get("data_sources") or []
    if not ds_list:
        return None
    first = ds_list[0] if isinstance(ds_list[0], dict) else {}
    ds_id = first.get("id")
    return str(ds_id) if ds_id else None


def query_db(client: Client, db_id: str, **kwargs: Any) -> Dict[str, Any]:
    if hasattr(client.databases, "query"):
        return client.databases.query(database_id=db_id, **kwargs)
    ds_id = resolve_data_source_id(client, db_id)
    if not ds_id:
        raise RuntimeError(
            f"Impossible de requêter la base {db_id} (data_source introuvable)."
        )
    return client.data_sources.query(data_source_id=ds_id, **kwargs)


def query_all(client: Client, db_id: str, **kwargs: Any) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    cursor: Optional[str] = None
    while True:
        args = {k: v for k, v in kwargs.items() if v is not None}
        args.setdefault("page_size", 100)
        if cursor:
            args["start_cursor"] = cursor
        payload = query_db(client, db_id, **args)
        page_results = payload.get("results", [])
        if isinstance(page_results, list):
            results.extend([r for r in page_results if isinstance(r, dict)])
        if not payload.get("has_more"):
            break
        cursor = payload.get("next_cursor")
        if not cursor:
            break
    return results


def safe_get_schema(client: Client, db_id: str) -> Dict[str, Any]:
    try:
        return get_database_schema(client, db_id)
    except Exception:
        debug("⚠️ Échec de récupération du schéma.")
        debug("🔍 Vérification des data_sources…")
        raise


def find_prop(
    schema: Dict[str, Any], expected: str, ptype: Optional[str] = None
) -> Optional[str]:
    if expected in schema:
        return expected
    if ptype:
        for name, meta in schema.items():
            if isinstance(meta, dict) and meta.get("type") == ptype:
                return str(name)
    return None


def rich_text_value(props: Dict[str, Any], prop_name: Optional[str]) -> str:
    if not prop_name:
        return ""
    value = props.get(prop_name)
    if not isinstance(value, dict) or value.get("type") != "rich_text":
        return ""
    return "".join(
        part.get("plain_text", "")
        for part in value.get("rich_text", [])
        if isinstance(part, dict)
    )


def title_value(props: Dict[str, Any], prop_name: Optional[str]) -> str:
    if not prop_name:
        return ""
    value = props.get(prop_name)
    if not isinstance(value, dict) or value.get("type") != "title":
        return ""
    return "".join(
        part.get("plain_text", "")
        for part in value.get("title", [])
        if isinstance(part, dict)
    )


def select_value(props: Dict[str, Any], prop_name: Optional[str]) -> str:
    if not prop_name:
        return ""
    value = props.get(prop_name)
    if not isinstance(value, dict) or value.get("type") != "select":
        return ""
    sel = value.get("select") or {}
    return str(sel.get("name", ""))


def multi_select_values(props: Dict[str, Any], prop_name: Optional[str]) -> List[str]:
    if not prop_name:
        return []
    value = props.get(prop_name)
    if not isinstance(value, dict) or value.get("type") != "multi_select":
        return []
    return [
        str(opt.get("name", ""))
        for opt in value.get("multi_select", [])
        if isinstance(opt, dict) and opt.get("name")
    ]


def relation_ids(props: Dict[str, Any], prop_name: Optional[str]) -> List[str]:
    if not prop_name:
        return []
    value = props.get(prop_name)
    if not isinstance(value, dict) or value.get("type") != "relation":
        return []
    return [
        str(item.get("id"))
        for item in value.get("relation", [])
        if isinstance(item, dict) and item.get("id")
    ]


def to_json_text(value: Any) -> str:
    if isinstance(value, (dict, list, int, float, bool)):
        return json.dumps(value, ensure_ascii=False)
    return str(value or "")


def from_json_text(raw: str) -> Any:
    text = (raw or "").strip()
    if not text:
        return ""
    try:
        return json.loads(text)
    except Exception:
        return text


def extract_multi_options(
    schema: Dict[str, Any], prop_name: str, fallback: List[str]
) -> List[str]:
    prop = schema.get(prop_name)
    if not isinstance(prop, dict) or prop.get("type") != "multi_select":
        return fallback
    multi = prop.get("multi_select") or {}
    options = multi.get("options") if isinstance(multi, dict) else []
    labels = [
        str(opt.get("name", ""))
        for opt in options
        if isinstance(opt, dict) and opt.get("name")
    ]
    return labels or fallback


def list_sessions(
    client: Client, sessions_db_id: str, sessions_schema: Dict[str, Any]
) -> List[Dict[str, Any]]:
    active_prop = find_prop(sessions_schema, "active", "checkbox")
    code_rich = find_prop(sessions_schema, "session_code", "rich_text")
    code_title = find_prop(sessions_schema, "Name", "title")

    pages = query_all(
        client,
        sessions_db_id,
        sorts=[{"timestamp": "created_time", "direction": "descending"}],
        page_size=50,
    )
    rows: List[Dict[str, Any]] = []
    for page in pages:
        props = page.get("properties", {})
        code = (
            rich_text_value(props, code_rich)
            or title_value(props, code_title)
            or "Session"
        )
        is_active = False
        if active_prop:
            active_value = props.get(active_prop)
            is_active = (
                bool((active_value or {}).get("checkbox"))
                if isinstance(active_value, dict)
                else False
            )
        rows.append({"id": page.get("id", ""), "code": code, "active": is_active})
    return rows


def ensure_questions_for_session(
    client: Client,
    questions_db_id: str,
    questions_schema: Dict[str, Any],
    session_id: str,
    diet_options: List[str],
    allergens_options: List[str],
    hard_no_options: List[str],
) -> List[Dict[str, Any]]:
    session_prop = find_prop(questions_schema, "session", "relation")
    title_prop = find_prop(questions_schema, "Name", "title")
    kind_prop = find_prop(questions_schema, "kind", "select")
    qtype_prop = find_prop(questions_schema, "qtype", "select")
    order_prop = find_prop(questions_schema, "order", "number")
    required_prop = find_prop(questions_schema, "required", "checkbox")
    max_select_prop = find_prop(questions_schema, "max_select", "number")
    options_json_prop = find_prop(questions_schema, "options_json", "rich_text")

    if not title_prop:
        return []

    existing_pages = query_all(
        client,
        questions_db_id,
        filter={"property": session_prop, "relation": {"contains": session_id}}
        if session_prop
        else None,
        page_size=100,
    )

    existing_by_prompt: Dict[str, Dict[str, Any]] = {}
    for page in existing_pages:
        props = page.get("properties", {})
        prompt = title_value(props, title_prop)
        if prompt:
            existing_by_prompt[prompt] = page

    seeded: List[Dict[str, Any]] = []
    for item in QUESTION_CATALOG:
        options: List[str] = []
        if item["key"] == "diet":
            options = diet_options
        elif item["key"] == "allergens":
            options = allergens_options
        elif item["key"] == "hard_no":
            options = hard_no_options
        elif item["key"] == "texture":
            options = TEXTURE_OPTIONS
        elif item["key"] == "cravings":
            options = CRAVINGS_OPTIONS

        if item["prompt"] not in existing_by_prompt:
            properties: Dict[str, Any] = {
                title_prop: {
                    "title": [{"type": "text", "text": {"content": item["prompt"]}}]
                }
            }
            if session_prop:
                properties[session_prop] = {"relation": [{"id": session_id}]}
            if kind_prop:
                properties[kind_prop] = {"select": {"name": item["kind"]}}
            if qtype_prop:
                properties[qtype_prop] = {"select": {"name": item["qtype"]}}
            if order_prop:
                properties[order_prop] = {"number": item["order"]}
            if required_prop:
                properties[required_prop] = {
                    "checkbox": bool(item.get("required", False))
                }
            if max_select_prop and item.get("max_select") is not None:
                properties[max_select_prop] = {"number": int(item["max_select"])}
            if options_json_prop:
                properties[options_json_prop] = {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {
                                "content": json.dumps(options, ensure_ascii=False)
                            },
                        }
                    ]
                }

            page = client.pages.create(
                parent={"database_id": questions_db_id}, properties=properties
            )
            existing_by_prompt[item["prompt"]] = page

    # normalize output sorted by order
    for item in QUESTION_CATALOG:
        page = existing_by_prompt.get(item["prompt"])
        if not page:
            continue
        props = page.get("properties", {})
        stored_options = (
            from_json_text(rich_text_value(props, options_json_prop))
            if options_json_prop
            else []
        )
        if not isinstance(stored_options, list):
            stored_options = []
        seeded.append(
            {
                "id": page.get("id", ""),
                "key": item["key"],
                "prompt": item["prompt"],
                "kind": item["kind"],
                "qtype": item["qtype"],
                "required": bool(item.get("required", False)),
                "max_select": item.get("max_select"),
                "options": [str(v) for v in stored_options],
                "order": item["order"],
            }
        )

    seeded.sort(key=lambda r: int(r.get("order", 0)))
    return seeded


def list_players(
    client: Client, players_db_id: str, players_schema: Dict[str, Any], session_id: str
) -> List[Dict[str, Any]]:
    session_prop = find_prop(players_schema, "session", "relation")
    nickname_prop = find_prop(players_schema, "nickname", "rich_text")
    title_prop = find_prop(players_schema, "Name", "title")
    role_prop = find_prop(players_schema, "role", "select")
    bio_prop = find_prop(players_schema, "notes_public", "rich_text")
    diet_prop = find_prop(players_schema, "diet", "multi_select")
    allergens_prop = find_prop(players_schema, "allergens", "multi_select")
    hard_no_prop = find_prop(players_schema, "hard_no", "multi_select")

    filters = (
        {"property": session_prop, "relation": {"contains": session_id}}
        if session_prop
        else None
    )
    pages = query_all(client, players_db_id, filter=filters, page_size=100)

    rows: List[Dict[str, Any]] = []
    for page in pages:
        props = page.get("properties", {})
        name = rich_text_value(props, nickname_prop) or title_value(props, title_prop)
        rows.append(
            {
                "id": page.get("id", ""),
                "name": name or "Sans nom",
                "role": (select_value(props, role_prop) or "guest").lower(),
                "bio_note": rich_text_value(props, bio_prop),
                "diet": multi_select_values(props, diet_prop),
                "allergens": multi_select_values(props, allergens_prop),
                "hard_no": multi_select_values(props, hard_no_prop),
            }
        )

    return rows


def save_player_profile(
    client: Client,
    player_id: str,
    players_schema: Dict[str, Any],
    *,
    diet: List[str],
    allergens: List[str],
    hard_no: List[str],
    bio_note: str,
) -> None:
    diet_prop = find_prop(players_schema, "diet", "multi_select")
    allergens_prop = find_prop(players_schema, "allergens", "multi_select")
    hard_no_prop = find_prop(players_schema, "hard_no", "multi_select")
    bio_prop = find_prop(players_schema, "notes_public", "rich_text")

    props: Dict[str, Any] = {}
    if diet_prop:
        props[diet_prop] = {"multi_select": [{"name": v} for v in diet]}
    if allergens_prop:
        props[allergens_prop] = {"multi_select": [{"name": v} for v in allergens]}
    if hard_no_prop:
        props[hard_no_prop] = {"multi_select": [{"name": v} for v in hard_no]}
    if bio_prop:
        props[bio_prop] = {
            "rich_text": [{"type": "text", "text": {"content": bio_note}}]
        }

    if props:
        client.pages.update(page_id=player_id, properties=props)
        _bust_cache()


def get_player_by_page_id(
    client: Client,
    players_schema: Dict[str, Any],
    player_page_id: str,
) -> Optional[Dict[str, Any]]:
    if not player_page_id:
        return None
    try:
        page = client.pages.retrieve(page_id=player_page_id)
    except Exception:
        return None
    props = page.get("properties", {})
    nickname_prop = find_prop(players_schema, "nickname", "rich_text")
    title_prop = find_prop(players_schema, "Name", "title")
    role_prop = find_prop(players_schema, "role", "select")
    return {
        "id": page.get("id", ""),
        "name": rich_text_value(props, nickname_prop)
        or title_value(props, title_prop)
        or "Participant",
        "role": (select_value(props, role_prop) or "guest").lower(),
        "bio_note": rich_text_value(
            props, find_prop(players_schema, "notes_public", "rich_text")
        ),
        "diet": multi_select_values(
            props, find_prop(players_schema, "diet", "multi_select")
        ),
        "allergens": multi_select_values(
            props, find_prop(players_schema, "allergens", "multi_select")
        ),
        "hard_no": multi_select_values(
            props, find_prop(players_schema, "hard_no", "multi_select")
        ),
    }


def link_player_to_session(
    client: Client,
    players_schema: Dict[str, Any],
    player_page_id: str,
    session_id: str,
) -> None:
    session_prop = find_prop(players_schema, "session", "relation")
    if not session_prop:
        return
    page = client.pages.retrieve(page_id=player_page_id)
    props = page.get("properties", {})
    current_ids = relation_ids(props, session_prop)
    if session_id in current_ids:
        return
    next_ids = [{"id": pid} for pid in [*current_ids, session_id] if pid]
    client.pages.update(
        page_id=player_page_id, properties={session_prop: {"relation": next_ids}}
    )
    _bust_cache()


def join_participant_to_session(
    client: Client,
    players_db_id: str,
    players_schema: Dict[str, Any],
    session_id: str,
    display_name: str,
    role: str = "guest",
) -> Dict[str, Any]:
    existing = list_players(client, players_db_id, players_schema, session_id)
    target = normalize_label(display_name)
    for player in existing:
        if normalize_label(player.get("name", "")) == target:
            return player

    session_prop = find_prop(players_schema, "session", "relation")
    nickname_prop = find_prop(players_schema, "nickname", "rich_text")
    title_prop = find_prop(players_schema, "Name", "title")
    role_prop = find_prop(players_schema, "role", "select")
    status_prop = find_prop(players_schema, "status", "select")
    access_prop = find_prop(players_schema, "access_key", "rich_text") or find_prop(
        players_schema, "player_id", "rich_text"
    )

    synthetic_key = (
        f"guest-{int(datetime.now().timestamp())}-{abs(hash(display_name)) % 10000}"
    )
    props: Dict[str, Any] = {}
    if session_prop:
        props[session_prop] = {"relation": [{"id": session_id}]}
    if nickname_prop:
        props[nickname_prop] = {
            "rich_text": [{"type": "text", "text": {"content": display_name}}]
        }
    if title_prop:
        props[title_prop] = {
            "title": [{"type": "text", "text": {"content": display_name}}]
        }
    if role_prop:
        props[role_prop] = {"select": {"name": role}}
    if status_prop:
        props[status_prop] = {"select": {"name": "active"}}
    if access_prop:
        props[access_prop] = {
            "rich_text": [{"type": "text", "text": {"content": synthetic_key}}]
        }

    created = client.pages.create(
        parent={"database_id": players_db_id}, properties=props
    )
    _bust_cache()
    created_props = created.get("properties", {})
    return {
        "id": created.get("id", ""),
        "name": rich_text_value(created_props, nickname_prop)
        or title_value(created_props, title_prop)
        or display_name,
        "role": (select_value(created_props, role_prop) or role).lower(),
        "bio_note": rich_text_value(
            created_props, find_prop(players_schema, "notes_public", "rich_text")
        ),
        "diet": multi_select_values(
            created_props, find_prop(players_schema, "diet", "multi_select")
        ),
        "allergens": multi_select_values(
            created_props, find_prop(players_schema, "allergens", "multi_select")
        ),
        "hard_no": multi_select_values(
            created_props, find_prop(players_schema, "hard_no", "multi_select")
        ),
    }


def upsert_response(
    client: Client,
    responses_db_id: str,
    responses_schema: Dict[str, Any],
    *,
    session_id: str,
    player_id: str,
    question_id: str,
    value: Any,
    tonight_note: str = "",
) -> None:
    title_prop = find_prop(responses_schema, "Name", "title")
    session_prop = find_prop(responses_schema, "session", "relation")
    player_prop = find_prop(responses_schema, "player", "relation")
    value_prop = find_prop(responses_schema, "value", "rich_text")
    value_number_prop = find_prop(responses_schema, "value_number", "number")
    question_rel_prop = find_prop(responses_schema, "question", "relation")
    question_id_prop = find_prop(
        responses_schema, "question_id", "rich_text"
    ) or find_prop(responses_schema, "item_id", "rich_text")
    note_public_prop = find_prop(responses_schema, "notes_public", "rich_text")
    created_prop = find_prop(responses_schema, "created_at", "date")

    logical_key = f"Q:{question_id} P:{player_id}"

    filter_blocks: List[Dict[str, Any]] = []
    if title_prop:
        filter_blocks.append({"property": title_prop, "title": {"equals": logical_key}})
    if session_prop:
        filter_blocks.append(
            {"property": session_prop, "relation": {"contains": session_id}}
        )
    if player_prop:
        filter_blocks.append(
            {"property": player_prop, "relation": {"contains": player_id}}
        )
    if question_rel_prop:
        filter_blocks.append(
            {"property": question_rel_prop, "relation": {"contains": question_id}}
        )
    elif question_id_prop:
        filter_blocks.append(
            {"property": question_id_prop, "rich_text": {"equals": question_id}}
        )

    query_filter = None
    if filter_blocks:
        query_filter = (
            filter_blocks[0] if len(filter_blocks) == 1 else {"and": filter_blocks}
        )

    existing = query_all(client, responses_db_id, filter=query_filter, page_size=10)
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
            "rich_text": [{"type": "text", "text": {"content": to_json_text(value)}}]
        }
    if value_number_prop and isinstance(value, (int, float)):
        props[value_number_prop] = {"number": float(value)}
    if note_public_prop and tonight_note:
        props[note_public_prop] = {
            "rich_text": [{"type": "text", "text": {"content": tonight_note}}]
        }
    if created_prop:
        props[created_prop] = {"date": {"start": now_iso()}}

    if target_page_id:
        client.pages.update(page_id=target_page_id, properties=props)
    else:
        client.pages.create(parent={"database_id": responses_db_id}, properties=props)
    _bust_cache()


def load_responses_by_session(
    client: Client,
    responses_db_id: str,
    responses_schema: Dict[str, Any],
    session_id: str,
) -> List[Dict[str, Any]]:
    session_prop = find_prop(responses_schema, "session", "relation")
    player_prop = find_prop(responses_schema, "player", "relation")
    value_prop = find_prop(responses_schema, "value", "rich_text")
    question_rel_prop = find_prop(responses_schema, "question", "relation")
    question_id_prop = find_prop(
        responses_schema, "question_id", "rich_text"
    ) or find_prop(responses_schema, "item_id", "rich_text")
    title_prop = find_prop(responses_schema, "Name", "title")
    note_public_prop = find_prop(responses_schema, "notes_public", "rich_text")

    pages = query_all(
        client,
        responses_db_id,
        filter={"property": session_prop, "relation": {"contains": session_id}}
        if session_prop
        else None,
        page_size=200,
    )

    rows: List[Dict[str, Any]] = []
    for page in pages:
        props = page.get("properties", {})
        qid = ""
        rel_ids = relation_ids(props, question_rel_prop)
        if rel_ids:
            qid = rel_ids[0]
        if not qid:
            qid = rich_text_value(props, question_id_prop)
        if not qid and title_prop:
            title_raw = title_value(props, title_prop)
            if title_raw.startswith("Q:") and " P:" in title_raw:
                qid = title_raw.split(" P:")[0].replace("Q:", "").strip()

        rows.append(
            {
                "id": page.get("id", ""),
                "player_id": (relation_ids(props, player_prop) or [""])[0],
                "question_id": qid,
                "value": from_json_text(rich_text_value(props, value_prop)),
                "tonight_note": rich_text_value(props, note_public_prop),
                "created_at": page.get("created_time", ""),
            }
        )

    return rows


def build_latest_by_player_question(
    rows: List[Dict[str, Any]],
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    latest: Dict[str, Dict[str, Dict[str, Any]]] = {}
    sorted_rows = sorted(rows, key=lambda r: str(r.get("created_at", "")), reverse=True)
    for row in sorted_rows:
        pid = str(row.get("player_id", ""))
        qid = str(row.get("question_id", ""))
        if not pid or not qid:
            continue
        latest.setdefault(pid, {})
        if qid not in latest[pid]:
            latest[pid][qid] = row
    return latest


def render_host_view(
    players: List[Dict[str, Any]],
    questions: List[Dict[str, Any]],
    responses: List[Dict[str, Any]],
) -> None:
    st.markdown("### 🔎 Vue hôte (debug)")

    question_by_key = {q["key"]: q["id"] for q in questions}
    latest = build_latest_by_player_question(responses)

    respondents = len([pid for pid in latest.keys() if pid])
    invited = len(players)

    st.markdown("#### 📊 Totaux")
    st.write(f"Répondants: **{respondents}** / Invités: **{invited}**")

    diet_counter = Counter()
    allergens_counter = Counter()
    hard_no_counter = Counter()
    spice_counter = Counter()
    cravings_counter = Counter()

    for player in players:
        for val in player.get("diet", []):
            diet_counter[val] += 1
        for val in player.get("allergens", []):
            allergens_counter[val] += 1
        for val in player.get("hard_no", []):
            hard_no_counter[val] += 1

    for pid, qmap in latest.items():
        spice_row = qmap.get(question_by_key.get("spice", ""), {})
        cravings_row = qmap.get(question_by_key.get("cravings", ""), {})
        spice_val = spice_row.get("value")
        if isinstance(spice_val, (int, float, str)):
            try:
                spice_counter[int(float(spice_val))] += 1
            except Exception:
                pass
        cravings_val = cravings_row.get("value")
        if isinstance(cravings_val, list):
            for c in cravings_val:
                cravings_counter[str(c)] += 1

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Régimes**")
        st.dataframe(
            pd.DataFrame(diet_counter.items(), columns=["régime", "total"]),
            hide_index=True,
            use_container_width=True,
        )
        st.markdown("**Allergènes**")
        st.dataframe(
            pd.DataFrame(allergens_counter.items(), columns=["allergène", "total"]),
            hide_index=True,
            use_container_width=True,
        )
    with c2:
        st.markdown("**Ingrédients non**")
        st.dataframe(
            pd.DataFrame(hard_no_counter.items(), columns=["ingrédient", "total"]),
            hide_index=True,
            use_container_width=True,
        )
        st.markdown("**Envies principales**")
        st.dataframe(
            pd.DataFrame(cravings_counter.most_common(10), columns=["envie", "total"]),
            hide_index=True,
            use_container_width=True,
        )

    if spice_counter:
        spice_df = pd.DataFrame(
            sorted(spice_counter.items()), columns=["niveau", "total"]
        )
        st.markdown("**Distribution du piquant (0-5)**")
        st.bar_chart(spice_df.set_index("niveau"))

    st.markdown("#### 👥 Détail par personne")
    only_critical = st.checkbox("Afficher seulement contraintes critiques", value=False)

    table_rows: List[Dict[str, Any]] = []
    for p in players:
        qmap = latest.get(p["id"], {})
        spice_val = qmap.get(question_by_key.get("spice", ""), {}).get("value", "")
        cravings_val = qmap.get(question_by_key.get("cravings", ""), {}).get(
            "value", []
        )
        tonight_payload = qmap.get(question_by_key.get("tonight_note", ""), {}).get(
            "value", ""
        )
        tonight_note = (
            tonight_payload.get("note", "")
            if isinstance(tonight_payload, dict)
            else tonight_payload
        )
        if not tonight_note:
            tonight_note = qmap.get(question_by_key.get("tonight_note", ""), {}).get(
                "tonight_note", ""
            )

        if only_critical and not (p.get("allergens") or p.get("hard_no")):
            continue

        table_rows.append(
            {
                "nom": p.get("name", ""),
                "régime": ", ".join(p.get("diet", [])),
                "allergènes": ", ".join(p.get("allergens", [])),
                "ingrédients non": ", ".join(p.get("hard_no", [])),
                "piquant": spice_val,
                "envies": ", ".join(
                    cravings_val if isinstance(cravings_val, list) else []
                ),
                "note bio": p.get("bio_note", ""),
                "note ce soir": tonight_note if isinstance(tonight_note, str) else "",
            }
        )

    table_df = pd.DataFrame(table_rows)
    st.dataframe(table_df, use_container_width=True, hide_index=True)

    st.markdown("#### 🧾 Export")
    csv_data = table_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Télécharger CSV (vue hôte)",
        data=csv_data,
        file_name=f"affranchis-host-{datetime.now().strftime('%Y%m%d-%H%M%S')}.csv",
        mime="text/csv",
        use_container_width=True,
    )


def main() -> None:
    page_started = perf_counter()
    st.set_page_config(
        page_title="Les Affranchis · Cuisine",
        page_icon="🍲",
        # layout="wide"
    )
    st.title("Les Affranchis · Cuisine")

    ensure_session_state()
    repo = timed_call("repo_init", get_notion_repo)
    if not repo:
        st.error("⚠️ Erreur : connexion Notion indisponible.")
        st.stop()
    authenticator = get_authenticator(repo)
    _, authentication_status, _ = ensure_auth(
        authenticator,
        callback=remember_access,
        key="affranchis-cuisine-cookie-auth",
        location="hidden",
    )
    if not authentication_status:
        st.warning("Please log in first.")
        if st.button("Go to Access", type="primary", use_container_width=True):
            st.switch_page("pages/01_Login.py")
        st.stop()
    authenticator.logout(button_name="Se déconnecter", location="sidebar")

    try:
        players_db_id = (repo.players_db_id or "").strip() or secret_required(
            "AFF_PLAYERS_DB_ID"
        )
        sessions_db_id = (repo.session_db_id or "").strip() or secret_required(
            "AFF_SESSIONS_DB_ID"
        )
        questions_db_id = (repo.questions_db_id or "").strip() or secret_required(
            "AFF_QUESTIONS_DB_ID"
        )
        responses_db_id = (repo.responses_db_id or "").strip() or secret_required(
            "AFF_RESPONSES_DB_ID"
        )
    except Exception as exc:
        st.error(f"⚠️ Erreur : {exc}")
        st.stop()

    client = repo.client

    try:
        sessions_schema = timed_call(
            "schema_sessions",
            lambda: cached_read(
                f"schema:{sessions_db_id}",
                lambda: safe_get_schema(client, sessions_db_id),
            ),
        )
        players_schema = timed_call(
            "schema_players",
            lambda: cached_read(
                f"schema:{players_db_id}",
                lambda: safe_get_schema(client, players_db_id),
            ),
        )
        questions_schema = timed_call(
            "schema_questions",
            lambda: cached_read(
                f"schema:{questions_db_id}",
                lambda: safe_get_schema(client, questions_db_id),
            ),
        )
        responses_schema = timed_call(
            "schema_responses",
            lambda: cached_read(
                f"schema:{responses_db_id}",
                lambda: safe_get_schema(client, responses_db_id),
            ),
        )
    except Exception as exc:
        st.error("⚠️ Erreur : impossible d'initialiser les schémas base de données.")
        st.error(f"🔍 Détail : {exc}")
        st.stop()

    sessions = timed_call(
        "sessions_list",
        lambda: cached_read(
            f"sessions:{sessions_db_id}",
            lambda: list_sessions(client, sessions_db_id, sessions_schema),
        ),
    )
    active_sessions = [s for s in sessions if s.get("active")]
    if len(active_sessions) == 1:
        selected_session = active_sessions[0]
    elif sessions:
        labels = [
            f"{s['code']} {'(active)' if s.get('active') else ''}" for s in sessions
        ]
        idx = 0
        selected_label = st.selectbox("Session", labels, index=idx)
        selected_session = sessions[labels.index(selected_label)]
    else:
        st.error("⚠️ Erreur : aucune session trouvée.")
        st.stop()

    players = timed_call(
        "players_list_initial",
        lambda: cached_read(
            f"players:{players_db_id}:{selected_session['id']}",
            lambda: list_players(
                client, players_db_id, players_schema, selected_session["id"]
            ),
        ),
    )

    logged_player_page_id = str(st.session_state.get("player_page_id", "")).strip()
    if not logged_player_page_id:
        st.error("⚠️ Erreur : identité affranchi•e manquante, reconnecte-toi.")
        st.stop()

    logged_player = timed_call(
        "logged_player_lookup",
        lambda: cached_read(
            f"player:{logged_player_page_id}",
            lambda: get_player_by_page_id(
                client, players_schema, logged_player_page_id
            ),
        ),
    )
    if not logged_player:
        st.error("⚠️ Erreur : affranchi•e introuvable dans la base joueurs.")
        st.stop()

    intro_play_key = (
        f"aff_cuisine_intro_played:{selected_session['id']}:{logged_player_page_id}"
    )
    intro_start_key = (
        f"aff_cuisine_intro_started:{selected_session['id']}:{logged_player_page_id}"
    )
    if not bool(st.session_state.get(intro_start_key, False)):
        if not bool(st.session_state.get(intro_play_key, False)):
            if st.button("Afficher tout", key=f"{intro_play_key}:skip"):
                st.session_state[intro_play_key] = True
                st.rerun()
            st.write_stream(_stream_chunk("### Bienvenue", punctuation_pause=0.14))
            sleep(0.45)
            st.write_stream(
                _stream_chunk("La session commence par quelques réponses simples.")
            )
            sleep(0.4)
            st.write_stream(
                _stream_chunk(
                    "Tu peux répondre en quelques minutes. Tes contraintes passent avant tout."
                )
            )
            sleep(0.34)
            st.write_stream(_stream_chunk("Ta réponse aide à préparer la suite."))
            st.session_state[intro_play_key] = True
        else:
            st.markdown("### Bienvenue")
            st.markdown("La session commence par quelques réponses simples.")
            st.markdown(
                "Tu peux répondre en quelques minutes. Tes contraintes passent avant tout."
            )
            st.markdown("Taes réponses aident à préparer la suite.")
        if st.button("Je commence", type="primary", use_container_width=True):
            st.session_state[intro_start_key] = True
            st.rerun()
        st.stop()

    presence_touch_key = (
        f"aff_presence_touch_ts:{selected_session['id']}:{logged_player_page_id}"
    )
    now_ts = time()
    touch_interval_seconds = 60.0
    touched, touch_err = True, ""
    last_touch_ts = float(st.session_state.get(presence_touch_key, 0.0) or 0.0)
    if now_ts - last_touch_ts >= touch_interval_seconds:
        touched, touch_err = timed_call(
            "presence_touch",
            lambda: touch_player_presence(
                logged_player_page_id,
                page="cuisine",
                session_slug=selected_session.get("code", ""),
            ),
        )
        if touched:
            st.session_state[presence_touch_key] = now_ts
    else:
        LOGGER.info(
            "presence.touch_skipped_recently player_id=%s age_s=%.1f",
            logged_player_page_id,
            now_ts - last_touch_ts,
        )
    if not touched and touch_err:
        LOGGER.warning(
            "presence.touch_failed player_id=%s error=%s",
            logged_player_page_id,
            touch_err,
        )

    presence_count_key = f"aff_presence_count_12h:{selected_session['id']}"
    cached_presence = st.session_state.get(presence_count_key, {})
    cached_count_ts = float(cached_presence.get("ts", 0.0) or 0.0)
    if now_ts - cached_count_ts >= 30.0:
        active_12h = timed_call(
            "presence_count_12h",
            lambda: count_active_users(
                window_minutes=12 * 60, session_id=selected_session["id"]
            ),
        )
        st.session_state[presence_count_key] = {"ts": now_ts, "value": active_12h}
    else:
        active_12h = int(cached_presence.get("value", 0) or 0)
        LOGGER.info(
            "presence.count_cached session_id=%s age_s=%.1f value=%s",
            selected_session["id"],
            now_ts - cached_count_ts,
            active_12h,
        )
    st.metric(
        "Affranchi·e·s actif·ve·s ces 12 dernières heures",
        value=active_12h,
    )

    st.markdown("## 0 · Participation")
    participate = st.radio(
        f"Je vais participer à la session « {selected_session['code']} » ?",
        options=["Oui", "Non"],
        horizontal=True,
    )
    if participate == "Oui":
        st.caption("Ta participation sera signalée à l’équipe cuisine.")
    else:
        st.caption("Tu peux poursuivre sans participer à cette partie.")

    participation_confirm_key = (
        f"aff_participation_confirmed:{selected_session['id']}:{logged_player_page_id}"
    )
    participation_choice_key = (
        f"aff_participation_choice:{selected_session['id']}:{logged_player_page_id}"
    )
    already_in_session = any(p.get("id") == logged_player_page_id for p in players)
    if already_in_session and participation_choice_key not in st.session_state:
        st.session_state[participation_choice_key] = "Oui"
        st.session_state[participation_confirm_key] = True

    if st.button("Confirmer", type="primary", use_container_width=True):
        st.session_state[participation_choice_key] = participate
        if participate == "Oui":
            join_steps = [
                {
                    "title": "🔄 Participation · Démarrage",
                    "msg": "Initialisation de la confirmation.",
                },
                {
                    "title": "🔎 Participation · Vérification",
                    "msg": "Vérification du lien participant-session.",
                },
                {
                    "title": "🧾 Étape suivante · Écriture",
                    "msg": "Écriture en base Notion du lien de participation.",
                },
            ]

            def _join_runner(push):
                try:
                    push(
                        "🧾 Étape suivante · Écriture",
                        "- Écriture en base Notion en cours…",
                    )
                    link_player_to_session(
                        client,
                        players_schema,
                        logged_player_page_id,
                        selected_session["id"],
                    )
                    st.session_state[participation_confirm_key] = True
                    push(
                        "✅ Participation · Terminée",
                        f"- Participation confirmée pour **{logged_player['name']}**.",
                    )
                    st.rerun()
                except Exception as exc:
                    push(
                        "❌ Participation · Échec",
                        "- Impossible de confirmer la participation.",
                    )
                    st.error("⚠️ Erreur : impossible de confirmer la participation.")
                    st.error(f"🔍 Détail : {exc}")
                    st.stop()

            run_progress_expander(join_steps, _join_runner)
        else:
            st.session_state[participation_confirm_key] = True
            st.rerun()

    if not bool(st.session_state.get(participation_confirm_key, False)):
        st.stop()

    players = timed_call(
        "players_list_after_join",
        lambda: cached_read(
            f"players:{players_db_id}:{selected_session['id']}",
            lambda: list_players(
                client, players_db_id, players_schema, selected_session["id"]
            ),
        ),
    )
    selected_player = (
        next((p for p in players if p["id"] == logged_player_page_id), None)
        or logged_player
    )

    st.caption(f"Affranchi·e actif·ve : **{selected_player['name']}**")

    diet_options = extract_multi_options(
        players_schema,
        "diet",
        ["vegan", "végétarien", "pescétarien", "halal", "kosher", "sans porc"],
    )
    allergens_options = extract_multi_options(
        players_schema,
        "allergens",
        ["gluten", "fruits à coque", "arachide", "sésame", "soja", "lactose", "œuf"],
    )
    hard_no_options = extract_multi_options(
        players_schema, "hard_no", ["ail", "oignon", "coriandre", "très épicé"]
    )

    try:
        questions = timed_call(
            "questions_ensure",
            lambda: cached_read(
                f"questions:{questions_db_id}:{selected_session['id']}",
                lambda: ensure_questions_for_session(
                    client,
                    questions_db_id,
                    questions_schema,
                    selected_session["id"],
                    diet_options,
                    allergens_options,
                    hard_no_options,
                ),
            ),
        )
    except Exception as exc:
        st.error("⚠️ Erreur : impossible de préparer le questionnaire.")
        st.error(f"🔍 Détail : {exc}")
        st.stop()

    qid_by_key = {q["key"]: q["id"] for q in questions}

    is_host = selected_player.get("role") == "host"
    ensure_questionnaire_state(
        selected_session["id"],
        selected_player["id"],
        selected_player,
    )
    values: Dict[str, Any] = st.session_state["aff_form_values"]

    mode = st.radio(
        "Mode",
        options=["Necessary", "Extended"],
        horizontal=True,
        index=0,
        help="Necessary = shortest form. Extended = includes texture and cravings.",
    )
    active_keys = (
        NECESSARY_KEYS if mode == "Necessary" else NECESSARY_KEYS + EXTENDED_ONLY_KEYS
    )
    active_catalog = [q for q in QUESTION_CATALOG if q["key"] in active_keys]

    question_index = int(st.session_state.get("aff_form_index", 0))
    total_questions = len(active_catalog)
    question_index = max(0, min(question_index, max(0, total_questions - 1)))
    st.session_state["aff_form_index"] = question_index
    current = active_catalog[question_index]
    key = current["key"]

    required_count = len([q for q in active_catalog if q.get("required")])
    answered_required = len(
        [
            q
            for q in active_catalog
            if q.get("required") and _required_answered(q["key"], values)
        ]
    )
    st.progress(answered_required / required_count if required_count else 0.0)
    st.caption(
        f"Progression: étape {question_index + 1}/{total_questions} · requis {answered_required}/{required_count}"
    )

    title_prompt = current["prompt"]
    if key == "diet":
        title_prompt = "Regime / preference"
    elif key == "spice":
        title_prompt = "Piquant & Condiments"
    st.markdown(f"## {question_index + 1}) {title_prompt}")
    if key == "diet":
        values["diet"] = st.multiselect(
            "Regime / preference", options=diet_options, default=values.get("diet", [])
        )
    elif key == "allergens":
        mode_options = ["Choose one", "Known allergens", "None known"]
        mode_value = values.get("allergens_mode", "")
        mode_index = mode_options.index(mode_value) if mode_value in mode_options else 0
        selected_mode = st.radio(
            "Allergènes (required)",
            options=mode_options,
            index=mode_index,
            horizontal=True,
        )
        values["allergens_mode"] = (
            selected_mode if selected_mode != "Choose one" else ""
        )
        values["allergens_none_known"] = values["allergens_mode"] == "None known"
        if values["allergens_none_known"]:
            values["allergens"] = []
            values["allergen_other"] = ""
            st.caption("No known allergens selected.")
        elif values["allergens_mode"] == "Known allergens":
            values["allergens"] = st.multiselect(
                "Allergens",
                options=allergens_options,
                default=values.get("allergens", []),
            )
            values["allergen_other"] = normalize_label(
                st.text_input(
                    "Other allergens (optional, comma-separated)",
                    value=values.get("allergen_other", ""),
                    help="Example: sesame, buckwheat, kiwi",
                )
            )
            st.caption("Use commas to add multiple custom allergens.")
        else:
            values["allergens"] = []
            values["allergen_other"] = ""
            st.info("Please choose either 'Known allergens' or 'None known'.")
    elif key == "hard_no":
        values["hard_no_none"] = (
            st.radio(
                "Ingredient exclusions",
                options=["I have exclusions", "No ingredient restrictions"],
                index=1 if bool(values.get("hard_no_none", False)) else 0,
                horizontal=True,
            )
            == "No ingredient restrictions"
        )
        if values["hard_no_none"]:
            values["hard_no"] = []
            values["hard_no_other"] = ""
            st.caption("No ingredient restrictions selected.")
        else:
            values["hard_no"] = st.multiselect(
                'Ingrédients "non"',
                options=hard_no_options,
                default=values.get("hard_no", []),
            )
            values["hard_no_other"] = normalize_label(
                st.text_input(
                    "Autre ingrédient non (optionnel)",
                    value=values.get("hard_no_other", ""),
                )
            )
    elif key == "spice":
        values["spice"] = st.slider(
            "Plaisir du piquant",
            min_value=0,
            max_value=5,
            value=int(values.get("spice", 3)),
            step=1,
        )
        st.caption("0 pas du tout · 3 ok · 5 à fond")
        st.caption("Condiments (j'évite = gauche, j'aime = droite)")
        c1, c2, c3 = st.columns(3)
        with c1:
            values["pref_ail"] = st.select_slider(
                "Ail",
                options=CONDIMENT_CHOICES,
                value=str(values.get("pref_ail", "ok")),
            )
        with c2:
            values["pref_oignon"] = st.select_slider(
                "Oignon",
                options=CONDIMENT_CHOICES,
                value=str(values.get("pref_oignon", "ok")),
            )
        with c3:
            values["pref_coriandre"] = st.select_slider(
                "Coriandre",
                options=CONDIMENT_CHOICES,
                value=str(values.get("pref_coriandre", "ok")),
            )
    elif key == "texture":
        values["texture"] = st.radio(
            "Texture",
            options=TEXTURE_OPTIONS,
            horizontal=True,
            index=TEXTURE_OPTIONS.index(values["texture"])
            if values.get("texture") in TEXTURE_OPTIONS
            else 0,
        )
    elif key == "cravings":
        values["cravings"] = (
            st.pills(
                "Choisis jusqu'à 2 envies",
                options=CRAVINGS_OPTIONS,
                selection_mode="multi",
                default=values.get("cravings", []),
            )
            or []
        )
        if len(values["cravings"]) > 2:
            st.warning("⚠️ Maximum 2 envies pour faciliter la synthèse cuisine.")
    elif key == "contribution":
        st.caption("Choose a suggested amount or enter another admissible amount.")
        st.markdown(
            """
            <style>
            div[data-testid="stButton"] button[kind="secondary"] { font-size: 2.5rem; font-weight: 900; padding-top: 1rem; padding-bottom: 1rem; }
            </style>
            """,
            unsafe_allow_html=True,
        )
        preset_amounts = [0, 1, 10, 15, 20, 30]
        amount_emoji = {0: "🪙", 1: "☎️", 10: "📞", 15: "📟", 20: "🔔", 30: "🥇"}
        selected_choice = str(values.get("contribution_choice", ""))
        contrib_mode_options = ["Choose one", "Use preset", "Other amount"]
        mode_key = "q-contribution-mode"
        if mode_key not in st.session_state:
            st.session_state[mode_key] = (
                "Other amount"
                if selected_choice == "Other amount"
                else (
                    "Use preset"
                    if selected_choice in {"0", "1", "10", "15", "20", "30"}
                    else "Choose one"
                )
            )
        st.markdown("**Preset amounts (EUR)**")
        _, center_col, _ = st.columns([1.7, 1.6, 1.7])
        with center_col:
            for row in [preset_amounts[:3], preset_amounts[3:]]:
                cols = st.columns(3)
                for idx, amount in enumerate(row):
                    is_selected = selected_choice == str(amount)
                    prefix = "✅ " if is_selected else ""
                    label = f"{prefix}{amount_emoji.get(amount, '☎️')} {amount}"
                    if cols[idx].button(
                        label,
                        key=f"eco-amount-{amount}",
                        use_container_width=True,
                        type="primary" if is_selected else "secondary",
                    ):
                        selected_choice = str(amount)
                        values["contribution_choice"] = selected_choice
                        values["contribution_value"] = float(amount)
                        values["contribution_custom"] = ""
                        st.session_state[mode_key] = "Use preset"
                        st.rerun()

        current_mode = st.session_state.get(mode_key, "Choose one")
        if current_mode not in contrib_mode_options:
            current_mode = "Choose one"
        contribution_mode = st.radio(
            "Or choose custom amount (EUR)",
            options=contrib_mode_options,
            index=contrib_mode_options.index(current_mode),
            horizontal=True,
            key=mode_key,
        )
        if contribution_mode == "Other amount":
            values["contribution_choice"] = "Other amount"
            custom_raw = st.text_input(
                "Other amount in EUR",
                value=str(values.get("contribution_custom", "")),
                help="Enter a number between 1 and 100000, or 0.",
            )
            values["contribution_custom"] = custom_raw
            try:
                values["contribution_value"] = float(custom_raw)
            except Exception:
                values["contribution_value"] = None
        elif contribution_mode == "Use preset":
            values["contribution_choice"] = selected_choice
            if selected_choice not in {"0", "1", "10", "15", "20", "30"}:
                values["contribution_choice"] = ""
                values["contribution_value"] = None
                st.info("Select one preset amount.")
            else:
                values["contribution_value"] = float(selected_choice)
        else:
            values["contribution_choice"] = ""
            values["contribution_custom"] = ""
            values["contribution_value"] = None
            st.info("Please choose a contribution mode.")

        if contribution_mode != "Choose one" and not _is_valid_contribution(
            values.get("contribution_value")
        ):
            st.error(
                "Invalid contribution amount. Use 0 or a value in [1, 100000] EUR."
            )

    st.session_state["aff_form_values"] = values
    is_required_current = bool(current.get("required"))
    can_next = (not is_required_current) or _required_answered(key, values)

    missing_items: List[str] = []
    for q in QUESTION_CATALOG:
        if q["key"] not in active_keys:
            continue
        if not q.get("required"):
            continue
        if _required_answered(q["key"], values):
            continue
        if q["key"] == "diet":
            missing_items.append("Regime / preference: pick at least one option.")
        elif q["key"] == "allergens":
            missing_items.append(
                "Allergènes: pick one allergen or choose 'None known'."
            )
        elif q["key"] == "hard_no":
            missing_items.append(
                "Ingrédients non: pick one exclusion or choose 'No ingredient restrictions'."
            )
        elif q["key"] == "spice":
            missing_items.append(
                "Piquant & Condiments: set your spice level and condiments."
            )
        elif q["key"] == "texture":
            missing_items.append("Texture: choose one texture option.")
        elif q["key"] == "cravings":
            missing_items.append("Envies du moment: choose at least one craving.")
    if not _is_valid_contribution(values.get("contribution_value")):
        missing_items.append(
            "Contribution (EUR): choose 0 / 1 / 10 / 15 / 20 / 30, or enter a custom amount in [1, 100000]."
        )

    if missing_items:
        st.info("Almost there. Please complete these required choices:")
        for item in missing_items:
            st.markdown(f"- {item}")

    submit_now = False
    col_back, col_next, col_submit = st.columns(3)
    with col_back:
        if st.button("Retour", use_container_width=True, disabled=question_index == 0):
            st.session_state["aff_form_index"] = max(0, question_index - 1)
            st.rerun()
    with col_next:
        if st.button(
            "Suivant",
            use_container_width=True,
            disabled=question_index >= total_questions - 1 or not can_next,
        ):
            st.session_state["aff_form_index"] = min(
                total_questions - 1, question_index + 1
            )
            st.rerun()
    with col_submit:
        save_ready = len(missing_items) == 0
        last_save = st.session_state.get("aff_last_save_summary")
        is_update = bool(
            isinstance(last_save, dict)
            and last_save.get("player_id") == selected_player["id"]
            and last_save.get("session_id") == selected_session["id"]
        )
        submit_now = st.button(
            "Mettre à jour les préférences" if is_update else "Enregistrer",
            type="primary",
            use_container_width=True,
            disabled=not save_ready,
        )
    last_save = st.session_state.get("aff_last_save_summary")
    if isinstance(last_save, dict):
        if (
            last_save.get("player_id") == selected_player["id"]
            and last_save.get("session_id") == selected_session["id"]
            and bool(st.session_state.get("aff_show_save_success_once", False))
        ):
            stream_key = "aff_save_confirmation_streamed"
            if not bool(st.session_state.get(stream_key, False)):
                st.write_stream(
                    _stream_chunk("Réponse enregistrée", punctuation_pause=0.14)
                )
                sleep(0.42)
                st.write_stream(
                    _stream_chunk("Merci. Ton signal a bien été pris en compte.")
                )
                sleep(0.36)
                st.write_stream(
                    _stream_chunk(
                        "Tu peux maintenant attendre la suite ou revenir plus tard avec ta clé."
                    )
                )
                st.session_state[stream_key] = True
            else:
                st.success("Réponse enregistrée")
                st.caption("Merci. Ton signal a bien été pris en compte.")
                st.caption(
                    "Tu peux maintenant attendre la suite ou revenir plus tard avec ta clé."
                )
            if st.session_state.pop("aff_show_balloons", False):
                st.balloons()
            st.markdown(
                f"<p style='font-size:1.45rem; line-height:1.5; font-weight:600; margin: 0.25rem 0 0.75rem 0;'>{short_save_summary(last_save.get('summary', {}))}</p>",
                unsafe_allow_html=True,
            )
            col_done, col_review = st.columns(2)
            with col_done:
                if st.button("Terminer", type="primary", use_container_width=True):
                    st.switch_page("pages/02_Home.py")
            with col_review:
                if st.button("Revoir mes réponses", use_container_width=True):
                    st.session_state["aff_show_save_success_once"] = False
                    st.rerun()
            st.session_state["aff_show_save_success_once"] = False

    if submit_now:
        if missing_items:
            st.error("⚠️ Some required inputs are still missing.")
            st.caption(
                "Please complete the items listed above, then click Enregistrer."
            )
            st.stop()

        save_steps = [
            {
                "title": "🔄 Sauvegarde · Démarrage",
                "msg": "Préparation des réponses.",
            },
            {
                "title": "🧍 Sauvegarde · Profil",
                "msg": "Mise à jour des champs persistants du participant.",
            },
            {
                "title": "🗳️ Sauvegarde · Réponses",
                "msg": "Upsert des réponses par question.",
            },
            {
                "title": "✅ Sauvegarde · Finalisation",
                "msg": "Validation finale des écritures.",
            },
        ]

        def _save_runner(push):
            try:
                LOGGER.info(
                    "save.start session_id=%s player_id=%s",
                    selected_session["id"],
                    selected_player["id"],
                )
                required_qids = list(active_keys)
                missing_qids = [k for k in required_qids if not qid_by_key.get(k)]
                if missing_qids:
                    push(
                        "🔧 Sauvegarde · Réparation",
                        "Question IDs manquants, ré-initialisation des questions.",
                    )
                    LOGGER.warning(
                        "save.missing_qids initial_missing=%s", ",".join(missing_qids)
                    )
                    _bust_cache()
                    refreshed_questions = ensure_questions_for_session(
                        client,
                        questions_db_id,
                        questions_schema,
                        selected_session["id"],
                        diet_options,
                        allergens_options,
                        hard_no_options,
                    )
                    refreshed_qid_by_key = {
                        q["key"]: q["id"] for q in refreshed_questions
                    }
                    missing_after_refresh = [
                        k for k in required_qids if not refreshed_qid_by_key.get(k)
                    ]
                    if missing_after_refresh:
                        push(
                            "⚠️ Sauvegarde · Réparation incomplète",
                            "Certaines questions restent absentes ; sauvegarde partielle.",
                        )
                        LOGGER.error(
                            "save.missing_qids after_refresh=%s",
                            ",".join(missing_after_refresh),
                        )
                    qid_map = refreshed_qid_by_key
                else:
                    qid_map = dict(qid_by_key)
                push("🧍 Sauvegarde · Profil", "Updating persistent player profile.")
                hard_no_merged = list(
                    dict.fromkeys(
                        [
                            *(values.get("hard_no") or []),
                            *(
                                [values.get("hard_no_other")]
                                if values.get("hard_no_other")
                                else []
                            ),
                        ]
                    )
                )
                allergen_custom_values = (
                    parse_csv_labels(values.get("allergen_other", ""))
                    if not bool(values.get("allergens_none_known", False))
                    else []
                )
                allergens_merged = list(
                    dict.fromkeys(
                        [
                            *(values.get("allergens") or []),
                            *allergen_custom_values,
                        ]
                    )
                )
                if bool(values.get("allergens_none_known", False)):
                    allergens_merged = []
                save_player_profile(
                    client,
                    selected_player["id"],
                    players_schema,
                    diet=list(values.get("diet") or []),
                    allergens=allergens_merged,
                    hard_no=hard_no_merged,
                    bio_note=str(values.get("bio_note", "")),
                )

                if values.get("hard_no_other") and is_host:
                    push("🔤 Sauvegarde · Options hard-no")
                    _ = ensure_multiselect_option(
                        client,
                        players_db_id,
                        "hard_no",
                        str(values.get("hard_no_other")),
                        similarity_threshold=0.90,
                    )
                    _bust_cache()
                if allergen_custom_values and is_host:
                    push("🔤 Sauvegarde · Options allergènes")
                    for allergen in allergen_custom_values:
                        _ = ensure_multiselect_option(
                            client,
                            players_db_id,
                            "allergens",
                            allergen,
                            similarity_threshold=0.90,
                        )
                    _bust_cache()

                push("🗳️ Sauvegarde · Réponses")
                if "diet" in active_keys and qid_map.get("diet"):
                    upsert_response(
                        client,
                        responses_db_id,
                        responses_schema,
                        session_id=selected_session["id"],
                        player_id=selected_player["id"],
                        question_id=qid_map["diet"],
                        value=list(values.get("diet") or []),
                    )
                if "allergens" in active_keys and qid_map.get("allergens"):
                    upsert_response(
                        client,
                        responses_db_id,
                        responses_schema,
                        session_id=selected_session["id"],
                        player_id=selected_player["id"],
                        question_id=qid_map["allergens"],
                        value=allergens_merged,
                    )
                if "hard_no" in active_keys and qid_map.get("hard_no"):
                    upsert_response(
                        client,
                        responses_db_id,
                        responses_schema,
                        session_id=selected_session["id"],
                        player_id=selected_player["id"],
                        question_id=qid_map["hard_no"],
                        value=hard_no_merged,
                    )
                if "spice" in active_keys and qid_map.get("spice"):
                    upsert_response(
                        client,
                        responses_db_id,
                        responses_schema,
                        session_id=selected_session["id"],
                        player_id=selected_player["id"],
                        question_id=qid_map["spice"],
                        value=int(values.get("spice", 3)),
                    )
                if "texture" in active_keys and qid_map.get("texture"):
                    upsert_response(
                        client,
                        responses_db_id,
                        responses_schema,
                        session_id=selected_session["id"],
                        player_id=selected_player["id"],
                        question_id=qid_map["texture"],
                        value=str(values.get("texture", "")),
                    )
                if "cravings" in active_keys and qid_map.get("cravings"):
                    upsert_response(
                        client,
                        responses_db_id,
                        responses_schema,
                        session_id=selected_session["id"],
                        player_id=selected_player["id"],
                        question_id=qid_map["cravings"],
                        value=[
                            normalize_label(v) for v in (values.get("cravings") or [])
                        ],
                    )
                contribution_value = values.get("contribution_value")
                if not _is_valid_contribution(contribution_value):
                    raise RuntimeError(
                        "Contribution is required and must be admissible."
                    )
                if qid_map.get("contribution"):
                    push(
                        "🗳️ Sauvegarde · Contribution",
                    )
                    upsert_response(
                        client,
                        responses_db_id,
                        responses_schema,
                        session_id=selected_session["id"],
                        player_id=selected_player["id"],
                        question_id=qid_map["contribution"],
                        value=float(contribution_value),
                    )
                else:
                    push(
                        "⚠️ Sauvegarde · Contribution",
                        "Question absente, contribution non enregistrée.",
                    )
                    LOGGER.warning(
                        "save.skip contribution session_id=%s player_id=%s",
                        selected_session["id"],
                        selected_player["id"],
                    )
                push("✅ Sauvegarde · Finalisation")
                summary = {
                    "diet": list(values.get("diet") or []),
                    "allergens": allergens_merged,
                    "hard_no": hard_no_merged,
                    "spice": int(values.get("spice", 3)),
                    "texture": str(values.get("texture", ""))
                    if "texture" in active_keys
                    else "—",
                    "cravings": [
                        normalize_label(v) for v in (values.get("cravings") or [])
                    ]
                    if "cravings" in active_keys
                    else [],
                    "contribution": float(contribution_value),
                }
                touched_after_save, touch_err_after_save = touch_player_presence(
                    selected_player["id"],
                    page="cuisine-save",
                    session_slug=selected_session.get("code", ""),
                )
                if not touched_after_save and touch_err_after_save:
                    LOGGER.warning(
                        "presence.touch_after_save_failed player_id=%s error=%s",
                        selected_player["id"],
                        touch_err_after_save,
                    )
                st.session_state["aff_last_save_summary"] = {
                    "session_id": selected_session["id"],
                    "player_id": selected_player["id"],
                    "summary": summary,
                }
                st.session_state["aff_show_save_success_once"] = True
                st.session_state["aff_save_confirmation_streamed"] = False
                LOGGER.info(
                    "save.success session_id=%s player_id=%s summary=%s",
                    selected_session["id"],
                    selected_player["id"],
                    json.dumps(summary, ensure_ascii=False),
                )
                st.session_state["aff_show_balloons"] = True
                st.success(
                    "✅ Merci. C'est noté. Tu peux modifier plus tard si besoin."
                )
                st.rerun()
            except Exception as exc:
                push("❌ Sauvegarde · Échec", repr(exc))
                st.error("⚠️ Erreur : impossible d'écrire dans base de données.")
                st.error(f"🔍 Détail : {exc}")
                st.code(traceback.format_exc(), language="text")
                LOGGER.exception("save.error")

        run_progress_expander(save_steps, _save_runner)

    LOGGER.info("perf.page_total_ms=%.1f", (perf_counter() - page_started) * 1000)

    if is_host:
        st.markdown("---")
        st.markdown("### 🛠️ Gestion des options (hôtes)")
        option_target = st.selectbox(
            "Propriété à enrichir",
            options=["diet", "allergens", "hard_no"],
            format_func=lambda v: {
                "diet": "Régime",
                "allergens": "Allergènes",
                "hard_no": "Ingrédients non",
            }[v],
        )
        new_option_label = st.text_input("Nouvelle option")
        if st.button("Ajouter l'option", disabled=not new_option_label.strip()):
            try:
                result = ensure_multiselect_option(
                    client,
                    players_db_id,
                    option_target,
                    normalize_label(new_option_label),
                    similarity_threshold=0.90,
                )
                _bust_cache()
                if result["status"] == "added":
                    st.success(f"✅ Option ajoutée : {result['added']}")
                elif result["status"] == "exists":
                    st.info(f"🔁 Option déjà existante : {result['existing']}")
                elif result["status"] == "similar":
                    st.warning(f"🤔 Option proche détectée : {result['existing']}")
                else:
                    st.warning("⚠️ Option invalide.")
            except Exception as exc:
                st.error("⚠️ Erreur : impossible de mettre à jour les options.")
                st.error(f"🔍 Détail : {exc}")

        responses = cached_read(
            f"responses:{responses_db_id}:{selected_session['id']}",
            lambda: load_responses_by_session(
                client,
                responses_db_id,
                responses_schema,
                selected_session["id"],
            ),
        )
        render_host_view(players, questions, responses)


if __name__ == "__main__":
    main()
