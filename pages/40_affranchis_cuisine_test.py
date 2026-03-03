from __future__ import annotations

import json
import os
import re
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st
from notion_client import Client

from infra.app_context import get_authenticator, get_notion_repo
from infra.app_state import (
    ensure_auth,
    ensure_session_state,
    remember_access,
    require_login,
)
from infra.notion_repo import get_database_schema
from lib.notion_options import ensure_multiselect_option


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
        "prompt": "Envies (ressentis)",
        "kind": "craving",
        "qtype": "multi",
        "required": True,
        "max_select": 2,
        "order": 6,
    },
    {
        "key": "surprise",
        "prompt": "Surprise",
        "kind": "craving",
        "qtype": "single",
        "required": False,
        "order": 7,
    },
    {
        "key": "tonight_note",
        "prompt": "Un souhait pour ce soir",
        "kind": "notes",
        "qtype": "text",
        "required": False,
        "order": 8,
    },
]

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
    "surprise joueuse",
]

TEXTURE_OPTIONS = ["croquant", "crémeux", "mixte"]
CONDIMENT_CHOICES = ["j'aime", "ok", "j'évite"]


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


def run_progress_expander(steps: List[Dict[str, str]], runner):
    logs: List[str] = []
    placeholder = st.empty()

    def push(title: str, msg: str) -> None:
        logs.append(msg)
        with placeholder.container():
            with st.expander(title, expanded=True):
                for line in logs:
                    st.markdown(line)

    for step in steps:
        push(step["title"], f"- {step['msg']}")
    return runner(push)


def normalize_label(value: str) -> str:
    txt = re.sub(r"\s+", " ", (value or "").strip().lower())
    return txt


def now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def env_required(name: str) -> str:
    value = str(os.getenv(name, "")).strip()
    if not value:
        raise RuntimeError(f"Variable d'environnement manquante : {name}")
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
    st.set_page_config(
        page_title="Les Affranchis · Cuisine", page_icon="🍲", layout="wide"
    )
    st.title("Les Affranchis · Cuisine")

    debug("🔄 App mise à jour !")
    debug("🔐 Vérification de la connexion…")

    ensure_session_state()
    repo = get_notion_repo()
    if not repo:
        st.error("⚠️ Erreur : connexion Notion indisponible.")
        st.stop()
    authenticator = get_authenticator(repo)
    _, authentication_status, _ = ensure_auth(
        authenticator,
        callback=remember_access,
        key="affranchis-cuisine-login",
        location="sidebar",
    )
    require_login()
    if authentication_status:
        authenticator.logout(button_name="Se déconnecter", location="sidebar")

    try:
        players_db_id = (repo.players_db_id or "").strip() or env_required(
            "AFF_PLAYERS_DB_ID"
        )
        sessions_db_id = (repo.session_db_id or "").strip() or env_required(
            "AFF_SESSIONS_DB_ID"
        )
        questions_db_id = (repo.questions_db_id or "").strip() or env_required(
            "AFF_QUESTIONS_DB_ID"
        )
        responses_db_id = (repo.responses_db_id or "").strip() or env_required(
            "AFF_RESPONSES_DB_ID"
        )
    except Exception as exc:
        st.error(f"⚠️ Erreur : {exc}")
        st.stop()

    client = repo.client

    try:
        sessions_schema = cached_read(
            f"schema:{sessions_db_id}",
            lambda: safe_get_schema(client, sessions_db_id),
        )
        players_schema = cached_read(
            f"schema:{players_db_id}",
            lambda: safe_get_schema(client, players_db_id),
        )
        questions_schema = cached_read(
            f"schema:{questions_db_id}",
            lambda: safe_get_schema(client, questions_db_id),
        )
        responses_schema = cached_read(
            f"schema:{responses_db_id}",
            lambda: safe_get_schema(client, responses_db_id),
        )
    except Exception as exc:
        st.error("⚠️ Erreur : impossible d'initialiser les schémas base de données.")
        st.error(f"🔍 Détail : {exc}")
        st.stop()

    sessions = cached_read(
        f"sessions:{sessions_db_id}",
        lambda: list_sessions(client, sessions_db_id, sessions_schema),
    )
    active_sessions = [s for s in sessions if s.get("active")]
    if len(active_sessions) == 1:
        selected_session = active_sessions[0]
        debug(f"🗂️ Session active : {selected_session['code']}")
    elif sessions:
        debug("🗂️ Plusieurs sessions disponibles, sélection manuelle requise.")
        labels = [
            f"{s['code']} {'(active)' if s.get('active') else ''}" for s in sessions
        ]
        idx = 0
        selected_label = st.selectbox("Session", labels, index=idx)
        selected_session = sessions[labels.index(selected_label)]
    else:
        st.error("⚠️ Erreur : aucune session trouvée.")
        st.stop()

    players = cached_read(
        f"players:{players_db_id}:{selected_session['id']}",
        lambda: list_players(
            client, players_db_id, players_schema, selected_session["id"]
        ),
    )

    debug("🟢 Session prête")
    st.caption("Tu peux répondre en 2 minutes. Tes contraintes passent avant tout.")

    logged_player_page_id = str(st.session_state.get("player_page_id", "")).strip()
    if not logged_player_page_id:
        st.error("⚠️ Erreur : identité participant manquante, reconnecte-toi.")
        st.stop()

    logged_player = cached_read(
        f"player:{logged_player_page_id}",
        lambda: get_player_by_page_id(client, players_schema, logged_player_page_id),
    )
    if not logged_player:
        st.error("⚠️ Erreur : participant introuvable dans la base joueurs.")
        st.stop()

    st.markdown("## 0) Participation")
    participate = st.radio(
        f"Vas-tu participer à la session « {selected_session['code']} » ?",
        options=["Oui", "Non"],
        horizontal=True,
    )
    if participate == "Non":
        st.info("Merci. Tu pourras revenir plus tard si tu changes d'avis.")
        st.stop()

    player_state_key = (
        f"aff_participation_ok:{selected_session['id']}:{logged_player_page_id}"
    )
    already_in_session = any(p.get("id") == logged_player_page_id for p in players)
    if already_in_session and not st.session_state.get(player_state_key):
        st.session_state[player_state_key] = True

    if not st.session_state.get(player_state_key):
        st.info(
            "👉 Clique sur « Confirmer ma participation » pour valider l'inscription à la session."
        )
        if st.button(
            "Confirmer ma participation", type="primary", use_container_width=True
        ):
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
                    "title": "🧾 Participation · Écriture",
                    "msg": "Écriture en base Notion du lien de participation.",
                },
            ]

            def _join_runner(push):
                try:
                    push(
                        "🧾 Participation · Écriture",
                        "- Écriture en base Notion en cours…",
                    )
                    link_player_to_session(
                        client,
                        players_schema,
                        logged_player_page_id,
                        selected_session["id"],
                    )
                    st.session_state[player_state_key] = True
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
        st.stop()

    players = cached_read(
        f"players:{players_db_id}:{selected_session['id']}",
        lambda: list_players(
            client, players_db_id, players_schema, selected_session["id"]
        ),
    )
    selected_player = (
        next((p for p in players if p["id"] == logged_player_page_id), None)
        or logged_player
    )

    st.caption(f"Participant actif : **{selected_player['name']}**")

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

    with st.status(
        "🧱 Préparation du catalogue de questions…", expanded=False
    ) as seed_status:
        try:
            questions = cached_read(
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
            )
            seed_status.update(label="✅ Questions prêtes", state="complete")
        except Exception as exc:
            seed_status.update(
                label="⚠️ Erreur de préparation des questions", state="error"
            )
            st.error(f"🔍 Détail : {exc}")
            st.stop()

    qid_by_key = {q["key"]: q["id"] for q in questions}

    is_host = selected_player.get("role") == "host"

    st.markdown("## 1) Contraintes")
    diet = st.multiselect(
        "Régime", options=diet_options, default=selected_player.get("diet", [])
    )
    allergens = st.multiselect(
        "Allergènes",
        options=allergens_options,
        default=selected_player.get("allergens", []),
    )
    hard_no = st.multiselect(
        'Ingrédients "non"',
        options=hard_no_options,
        default=selected_player.get("hard_no", []),
    )
    hard_no_other = normalize_label(st.text_input("Autre ingrédient non (optionnel)"))

    st.markdown("## 2) Préférences")
    spice = st.slider("Tolérance au piquant", min_value=0, max_value=5, value=3, step=1)
    st.caption("0 pas du tout · 3 ok · 5 à fond")
    texture = st.radio("Texture", options=TEXTURE_OPTIONS, horizontal=True)
    st.markdown("Condiments (optionnel)")
    c1, c2, c3 = st.columns(3)
    with c1:
        pref_ail = st.select_slider("Ail", options=CONDIMENT_CHOICES, value="ok")
    with c2:
        pref_oignon = st.select_slider("Oignon", options=CONDIMENT_CHOICES, value="ok")
    with c3:
        pref_coriandre = st.select_slider(
            "Coriandre", options=CONDIMENT_CHOICES, value="ok"
        )

    st.markdown("## 3) Envies (ressentis)")
    cravings = st.pills(
        "Choisis jusqu'à 2 envies",
        options=CRAVINGS_OPTIONS,
        selection_mode="multi",
        # max_selections=2,
        default=[],
    )
    surprise = st.toggle("Surprise")
    if len(cravings or []) > 2:
        st.warning("⚠️ Maximum 2 envies pour faciliter la synthèse cuisine.")

    st.markdown("## 4) Notes")
    bio_note = st.text_area(
        "Une phrase sur toi (public, persistant)",
        value=selected_player.get("bio_note", ""),
    )
    tonight_note = st.text_area("Un souhait pour ce soir (public, session)", value="")

    if st.button("Enregistrer", type="primary", use_container_width=True):
        if not texture:
            st.error("⚠️ La texture est requise.")
            st.stop()

        save_steps = [
            {"title": "🔄 Sauvegarde · Démarrage", "msg": "Préparation des réponses."},
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
                push(
                    "🧍 Sauvegarde · Profil", "- Écriture des contraintes persistantes…"
                )
                hard_no_merged = list(
                    dict.fromkeys(
                        [*hard_no, *([hard_no_other] if hard_no_other else [])]
                    )
                )
                save_player_profile(
                    client,
                    selected_player["id"],
                    players_schema,
                    diet=diet,
                    allergens=allergens,
                    hard_no=hard_no_merged,
                    bio_note=bio_note,
                )

                if hard_no_other and is_host:
                    push(
                        "🔤 Sauvegarde · Normalisation",
                        '- Vérification typo/duplication pour "autre".',
                    )
                    _ = ensure_multiselect_option(
                        client,
                        players_db_id,
                        "hard_no",
                        hard_no_other,
                        similarity_threshold=0.90,
                    )

                push("🗳️ Sauvegarde · Réponses", "- Écriture des réponses de session…")
                upsert_response(
                    client,
                    responses_db_id,
                    responses_schema,
                    session_id=selected_session["id"],
                    player_id=selected_player["id"],
                    question_id=qid_by_key["diet"],
                    value=diet,
                )
                upsert_response(
                    client,
                    responses_db_id,
                    responses_schema,
                    session_id=selected_session["id"],
                    player_id=selected_player["id"],
                    question_id=qid_by_key["allergens"],
                    value=allergens,
                )
                upsert_response(
                    client,
                    responses_db_id,
                    responses_schema,
                    session_id=selected_session["id"],
                    player_id=selected_player["id"],
                    question_id=qid_by_key["hard_no"],
                    value=hard_no_merged,
                )
                upsert_response(
                    client,
                    responses_db_id,
                    responses_schema,
                    session_id=selected_session["id"],
                    player_id=selected_player["id"],
                    question_id=qid_by_key["spice"],
                    value=spice,
                )
                upsert_response(
                    client,
                    responses_db_id,
                    responses_schema,
                    session_id=selected_session["id"],
                    player_id=selected_player["id"],
                    question_id=qid_by_key["texture"],
                    value=texture,
                )
                upsert_response(
                    client,
                    responses_db_id,
                    responses_schema,
                    session_id=selected_session["id"],
                    player_id=selected_player["id"],
                    question_id=qid_by_key["cravings"],
                    value=[normalize_label(v) for v in (cravings or [])],
                )
                upsert_response(
                    client,
                    responses_db_id,
                    responses_schema,
                    session_id=selected_session["id"],
                    player_id=selected_player["id"],
                    question_id=qid_by_key["surprise"],
                    value=bool(surprise),
                )
                upsert_response(
                    client,
                    responses_db_id,
                    responses_schema,
                    session_id=selected_session["id"],
                    player_id=selected_player["id"],
                    question_id=qid_by_key["tonight_note"],
                    value={
                        "note": tonight_note,
                        "condiments": {
                            "ail": pref_ail,
                            "oignon": pref_oignon,
                            "coriandre": pref_coriandre,
                        },
                    },
                    tonight_note=tonight_note,
                )

                push(
                    "✅ Sauvegarde · Finalisation",
                    "- Réponses enregistrées avec succès.",
                )
                st.success(
                    "✅ Merci. C'est noté. Tu peux modifier plus tard si besoin."
                )
                st.rerun()
            except Exception as exc:
                push(
                    "❌ Sauvegarde · Échec",
                    "- Impossible d'écrire dans la base Notion.",
                )
                st.error("⚠️ Erreur : impossible d'écrire dans base de données.")
                detail = str(exc)
                if is_host:
                    st.error(f"🔍 Détail : {detail}")

        run_progress_expander(save_steps, _save_runner)

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
