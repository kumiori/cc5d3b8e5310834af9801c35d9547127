from __future__ import annotations

import json
import os
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st
from notion_client import Client

from infra.notion_repo import get_database_schema
from lib.notion_options import ensure_multiselect_option


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


def debug(msg: str) -> None:
    st.markdown(msg)


def env_required(name: str) -> str:
    value = str(os.getenv(name, "")).strip()
    if not value:
        raise RuntimeError(f"Variable d'environnement manquante : {name}")
    return value


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_spaces(value: str) -> str:
    return " ".join((value or "").strip().split())


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
        raise RuntimeError(f"Impossible de requêter la base {db_id} (data_source introuvable).")
    return client.data_sources.query(data_source_id=ds_id, **kwargs)


def safe_get_schema(client: Client, db_id: str) -> Dict[str, Any]:
    try:
        return get_database_schema(client, db_id)
    except Exception:
        debug("⚠️ Échec de récupération du schéma.")
        debug("🔍 Vérification des data_sources…")
        raise


def find_prop(schema: Dict[str, Any], expected: str, ptype: Optional[str] = None) -> Optional[str]:
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
    return "".join(part.get("plain_text", "") for part in value.get("rich_text", []) if isinstance(part, dict))


def title_value(props: Dict[str, Any], prop_name: Optional[str]) -> str:
    if not prop_name:
        return ""
    value = props.get(prop_name)
    if not isinstance(value, dict) or value.get("type") != "title":
        return ""
    return "".join(part.get("plain_text", "") for part in value.get("title", []) if isinstance(part, dict))


def select_value(props: Dict[str, Any], prop_name: Optional[str]) -> str:
    if not prop_name:
        return ""
    value = props.get(prop_name)
    if not isinstance(value, dict) or value.get("type") != "select":
        return ""
    select = value.get("select") or {}
    return str(select.get("name", ""))


def multi_select_values(props: Dict[str, Any], prop_name: Optional[str]) -> List[str]:
    if not prop_name:
        return []
    value = props.get(prop_name)
    if not isinstance(value, dict) or value.get("type") != "multi_select":
        return []
    return [str(opt.get("name", "")) for opt in value.get("multi_select", []) if isinstance(opt, dict) and opt.get("name")]


def date_start(props: Dict[str, Any], prop_name: Optional[str]) -> str:
    if not prop_name:
        return ""
    value = props.get(prop_name)
    if not isinstance(value, dict) or value.get("type") != "date":
        return ""
    date_obj = value.get("date") or {}
    return str(date_obj.get("start", ""))


def relation_ids(props: Dict[str, Any], prop_name: Optional[str]) -> List[str]:
    if not prop_name:
        return []
    value = props.get(prop_name)
    if not isinstance(value, dict) or value.get("type") != "relation":
        return []
    return [str(item.get("id")) for item in value.get("relation", []) if isinstance(item, dict) and item.get("id")]


def extract_multi_options(schema: Dict[str, Any], prop_name: str, fallback: List[str]) -> List[str]:
    prop = schema.get(prop_name)
    if not isinstance(prop, dict):
        return fallback
    if prop.get("type") != "multi_select":
        return fallback
    multi = prop.get("multi_select") or {}
    options = multi.get("options") if isinstance(multi, dict) else []
    labels = [str(o.get("name", "")) for o in options if isinstance(o, dict) and o.get("name")]
    return labels or fallback


def load_active_session(client: Client, sessions_db_id: str, sessions_schema: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    active_prop = find_prop(sessions_schema, "active", "checkbox")
    title_prop = find_prop(sessions_schema, "Name", "title") or find_prop(sessions_schema, "session_code", "rich_text")

    query_args: Dict[str, Any] = {"page_size": 1, "sorts": [{"timestamp": "created_time", "direction": "descending"}]}
    if active_prop:
        query_args["filter"] = {"property": active_prop, "checkbox": {"equals": True}}

    payload = query_db(client, sessions_db_id, **query_args)
    results = payload.get("results", [])
    if not results:
        return None
    page = results[0]
    props = page.get("properties", {})

    code = title_value(props, title_prop)
    if not code:
        code = rich_text_value(props, find_prop(sessions_schema, "session_code", "rich_text"))

    return {"id": page.get("id", ""), "code": code or "GLOBAL-SESSION"}


def list_players(client: Client, players_db_id: str, players_schema: Dict[str, Any], session_id: str) -> List[Dict[str, Any]]:
    session_prop = find_prop(players_schema, "session", "relation")
    nickname_prop = find_prop(players_schema, "nickname", "rich_text")
    title_prop = find_prop(players_schema, "Name", "title")
    role_prop = find_prop(players_schema, "role", "select")
    bio_prop = find_prop(players_schema, "notes_public", "rich_text")
    diet_prop = find_prop(players_schema, "diet", "multi_select")
    allergens_prop = find_prop(players_schema, "allergens", "multi_select")
    hard_no_prop = find_prop(players_schema, "hard_no", "multi_select")

    args: Dict[str, Any] = {"page_size": 100}
    if session_prop:
        args["filter"] = {"property": session_prop, "relation": {"contains": session_id}}

    payload = query_db(client, players_db_id, **args)
    rows: List[Dict[str, Any]] = []
    for page in payload.get("results", []):
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
    props: Dict[str, Any] = {}
    diet_prop = find_prop(players_schema, "diet", "multi_select")
    allergens_prop = find_prop(players_schema, "allergens", "multi_select")
    hard_no_prop = find_prop(players_schema, "hard_no", "multi_select")
    bio_prop = find_prop(players_schema, "notes_public", "rich_text")

    if diet_prop:
        props[diet_prop] = {"multi_select": [{"name": v} for v in diet]}
    if allergens_prop:
        props[allergens_prop] = {"multi_select": [{"name": v} for v in allergens]}
    if hard_no_prop:
        props[hard_no_prop] = {"multi_select": [{"name": v} for v in hard_no]}
    if bio_prop:
        props[bio_prop] = {"rich_text": [{"type": "text", "text": {"content": bio_note}}]}

    if props:
        client.pages.update(page_id=player_id, properties=props)


def save_tonight_response(
    client: Client,
    responses_db_id: str,
    responses_schema: Dict[str, Any],
    *,
    session_id: str,
    player_id: str,
    spice: int,
    texture: str,
    cravings: List[str],
    tonight_note: str,
) -> None:
    title_prop = find_prop(responses_schema, "Name", "title")
    session_prop = find_prop(responses_schema, "session", "relation")
    player_prop = find_prop(responses_schema, "player", "relation")
    value_prop = find_prop(responses_schema, "value", "rich_text")
    value_number_prop = find_prop(responses_schema, "value_number", "number")
    note_public_prop = find_prop(responses_schema, "notes_public", "rich_text")
    created_prop = find_prop(responses_schema, "created_at", "date")

    payload_json = json.dumps(
        {
            "form_type": "affranchis_cuisine_v0",
            "spice_tolerance": spice,
            "texture": texture,
            "cravings": cravings,
        },
        ensure_ascii=False,
    )

    props: Dict[str, Any] = {}
    if title_prop:
        props[title_prop] = {
            "title": [
                {
                    "type": "text",
                    "text": {"content": f"Cuisine · {datetime.now().strftime('%Y-%m-%d %H:%M')}"},
                }
            ]
        }
    if session_prop:
        props[session_prop] = {"relation": [{"id": session_id}]}
    if player_prop:
        props[player_prop] = {"relation": [{"id": player_id}]}
    if value_prop:
        props[value_prop] = {"rich_text": [{"type": "text", "text": {"content": payload_json}}]}
    if value_number_prop:
        props[value_number_prop] = {"number": spice}
    if note_public_prop:
        props[note_public_prop] = {"rich_text": [{"type": "text", "text": {"content": tonight_note}}]}
    if created_prop:
        props[created_prop] = {"date": {"start": now_iso()}}

    client.pages.create(parent={"database_id": responses_db_id}, properties=props)


def load_tonight_responses(
    client: Client,
    responses_db_id: str,
    responses_schema: Dict[str, Any],
    session_id: str,
) -> List[Dict[str, Any]]:
    session_prop = find_prop(responses_schema, "session", "relation")
    player_prop = find_prop(responses_schema, "player", "relation")
    value_prop = find_prop(responses_schema, "value", "rich_text")
    note_public_prop = find_prop(responses_schema, "notes_public", "rich_text")
    created_prop = find_prop(responses_schema, "created_at", "date")

    args: Dict[str, Any] = {"page_size": 100, "sorts": [{"timestamp": "created_time", "direction": "descending"}]}
    if session_prop:
        args["filter"] = {"property": session_prop, "relation": {"contains": session_id}}

    payload = query_db(client, responses_db_id, **args)
    rows: List[Dict[str, Any]] = []
    for page in payload.get("results", []):
        props = page.get("properties", {})
        raw_json = rich_text_value(props, value_prop)
        parsed: Dict[str, Any] = {}
        try:
            parsed = json.loads(raw_json) if raw_json else {}
        except Exception:
            parsed = {}
        if parsed.get("form_type") != "affranchis_cuisine_v0":
            continue
        player_ids = relation_ids(props, player_prop)
        rows.append(
            {
                "id": page.get("id", ""),
                "player_id": player_ids[0] if player_ids else "",
                "spice": int(parsed.get("spice_tolerance", 0) or 0),
                "texture": str(parsed.get("texture", "")),
                "cravings": [str(v) for v in parsed.get("cravings", []) if v],
                "tonight_note": rich_text_value(props, note_public_prop),
                "created_at": date_start(props, created_prop) or page.get("created_time", ""),
            }
        )
    return rows


def render_host_view(players: List[Dict[str, Any]], responses: List[Dict[str, Any]]) -> None:
    st.markdown("### 🔎 Vue hôte (debug)")
    st.write(f"Participants: **{len(players)}**")

    spice_counter = Counter([max(0, min(5, int(r.get("spice", 0)))) for r in responses])
    if spice_counter:
        spice_df = pd.DataFrame(
            {"niveau": list(spice_counter.keys()), "total": list(spice_counter.values())}
        ).sort_values("niveau")
        st.markdown("**Distribution du piquant**")
        st.bar_chart(spice_df.set_index("niveau"))
    else:
        st.info("Aucune donnée de piquant pour le moment.")

    cravings_counter = Counter()
    for row in responses:
        for craving in row.get("cravings", []):
            cravings_counter[craving] += 1
    if cravings_counter:
        st.markdown("**Envies principales**")
        top_df = pd.DataFrame(cravings_counter.most_common(10), columns=["envie", "total"])
        st.dataframe(top_df, use_container_width=True, hide_index=True)
    else:
        st.info("Aucune envie enregistrée pour le moment.")

    latest_by_player: Dict[str, Dict[str, Any]] = {}
    for row in responses:
        pid = row.get("player_id", "")
        if pid and pid not in latest_by_player:
            latest_by_player[pid] = row

    table_rows: List[Dict[str, Any]] = []
    for player in players:
        latest = latest_by_player.get(player["id"], {})
        table_rows.append(
            {
                "name": player.get("name", ""),
                "diet": ", ".join(player.get("diet", [])),
                "allergens": ", ".join(player.get("allergens", [])),
                "hard-no": ", ".join(player.get("hard_no", [])),
                "cravings": ", ".join(latest.get("cravings", [])),
                "bio note": player.get("bio_note", ""),
                "tonight note": latest.get("tonight_note", ""),
            }
        )

    st.markdown("**Détails par personne**")
    st.dataframe(pd.DataFrame(table_rows), use_container_width=True, hide_index=True)


def main() -> None:
    st.set_page_config(page_title="Les Affranchis · Cuisine", page_icon="🍲", layout="wide")
    st.title("Les Affranchis · Cuisine")

    debug("🔄 App mise à jour !")
    debug("🐙 Récupération du code depuis Github...")
    debug("📦 Traitement des dépendances...")
    debug("📦 Dépendances traitées !")

    try:
        token = env_required("NOTION_TOKEN")
        players_db_id = env_required("AFF_PLAYERS_DB_ID")
        sessions_db_id = env_required("AFF_SESSIONS_DB_ID")
        responses_db_id = env_required("AFF_RESPONSES_DB_ID")
    except Exception as exc:
        st.error(str(exc))
        st.stop()

    client = Client(auth=token)

    try:
        sessions_schema = safe_get_schema(client, sessions_db_id)
        players_schema = safe_get_schema(client, players_db_id)
        responses_schema = safe_get_schema(client, responses_db_id)
    except Exception as exc:
        st.error(f"Erreur Notion: {exc}")
        st.stop()

    active_session = load_active_session(client, sessions_db_id, sessions_schema)
    if not active_session:
        st.error("Aucune session active trouvée.")
        st.stop()

    debug("🟢 Session Les Affranchis prête.")
    debug("🍲 Collecte des contraintes et des envies.")

    players = list_players(client, players_db_id, players_schema, active_session["id"])
    if not players:
        st.error("Aucun participant disponible dans la session active.")
        st.stop()

    player_names = [p["name"] for p in players]
    selected_name = st.selectbox("Participant", player_names)
    selected_player = next((p for p in players if p["name"] == selected_name), players[0])
    is_host = selected_player.get("role") == "host"

    st.markdown("## Étape 1 · Contraintes")
    diet_options = extract_multi_options(players_schema, "diet", ["vegan", "végétarien", "pescétarien", "halal", "kosher", "sans porc"])
    allergens_options = extract_multi_options(players_schema, "allergens", ["gluten", "fruits à coque", "arachide", "sésame", "soja", "lactose", "œuf"])
    hard_no_options = extract_multi_options(players_schema, "hard_no", ["ail", "oignon", "coriandre", "très épicé"])

    diet = st.multiselect("Régime", options=diet_options, default=selected_player.get("diet", []))
    allergens = st.multiselect("Allergènes", options=allergens_options, default=selected_player.get("allergens", []))
    hard_no = st.multiselect("Ingrédients interdits", options=hard_no_options, default=selected_player.get("hard_no", []))

    st.markdown("## Étape 2 · Préférences")
    spice = st.slider("Tolérance au piquant", min_value=0, max_value=5, value=2, step=1)
    texture = st.radio("Texture", options=["croquant", "crémeux", "mixte"], horizontal=True)

    st.markdown("## Étape 3 · Envies (ressenti)")
    cravings = st.pills(
        "Choisissez jusqu'à 2 envies",
        options=CRAVINGS_OPTIONS,
        selection_mode="multi",
        max_selections=2,
        default=[],
    )

    st.markdown("## Étape 4 · Notes publiques")
    bio_note = st.text_area(
        "Note bio (persistante)",
        value=selected_player.get("bio_note", ""),
        placeholder="Qui êtes-vous, en quelques mots ?",
    )
    tonight_note = st.text_area(
        "Note ce soir (partagée)",
        value="",
        placeholder="Votre humeur et vos envies de ce soir.",
    )

    if st.button("Enregistrer", type="primary", use_container_width=True):
        try:
            save_player_profile(
                client,
                selected_player["id"],
                players_schema,
                diet=diet,
                allergens=allergens,
                hard_no=hard_no,
                bio_note=bio_note,
            )
            save_tonight_response(
                client,
                responses_db_id,
                responses_schema,
                session_id=active_session["id"],
                player_id=selected_player["id"],
                spice=spice,
                texture=texture,
                cravings=[normalize_spaces(v) for v in (cravings or [])],
                tonight_note=tonight_note,
            )
            st.success("Réponses enregistrées.")
            st.rerun()
        except Exception as exc:
            st.error(f"Échec de sauvegarde: {exc}")

    if is_host:
        st.markdown("---")
        st.markdown("### 🛠️ Gestion des options (hôtes)")
        option_target = st.selectbox(
            "Propriété à enrichir",
            options=["diet", "allergens", "hard_no"],
            format_func=lambda v: {"diet": "Régime", "allergens": "Allergènes", "hard_no": "Ingrédients interdits"}[v],
        )
        new_option_label = st.text_input("Nouvelle option")
        if st.button("Ajouter l'option", disabled=not new_option_label.strip()):
            try:
                result = ensure_multiselect_option(
                    client,
                    players_db_id,
                    option_target,
                    new_option_label,
                    similarity_threshold=0.90,
                )
                if result["status"] == "added":
                    st.success(f"✅ Option ajoutée : {result['added']}")
                elif result["status"] == "exists":
                    st.info(f"🔁 Option déjà existante : {result['existing']}")
                elif result["status"] == "similar":
                    st.warning(f"🤔 Option proche détectée : {result['existing']}")
                else:
                    st.warning("⚠️ Option invalide.")
            except Exception as exc:
                st.error(f"Erreur lors de l'ajout: {exc}")

        responses = load_tonight_responses(
            client,
            responses_db_id,
            responses_schema,
            active_session["id"],
        )
        render_host_view(players, responses)


if __name__ == "__main__":
    main()
