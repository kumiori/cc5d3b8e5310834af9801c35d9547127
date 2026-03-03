#!/usr/bin/env python3
import os
import sys
import json
import datetime as dt
from copy import deepcopy
from notion_client import Client


# -------------------------
# Helpers
# -------------------------
def iso_now():
    return dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def assert_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var {name}")
    return v


def db_update(client: Client, db_id: str, properties: dict):
    """
    notion-client>=3 stores editable schema on data_sources.
    Fall back to databases.update for older workspaces/clients.
    """
    db = client.databases.retrieve(database_id=db_id)
    ds_list = db.get("data_sources") or []
    if ds_list and hasattr(client, "data_sources") and hasattr(client.data_sources, "update"):
        ds_id = ds_list[0]["id"]
        # data_sources.update expects relation targets as data_source_id (not database_id)
        prepared_properties = deepcopy(properties)
        for _, prop_spec in prepared_properties.items():
            if not isinstance(prop_spec, dict):
                continue
            rel = prop_spec.get("relation")
            if not isinstance(rel, dict):
                continue
            target_db_id = rel.get("database_id")
            if not target_db_id or rel.get("data_source_id"):
                continue

            target_db = client.databases.retrieve(database_id=target_db_id)
            target_ds_list = target_db.get("data_sources") or []
            if not target_ds_list:
                raise RuntimeError(
                    f"Relation target db_id={target_db_id} has no data_sources"
                )
            rel["data_source_id"] = target_ds_list[0]["id"]
            rel.pop("database_id", None)

        return client.data_sources.update(
            data_source_id=ds_id, properties=prepared_properties
        )
    return client.databases.update(database_id=db_id, properties=properties)


def get_database_schema(client: Client, db_id: str) -> dict:
    """
    Notion has 'databases' and newer 'data_sources'.
    Some databases return schema columns via data_sources rather than database.properties.
    """
    db = client.databases.retrieve(database_id=db_id)

    props = db.get("properties")
    if isinstance(props, dict) and props:
        return props  # legacy / classic behavior

    # New behavior: schema lives in data_sources
    ds_list = db.get("data_sources") or []
    if not ds_list:
        raise RuntimeError(f"No properties and no data_sources for db_id={db_id}")

    ds_id = ds_list[0]["id"]

    # notion-sdk-py supports this if your notion-client version is recent enough
    ds = client.data_sources.retrieve(data_source_id=ds_id)
    ds_props = ds.get("properties")
    if not isinstance(ds_props, dict):
        raise RuntimeError(
            f"Could not retrieve data source properties for ds_id={ds_id}"
        )

    return ds_props


def db_retrieve(client: Client, db_id: str):
    return client.databases.retrieve(database_id=db_id)


def db_query(client: Client, db_id: str, filter_obj: dict):
    """
    notion-client>=3 uses data_sources.query (not databases.query).
    Keep compatibility with older clients that still expose databases.query.
    """
    if hasattr(client.databases, "query"):
        return client.databases.query(database_id=db_id, filter=filter_obj)

    db = db_retrieve(client, db_id)
    ds_list = db.get("data_sources") or []
    if not ds_list:
        raise RuntimeError(
            f"Cannot query db_id={db_id}: no databases.query and no data_sources found"
        )

    ds_id = ds_list[0]["id"]
    return client.data_sources.query(data_source_id=ds_id, filter=filter_obj)


def ensure_page_in_db_by_title(
    client: Client, db_id: str, title_prop: str, title_value: str, extra_props: dict
):
    # Query by title equals
    res = db_query(
        client,
        db_id,
        filter_obj={"property": title_prop, "title": {"equals": title_value}},
    )
    if res.get("results"):
        return res["results"][0]["id"], False

    created = client.pages.create(
        parent={"database_id": db_id},
        properties={
            title_prop: {"title": [{"type": "text", "text": {"content": title_value}}]},
            **extra_props,
        },
    )
    return created["id"], True


def print_db_properties(client: Client, db_id: str, label: str):
    db = db_retrieve(client, db_id)
    # props = db.get("properties", {})
    props = get_database_schema(client, db_id)
    print(f"\n[{label}] properties:")
    for k, v in props.items():
        print(f"  - {k}: {v.get('type')}")


# -------------------------
# Main
# -------------------------
def main():
    token = assert_env("NOTION_TOKEN")

    # Database IDs (use your exports)
    PLAYERS_DB_ID = assert_env("AFF_PLAYERS_DB_ID")
    SESSIONS_DB_ID = assert_env("AFF_SESSIONS_DB_ID")
    STATEMENTS_DB_ID = assert_env("AFF_STATEMENTS_DB_ID")
    RESPONSES_DB_ID = assert_env("AFF_RESPONSES_DB_ID")
    QUESTIONS_DB_ID = assert_env("AFF_QUESTIONS_DB_ID")
    VOTES_DB_ID = assert_env("AFF_VOTES_DB_ID")
    DECISIONS_DB_ID = assert_env("AFF_DECISIONS_DB_ID")

    client = Client(auth=token)

    # --- 1) Players schema additions (AFFRANCHIS)
    db_update(
        client,
        PLAYERS_DB_ID,
        properties={
            # existing
            "access_key": {"rich_text": {}},
            "emoji": {"rich_text": {}},
            "emoji_suffix_4": {"rich_text": {}},
            "emoji_suffix_6": {"rich_text": {}},
            "phrase": {"rich_text": {}},
            "status": {
                "select": {
                    "options": [
                        {"name": "active", "color": "green"},
                        {"name": "revoked", "color": "red"},
                    ]
                }
            },
            # new
            "role": {
                "select": {
                    "options": [
                        {"name": "host", "color": "purple"},
                        {"name": "guest", "color": "blue"},
                    ]
                }
            },
            "notes_public": {"rich_text": {}},  # shared
            "notes_private": {"rich_text": {}},  # internal
            "diet": {
                "multi_select": {
                    "options": [
                        {"name": "vegan", "color": "green"},
                        {"name": "vegetarian", "color": "green"},
                        {"name": "pescetarian", "color": "blue"},
                        {"name": "no pork", "color": "red"},
                        {"name": "halal", "color": "yellow"},
                        {"name": "kosher", "color": "yellow"},
                    ]
                }
            },
            "allergens": {
                "multi_select": {
                    "options": [
                        {"name": "gluten", "color": "red"},
                        {"name": "nuts", "color": "red"},
                        {"name": "peanuts", "color": "red"},
                        {"name": "sesame", "color": "orange"},
                        {"name": "soy", "color": "orange"},
                        {"name": "lactose", "color": "yellow"},
                        {"name": "egg", "color": "yellow"},
                    ]
                }
            },
            "hard_no": {
                "multi_select": {
                    "options": [
                        {"name": "garlic", "color": "red"},
                        {"name": "onion", "color": "red"},
                        {"name": "coriander", "color": "orange"},
                        {"name": "very spicy", "color": "orange"},
                    ]
                }
            },
        },
    )
    # --- 2) Sessions schema
    # Keep the existing title property name (likely "Title" or "Name"). We will not rename it.
    db_update(
        client,
        SESSIONS_DB_ID,
        properties={
            "active": {"checkbox": {}},
            "start": {"date": {}},
            "end": {"date": {}},
            "created_at": {"date": {}},
            "notes": {"rich_text": {}},
            "session_code": {"rich_text": {}},  # IMPORTANT for your UI labels
            "tone": {
                "select": {
                    "options": [
                        {"name": "affranchissement", "color": "purple"},
                        {"name": "kitchen", "color": "green"},
                    ]
                }
            },
        },
    )

    # --- 3) Statements schema
    db_update(
        client,
        STATEMENTS_DB_ID,
        properties={
            "theme": {
                "select": {
                    "options": [
                        {"name": "irreversibility", "color": "blue"},
                        {"name": "antarctica-commons", "color": "yellow"},
                        {"name": "agency", "color": "green"},
                        {"name": "emotion-rationality", "color": "purple"},
                        {"name": "science-dialogue", "color": "gray"},
                        {"name": "other", "color": "default"},
                    ]
                }
            },
            "active": {"checkbox": {}},
            "order": {"number": {"format": "number"}},
            # relation added below
        },
    )

    # --- 4) Responses schema
    db_update(
        client,
        RESPONSES_DB_ID,
        properties={
            "value": {"number": {"format": "number"}},
            "level_label": {
                "select": {
                    "options": [
                        {"name": "dissonance", "color": "red"},
                        {"name": "low", "color": "orange"},
                        {"name": "neutral", "color": "gray"},
                        {"name": "high", "color": "blue"},
                        {"name": "full", "color": "green"},
                    ]
                }
            },
            "note": {"rich_text": {}},
            "created_at": {"date": {}},
            # relations added below
        },
    )

    # --- 5) Questions schema
    db_update(
        client,
        QUESTIONS_DB_ID,
        properties={
            "kind": {
                "select": {
                    "options": [
                        {"name": "constraint", "color": "red"},
                        {"name": "preference", "color": "blue"},
                        {"name": "craving", "color": "purple"},
                        {"name": "logistics", "color": "gray"},
                        {"name": "power", "color": "yellow"},
                    ]
                }
            },
            "qtype": {
                "select": {
                    "options": [
                        {"name": "single", "color": "blue"},
                        {"name": "multi", "color": "purple"},
                        {"name": "text", "color": "gray"},
                    ]
                }
            },
            "max_select": {"number": {"format": "number"}},
            "required": {"checkbox": {}},
            "order": {"number": {"format": "number"}},
            "options_json": {"rich_text": {}},  # store list as JSON string
            "created_at": {"date": {}},
            "last_updated": {"date": {}},
            # keep your relations "session" and "submitted_by" as-is below
        },
    )
    # --- 6) ModerationVotes schema
    db_update(
        client,
        VOTES_DB_ID,
        properties={
            "vote": {
                "select": {
                    "options": [
                        {"name": "approve", "color": "green"},
                        {"name": "rewrite", "color": "orange"},
                        {"name": "park", "color": "red"},
                    ]
                }
            },
            "created_at": {"date": {}},
            # relations added below
        },
    )

    # --- 7) Decisions schema
    db_update(
        client,
        DECISIONS_DB_ID,
        properties={
            "type": {
                "select": {
                    "options": [
                        {"name": "description_status", "color": "blue"},
                        {"name": "journey_A", "color": "gray"},
                        {"name": "journey_B", "color": "gray"},
                        {"name": "structure_choice", "color": "purple"},
                    ]
                }
            },
            "payload": {"rich_text": {}},
            "created_at": {"date": {}},
            # relations added below
        },
    )

    # --- 8) Relations (official Notion API syntax)
    # Note: relations have to be created on both sides if you want both-direction UX;
    # Notion will create a paired property if you include "synced_property_name".
    #
    # We'll create one-way relations for v0 (simpler). You can later add the reverse in UI.
    db_update(
        client,
        STATEMENTS_DB_ID,
        properties={
            "session": {
                "relation": {
                    "database_id": SESSIONS_DB_ID,
                    "dual_property": {"synced_property_name": "statements"},
                }
            }
        },
    )
    db_update(
        client,
        RESPONSES_DB_ID,
        properties={
            "value": {
                "rich_text": {}
            },  # change to rich_text so you can store JSON or text safely
            "value_number": {"number": {"format": "number"}},  # optional
            "notes_public": {"rich_text": {}},  # shared
            "notes_private": {"rich_text": {}},
            "created_at": {"date": {}},
            "session": {
                "relation": {
                    "database_id": SESSIONS_DB_ID,
                    "dual_property": {"synced_property_name": "responses"},
                }
            },
            "player": {
                "relation": {
                    "database_id": PLAYERS_DB_ID,
                    "dual_property": {"synced_property_name": "responses"},
                }
            },
            "statement": {
                "relation": {
                    "database_id": STATEMENTS_DB_ID,
                    "dual_property": {"synced_property_name": "responses"},
                }
            },
        },
    )
    db_update(
        client,
        QUESTIONS_DB_ID,
        properties={
            "session": {
                "relation": {
                    "database_id": SESSIONS_DB_ID,
                    "dual_property": {"synced_property_name": "questions"},
                }
            },
            "submitted_by": {
                "relation": {
                    "database_id": PLAYERS_DB_ID,
                    "dual_property": {"synced_property_name": "questions_submitted"},
                }
            },
        },
    )
    db_update(
        client,
        VOTES_DB_ID,
        properties={
            "session": {
                "relation": {
                    "database_id": SESSIONS_DB_ID,
                    "dual_property": {"synced_property_name": "moderation_votes"},
                }
            },
            "question": {
                "relation": {
                    "database_id": QUESTIONS_DB_ID,
                    "dual_property": {"synced_property_name": "moderation_votes"},
                }
            },
            "voter": {
                "relation": {
                    "database_id": PLAYERS_DB_ID,
                    "dual_property": {"synced_property_name": "moderation_votes"},
                }
            },
        },
    )
    db_update(
        client,
        DECISIONS_DB_ID,
        properties={
            "session": {
                "relation": {
                    "database_id": SESSIONS_DB_ID,
                    "dual_property": {"synced_property_name": "decisions"},
                }
            },
            "player": {
                "relation": {
                    "database_id": PLAYERS_DB_ID,
                    "dual_property": {"synced_property_name": "decisions"},
                }
            },
        },
    )
    # --- 9) Seed GLOBAL-SESSION
    # Need the actual title property name in Sessions ("Title" vs "Name")
    # sessions_db = db_retrieve(client, SESSIONS_DB_ID)
    # title_prop = None

    # sessions_db = db_retrieve(client, SESSIONS_DB_ID)

    # props = sessions_db.get("properties")
    # if not isinstance(props, dict):
    #     raise RuntimeError(
    #         "SESSIONS DB retrieve did not return a database object.\n"
    #         f"Check SESSIONS_DB_ID and integration access.\n"
    #         f"Got: {json.dumps(sessions_db, indent=2)[:2000]}"
    #     )
    props = get_database_schema(client, SESSIONS_DB_ID)
    title_prop = None
    for prop_name, prop in props.items():
        if prop.get("type") == "title":
            title_prop = prop_name
            break
    if not title_prop:
        raise RuntimeError("Could not find title property in Sessions")

    session_id, created = ensure_page_in_db_by_title(
        client,
        SESSIONS_DB_ID,
        title_prop,
        "GLOBAL-SESSION",
        extra_props={
            "active": {"checkbox": True},
            "created_at": {"date": {"start": iso_now()}},
            "notes": {"rich_text": [{"type": "text", "text": {"content": "v0 seed"}}]},
        },
    )
    print(f"\nGLOBAL-SESSION: {'created' if created else 'exists'} ({session_id})")

    # --- 10) Print property lists as verification
    print_db_properties(client, PLAYERS_DB_ID, "Players")
    print_db_properties(client, SESSIONS_DB_ID, "Sessions")
    print_db_properties(client, STATEMENTS_DB_ID, "Statements")
    print_db_properties(client, RESPONSES_DB_ID, "Responses")
    print_db_properties(client, QUESTIONS_DB_ID, "Questions")
    print_db_properties(client, VOTES_DB_ID, "ModerationVotes")
    print_db_properties(client, DECISIONS_DB_ID, "Decisions")

    print("\nDone. Refresh Notion UI; you should see new properties and relations.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("\n[ERROR]", e)
        sys.exit(1)
