import streamlit as st
from streamlit_notion import NotionConnection
from functools import lru_cache
import pandas as pd
from typing import Dict, Any, List, Optional
from typing import Any, Dict, cast
from infra.notion_repo import get_database_schema

st.set_page_config(
    page_title="Notion · Read/Write Test Bench", page_icon="🧪", layout="wide"
)
st.title("🧪 Notion · Read/Write Test Bench")
st.caption(
    "A small battery of interactive tests for `streamlit_notion.NotionConnection`."
)

# ---- Connection ----
try:
    conn = st.connection(
        "notion", type=NotionConnection, notion_api_key=st.secrets["notion"]["api_key"]
    )
except Exception as e:
    st.error(
        "Could not create Notion connection named 'notion'. "
        "Add it in your .streamlit/secrets.toml. Example:\n\n"
        "[connections.notion]\napi_key = 'secret_xxx'"
    )
    st.stop()


# ---- Utilities ----


@st.cache_data(ttl=600, show_spinner=False)
def list_dbs() -> Dict[str, Any]:
    data: Any = conn.list_databases()  # Sync or async by type
    return cast(Dict[str, Any], data)  # At runtime it’s a dict; silence the warning


def get_title_prop(schema: Dict[str, Any]) -> Optional[str]:
    """Return the property name of type 'title' for a database schema."""
    for name, meta in schema.items():
        if meta.get("type") == "title":
            return name
    return None


def db_as_row(db: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": db.get("title", [{}])[0].get("plain_text", ""),
        "id": db.get("id", ""),
        "url": db.get("url", ""),
        "created_time": db.get("created_time", ""),
        "last_edited_time": db.get("last_edited_time", ""),
    }


def page_title_from_props(page: Dict[str, Any]) -> str:
    props = page.get("properties", {})
    # try to find a title prop
    for prop_name, prop in props.items():
        if prop.get("type") == "title":
            parts = prop.get("title", [])
            return "".join([p.get("plain_text", "") for p in parts])
    return page.get("id", "")


def flatten_pages(pages: List[Dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for p in pages:
        rows.append(
            {
                "id": p.get("id"),
                "last_edited_time": p.get("last_edited_time"),
                "archived": p.get("archived"),
                "title": page_title_from_props(p),
            }
        )
    return pd.DataFrame(rows)


# ---- 1) List Databases ----
with st.expander("1) List databases", expanded=True):
    try:
        raw = list_dbs()
        dbs = raw.get("results", [])
        st.success(f"Found {len(dbs)} databases.")
        db_rows = [db_as_row(d) for d in dbs]
        st.dataframe(pd.DataFrame(db_rows), use_container_width=True)
    except Exception as e:
        st.exception(e)
        st.stop()

# ---- Choose a database ----
db_options = {
    (d.get("title", [{}])[0].get("plain_text", "") or d.get("id", "")): d for d in dbs
}
db_name = st.selectbox("Choose a database", list(db_options.keys()))

db = db_options[db_name]
db_id = db.get("id")
client = conn.api()
db_schema = get_database_schema(client, db_id)
title_prop = get_title_prop(db_schema)

colA, colB = st.columns(2)
with colA:
    st.write("**Selected DB ID:**", db_id)
with colB:
    st.write("**Title property:**", title_prop or "— not found —")

with st.expander("Database properties (schema)"):
    st.json(db_schema)

# ---- 2) Query Pages (Read) ----
st.subheader("2) Query pages (read)")
ttl = st.slider(
    "Query cache TTL (seconds)",
    min_value=0,
    max_value=7200,
    value=600,
    step=60,
    help="For conn.query(..., ttl=TTL). 0 disables caching.",
)
page_size = st.slider("Page size", 1, 100, 10)

if st.button("🗑️ Clear Cache"):
    st.cache_data.clear()
    st.success("Cache cleared!")

try:
    result = conn.query(db_id, ttl=ttl, page_size=page_size)
    st.write(result)
    pages = cast(List[Dict[str, Any]], result)
    st.success(f"Queried {len(pages)} page(s).")
    df = flatten_pages(pages)
    st.dataframe(df, use_container_width=True)
except Exception as e:
    st.exception(e)
    pages = []

# Keep a small id→page lookup
id2page = {p.get("id"): p for p in pages}

# ---- 3) Create Page (Write) ----
st.subheader("3) Create a test page (write)")
st.caption(
    "This creates a page in the selected database with best-effort property mapping."
)

with st.form("create_page_form"):
    new_title = st.text_input(
        "Title",
        value="Test page via Streamlit",
        placeholder="Required if database has a title prop",
    )
    notes = st.text_area(
        "Notes (rich_text property if available)", placeholder="Optional"
    )
    score = st.number_input(
        "Score (number property if available)",
        min_value=0.0,
        max_value=100.0,
        value=0.0,
    )
    tags_csv = st.text_input(
        "Tags (multi_select property if available)", placeholder="comma,separated,tags"
    )
    submitted_create = st.form_submit_button("Create page")

if submitted_create:
    try:
        props = {}
        # Title
        if title_prop:
            props[title_prop] = {
                "title": [
                    {"type": "text", "text": {"content": new_title or "Untitled"}}
                ]
            }
        # Find candidate properties by type, if they exist
        db_props = db_schema
        # rich_text
        rt_prop_name = next(
            (n for n, meta in db_props.items() if meta.get("type") == "rich_text"), None
        )
        if rt_prop_name and notes:
            props[rt_prop_name] = {
                "rich_text": [{"type": "text", "text": {"content": notes}}]
            }
        # number
        num_prop_name = next(
            (n for n, meta in db_props.items() if meta.get("type") == "number"), None
        )
        if num_prop_name:
            props[num_prop_name] = {"number": float(score)}
        # multi_select
        ms_prop_name = next(
            (n for n, meta in db_props.items() if meta.get("type") == "multi_select"),
            None,
        )
        if ms_prop_name and tags_csv.strip():
            opts = [t.strip() for t in tags_csv.split(",") if t.strip()]
            props[ms_prop_name] = {"multi_select": [{"name": o} for o in opts]}

        client = conn.api()  # raw Notion client
        created = client.pages.create(parent={"database_id": db_id}, properties=props)
        st.success(f"Created page: {page_title_from_props(created)}")
        st.json(created)
    except Exception as e:
        st.exception(e)

# ---- 4) Update Page (Write) ----
st.subheader("4) Update an existing page (write)")
if not pages:
    st.info("Query some pages first (above).")
else:
    page_ids = [p.get("id") for p in pages]
    sel_id = st.selectbox(
        "Choose a page to update",
        page_ids,
        format_func=lambda pid: page_title_from_props(id2page[pid]),
    )
    with st.form("update_page_form"):
        new_note = st.text_area(
            "Append note (rich_text block at top level)",
            placeholder="Will create a paragraph block",
        )
        archive_toggle = st.checkbox("Archive page (soft delete)")
        submitted_update = st.form_submit_button("Apply update")

    if submitted_update:
        try:
            client = conn.api()
            if new_note.strip():
                # Append a paragraph block to the page
                client.blocks.children.append(
                    sel_id,
                    children=[
                        {
                            "object": "block",
                            "type": "paragraph",
                            "paragraph": {
                                "rich_text": [
                                    {"type": "text", "text": {"content": new_note}}
                                ]
                            },
                        }
                    ],
                )
            if archive_toggle:
                client.pages.update(sel_id, archived=True)
            st.success("Update completed.")
        except Exception as e:
            st.exception(e)


def get_title_from_page(p: Dict[str, Any]) -> str:
    try:
        return "".join(t["plain_text"] for t in p["properties"]["key"]["title"])
    except Exception:
        return p.get("id", "")


def list_blocks(client, block_id: str, page_size: int = 50) -> List[Dict[str, Any]]:
    resp = client.blocks.children.list(block_id, page_size=page_size)
    results = resp.get("results", []) if isinstance(resp, dict) else []
    return cast(List[Dict[str, Any]], results)


# ---- 4) Update Page (Write) ----
st.subheader("4) Update an existing page (write)")

# Always fetch fresh page list here so you see latest rows
rows = conn.query(db_id, ttl=0, page_size=50)  # returns a list of pages (rows)
if not isinstance(rows, list) or len(rows) == 0:
    st.info("Query some pages first (above). No rows in this DB yet.")
else:
    # Build lookup
    id2page: Dict[str, Dict[str, Any]] = {p["id"]: p for p in rows}
    page_ids = list(id2page.keys())

    # Keep selectbox index stable
    sel_id_opt: Optional[str] = st.selectbox(
        "Choose a page to update",
        options=page_ids,
        format_func=lambda pid: get_title_from_page(id2page[pid]),
        index=0 if page_ids else None,
        key="update_page_selectbox",
    )

    if sel_id_opt is None:
        st.warning("No page selected.")
    else:
        sel_id: str = cast(str, sel_id_opt)

        # Show existing blocks (so you can see the body content, not just properties)
        client = conn.api()
        st.markdown("**Current blocks (top-level):**")
        current_blocks = list_blocks(client, sel_id, page_size=50)
        if not current_blocks:
            st.info("No blocks yet on this page.")
        else:
            # Show a compact view of block types and first 80 chars
            preview = []
            for b in current_blocks:
                btype = b.get("type", "")
                txt = ""
                try:
                    if btype == "paragraph":
                        rt = b[btype]["rich_text"]
                        txt = "".join([r["plain_text"] for r in rt])[:80]
                except Exception:
                    pass
                preview.append({"type": btype, "text": txt})
            st.dataframe(preview, use_container_width=True)

        with st.form("update_page_form_2"):
            new_note = st.text_area(
                "Append note (as a paragraph block)",
                placeholder="This will create a new paragraph block on the page body",
            )
            archive_toggle = st.checkbox("Archive page (soft delete)")
            submitted_update = st.form_submit_button("Apply update")

        if submitted_update:
            try:
                if new_note.strip():
                    client.blocks.children.append(
                        sel_id,
                        children=[
                            {
                                "object": "block",
                                "type": "paragraph",
                                "paragraph": {
                                    "rich_text": [
                                        {"type": "text", "text": {"content": new_note}}
                                    ]
                                },
                            }
                        ],
                    )

                if archive_toggle:
                    client.pages.update(sel_id, archived=True)

                # Re-fetch blocks to confirm
                updated_blocks = list_blocks(client, sel_id, page_size=50)
                st.success("Update completed. Refetched blocks below.")
                # Show the last paragraph block we just added (if any)
                if new_note.strip():
                    last_para = next(
                        (
                            b
                            for b in reversed(updated_blocks)
                            if b.get("type") == "paragraph"
                        ),
                        None,
                    )
                    if last_para:
                        rt = last_para["paragraph"]["rich_text"]
                        text = "".join([r.get("plain_text", "") for r in rt])
                        st.code(text)
                else:
                    st.info("No note appended (archived state may have changed).")

            except Exception as e:
                st.exception(e)
# ---- 5) Edge/negative tests ----
with st.expander("5) Edge/negative tests"):
    st.write("• Invalid database id → expect API error")
    bad = st.text_input("Bad database id", value="not-a-real-db-id")
    if st.button("Query bad id"):
        try:
            conn.query(bad, ttl=0, page_size=1)
            st.warning("Unexpectedly succeeded; check your inputs.")
        except Exception as e:
            st.success("Error surfaced as expected:")
            st.exception(e)

    st.write(
        "• Bad permissions: try creating in a DB without create permissions to verify error handling."
    )

st.markdown("---")
st.caption(
    "Tip: adjust TTL to verify the caching behavior of `conn.query`. 0 = no cache."
)
