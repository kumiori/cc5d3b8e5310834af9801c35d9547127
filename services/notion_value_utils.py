"""Utilities for reading loosely-typed Notion page property payloads."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional


def find_prop(
    schema: Dict[str, Any],
    expected: str,
    ptype: Optional[str] = None,
) -> str:
    """Return a property name, preferring `expected`, then first matching type."""
    if expected in schema:
        return expected
    if ptype:
        for name, meta in schema.items():
            if isinstance(meta, dict) and meta.get("type") == ptype:
                return str(name)
    return expected


def find_exact_prop(schema: Dict[str, Any], names: List[str], ptype: str) -> str:
    """Return the first exact property name in `names` matching the given type."""
    for name in names:
        meta = schema.get(name)
        if isinstance(meta, dict) and meta.get("type") == ptype:
            return name
    return ""


def read_rich_text(props: Dict[str, Any], name: str) -> str:
    """Read a Notion rich_text value into a plain string."""
    value = props.get(name)
    if not isinstance(value, dict) or value.get("type") != "rich_text":
        return ""
    return "".join(
        part.get("plain_text", "")
        for part in value.get("rich_text", [])
        if isinstance(part, dict)
    )


def read_title(props: Dict[str, Any], name: str) -> str:
    """Read a Notion title value into a plain string."""
    value = props.get(name)
    if not isinstance(value, dict) or value.get("type") != "title":
        return ""
    return "".join(
        part.get("plain_text", "")
        for part in value.get("title", [])
        if isinstance(part, dict)
    )


def read_relation_first(props: Dict[str, Any], name: str) -> str:
    """Return the first relation id for a property, or empty string."""
    value = props.get(name)
    if not isinstance(value, dict) or value.get("type") != "relation":
        return ""
    rel = value.get("relation", [])
    if rel and isinstance(rel[0], dict):
        return str(rel[0].get("id") or "")
    return ""


def relation_contains(props: Dict[str, Any], name: str, target_id: str) -> bool:
    """Return True when relation property contains the target id."""
    value = props.get(name)
    if not isinstance(value, dict) or value.get("type") != "relation":
        return False
    for item in value.get("relation", []):
        if isinstance(item, dict) and str(item.get("id") or "") == target_id:
            return True
    return False


def read_select_name(props: Dict[str, Any], name: str) -> str:
    """Read the selected option name for a select property."""
    value = props.get(name)
    if not isinstance(value, dict):
        return ""
    selected = value.get("select")
    if not isinstance(selected, dict):
        return ""
    return str(selected.get("name") or "")


def read_checkbox(props: Dict[str, Any], name: str) -> bool:
    """Read a checkbox property as bool."""
    value = props.get(name)
    if not isinstance(value, dict):
        return False
    return bool(value.get("checkbox"))


def read_number(props: Dict[str, Any], name: str) -> Optional[float]:
    """Read a number property as float when present."""
    value = props.get(name)
    if not isinstance(value, dict) or value.get("type") != "number":
        return None
    raw = value.get("number")
    if raw is None:
        return None
    try:
        return float(raw)
    except Exception:
        return None


def read_multiselect_names(props: Dict[str, Any], name: str) -> List[str]:
    """Read selected option names for a multi_select property."""
    value = props.get(name)
    if not isinstance(value, dict) or value.get("type") != "multi_select":
        return []
    out: List[str] = []
    for item in value.get("multi_select", []):
        if isinstance(item, dict) and item.get("name"):
            out.append(str(item["name"]))
    return out


def parse_json_text(text: str) -> Any:
    """Parse string as JSON when possible; otherwise return original text."""
    raw = str(text or "").strip()
    if not raw:
        return ""
    try:
        return json.loads(raw)
    except Exception:
        return raw


def as_list_labels(value: Any) -> List[str]:
    """Convert mixed JSON/list/string values to a clean list of non-empty labels."""
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        if text.startswith("[") and text.endswith("]"):
            try:
                parsed = json.loads(text)
                if isinstance(parsed, list):
                    return [str(v).strip() for v in parsed if str(v).strip()]
            except Exception:
                pass
        return [text]
    return []


def parse_number(value: Any) -> Optional[float]:
    """Convert int/float/string numeric payloads to float."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip().replace(",", ".")
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except Exception:
            return None
    return None
