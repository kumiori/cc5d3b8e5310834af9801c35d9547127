from __future__ import annotations

import re
from typing import Any, Dict, Optional

from infra.notion_repo import get_database_schema

try:
    from rapidfuzz import fuzz  # type: ignore
except Exception:  # pragma: no cover
    fuzz = None  # type: ignore


def _normalize_label(label: str) -> str:
    text = re.sub(r"\s+", " ", (label or "").strip().lower())
    return text


def _similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    if fuzz is not None:
        return float(fuzz.ratio(a, b)) / 100.0
    if a == b:
        return 1.0
    shorter = min(len(a), len(b))
    if shorter == 0:
        return 0.0
    overlap = sum(1 for i in range(shorter) if a[i] == b[i])
    return overlap / max(len(a), len(b))


def _resolve_data_source_id(client: Any, database_id: str) -> Optional[str]:
    try:
        db = client.databases.retrieve(database_id=database_id)
    except Exception:
        return None
    ds_list = db.get("data_sources") if isinstance(db, dict) else None
    if not isinstance(ds_list, list) or not ds_list:
        return None
    first = ds_list[0] if isinstance(ds_list[0], dict) else {}
    ds_id = first.get("id")
    return str(ds_id) if ds_id else None


def ensure_multiselect_option(
    client: Any,
    db_id: str,
    prop_name: str,
    label: str,
    *,
    similarity_threshold: float = 0.90,
) -> Dict[str, Any]:
    """
    Ensure a multi-select option exists on a database schema.
    Returns dict with status: added | exists | similar | invalid.
    """
    normalized = _normalize_label(label)
    if not normalized:
        return {"status": "invalid", "message": "label vide"}

    schema = get_database_schema(client, db_id)
    prop = schema.get(prop_name) if isinstance(schema, dict) else None
    if not isinstance(prop, dict) or prop.get("type") != "multi_select":
        raise ValueError(f"'{prop_name}' doit être une propriété multi_select.")

    options = (((prop.get("multi_select") or {}).get("options")) if isinstance(prop.get("multi_select"), dict) else []) or []
    existing_by_normalized: Dict[str, str] = {}
    for option in options:
        if not isinstance(option, dict):
            continue
        name = str(option.get("name", "")).strip()
        if not name:
            continue
        existing_by_normalized[_normalize_label(name)] = name

    if normalized in existing_by_normalized:
        return {"status": "exists", "existing": existing_by_normalized[normalized]}

    best_name = None
    best_score = 0.0
    for norm_name, name in existing_by_normalized.items():
        score = _similarity(normalized, norm_name)
        if score > best_score:
            best_score = score
            best_name = name
    if best_name and best_score >= similarity_threshold:
        return {"status": "similar", "existing": best_name, "score": round(best_score, 3)}

    updated_options = [{"name": value} for value in sorted(set(existing_by_normalized.values()))]
    updated_options.append({"name": label.strip()})
    updated_options = sorted(updated_options, key=lambda item: str(item.get("name", "")).lower())
    payload = {
        prop_name: {
            "multi_select": {
                "options": updated_options,
            }
        }
    }

    # Explicitly keep databases.update for compatibility with existing code expectations.
    client.databases.update(database_id=db_id, properties=payload)

    ds_id = _resolve_data_source_id(client, db_id)
    if ds_id and hasattr(client, "data_sources") and hasattr(client.data_sources, "update"):
        try:
            client.data_sources.update(data_source_id=ds_id, properties=payload)
        except Exception:
            pass

    return {"status": "added", "added": label.strip()}
