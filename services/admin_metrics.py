from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List


def _parse_iso(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def compute_players_metrics(
    players: List[Dict[str, Any]],
    contact_preferences: List[Dict[str, Any]],
) -> Dict[str, int]:
    total = len(players)
    by_player: dict[str, str] = {}
    for pref in contact_preferences:
        pid = str(pref.get("player_id") or "").strip()
        if not pid:
            continue
        by_player[pid] = str(pref.get("value_label") or pref.get("value") or "").strip()

    with_contact = 0
    no_contact = 0
    for value in by_player.values():
        lower = value.lower()
        if not lower:
            continue
        if "no" in lower and "contact" in lower:
            no_contact += 1
        else:
            with_contact += 1
    return {
        "total_players": total,
        "with_contact_preference": with_contact,
        "no_contact_requested": no_contact,
    }


def compute_activity_metrics(players: List[Dict[str, Any]]) -> Dict[str, int]:
    now = datetime.now(timezone.utc)
    threshold = now - timedelta(hours=12)
    active = 0
    for player in players:
        seen = _parse_iso(player.get("last_activity"))
        if seen and seen >= threshold:
            active += 1
    return {"active_12h": active}


def compute_contact_metrics(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    with_contact = 0
    no_contact = 0
    unset = 0
    for row in rows:
        pref = str(row.get("contact_preference") or "").strip().lower()
        if not pref:
            unset += 1
        elif "no" in pref and "contact" in pref:
            no_contact += 1
        else:
            with_contact += 1
    return {
        "with_contact_preference": with_contact,
        "no_contact_requested": no_contact,
        "contact_not_set": unset,
    }
