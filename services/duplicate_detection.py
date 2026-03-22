from __future__ import annotations

import hashlib
import re
from typing import Any, Dict, List

from services.admin_logging import log_admin_event


def normalise_identity_token(value: str) -> str:
    compact = re.sub(r"\s+", " ", str(value or "").strip().lower())
    return compact


def duplicate_rule_text() -> str:
    return (
        "Règle: norm(x) = lower(trim(collapse_spaces(x))). "
        "Un groupe candidat est signalé si |G| >= 2 et au moins une condition est vraie: "
        "même pseudo normalisé, ou même email non vide normalisé."
    )


def detect_duplicate_candidates(players: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    nickname_groups: dict[str, list[dict[str, Any]]] = {}
    email_groups: dict[str, list[dict[str, Any]]] = {}
    by_id: dict[str, dict[str, Any]] = {}
    reasons_by_id: dict[str, set[str]] = {}
    keys_by_id: dict[str, set[str]] = {}

    for player in players:
        pid = str(player.get("id") or "").strip()
        if not pid:
            continue
        by_id[pid] = player

        nickname = normalise_identity_token(str(player.get("nickname") or ""))
        if nickname:
            nickname_groups.setdefault(nickname, []).append(player)
            reasons_by_id.setdefault(pid, set()).add("same_normalised_nickname")
            keys_by_id.setdefault(pid, set()).add(f"nickname:{nickname}")

        email = normalise_identity_token(str(player.get("email") or ""))
        if email:
            email_groups.setdefault(email, []).append(player)
            reasons_by_id.setdefault(pid, set()).add("same_normalised_email")
            keys_by_id.setdefault(pid, set()).add(f"email:{email}")

    candidate_sets: list[tuple[set[str], list[str], list[str]]] = []
    for token, group in nickname_groups.items():
        if len(group) >= 2:
            candidate_sets.append(
                (
                    {str(p.get("id")) for p in group if p.get("id")},
                    ["same_normalised_nickname"],
                    [f"nickname:{token}"],
                )
            )
    for token, group in email_groups.items():
        if len(group) >= 2:
            candidate_sets.append(
                (
                    {str(p.get("id")) for p in group if p.get("id")},
                    ["same_normalised_email"],
                    [f"email:{token}"],
                )
            )

    merged: list[dict[str, Any]] = []
    for ids, reasons, match_keys in candidate_sets:
        if len(ids) < 2:
            continue
        ordered_ids = sorted(ids)
        candidate_key = hashlib.sha1("|".join(ordered_ids).encode("utf-8")).hexdigest()[:12]
        merged.append(
            {
                "candidate_key": candidate_key,
                "player_ids": ordered_ids,
                "players": [by_id[pid] for pid in ordered_ids if pid in by_id],
                "reasons": reasons,
                "match_keys": match_keys,
            }
        )

    # De-duplicate groups that may emerge from both nickname and email paths.
    dedup: dict[str, dict[str, Any]] = {}
    for item in merged:
        key = "|".join(item["player_ids"])
        if key not in dedup:
            dedup[key] = item
            continue
        dedup[key]["reasons"] = sorted(
            set(dedup[key].get("reasons", [])) | set(item.get("reasons", []))
        )
        dedup[key]["match_keys"] = sorted(
            set(dedup[key].get("match_keys", [])) | set(item.get("match_keys", []))
        )
    return list(dedup.values())


def build_duplicate_activity_snapshot(players: List[Dict[str, Any]]) -> Dict[str, Any]:
    candidates = detect_duplicate_candidates(players)
    return {
        "players_scanned": len(players),
        "candidate_groups": len(candidates),
        "candidate_player_total": sum(len(group.get("player_ids", [])) for group in candidates),
    }


def mark_candidate_unrelated(candidate_key: str) -> None:
    log_admin_event(
        event_type="duplicate_mark_unrelated",
        page="07_Admin",
        item_id=candidate_key,
    )


def log_duplicate_merge_invite(
    *,
    actor_player_id: str,
    session_id: str,
    candidate_ids: List[str],
    reasons: List[str],
    match_keys: List[str],
) -> None:
    log_admin_event(
        event_type="duplicate_merge_invite",
        page="07_Admin",
        actor_player_id=actor_player_id,
        session_id=session_id,
        metadata={
            "candidate_ids": candidate_ids,
            "reasons": reasons,
            "match_keys": match_keys,
        },
    )
