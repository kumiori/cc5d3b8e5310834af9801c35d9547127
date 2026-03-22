from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional


def get_admin_logger() -> Any:
    return logging.getLogger("affranchis.admin")


def log_admin_event(
    *,
    event_type: str,
    page: str,
    actor_player_id: str = "",
    session_id: str = "",
    item_id: str = "",
    value_label: str = "",
    metadata: Optional[Dict[str, Any]] = None,
    status: str = "ok",
) -> None:
    logger = get_admin_logger()
    payload = {
        "event_type": event_type,
        "page": page,
        "actor_player_id": actor_player_id,
        "session_id": session_id,
        "item_id": item_id,
        "value_label": value_label,
        "status": status,
        "metadata": metadata or {},
    }
    logger.info("admin.event %s", json.dumps(payload, ensure_ascii=False, default=str))
