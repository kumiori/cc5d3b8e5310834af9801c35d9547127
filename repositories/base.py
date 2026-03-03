from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class InteractionRepository(ABC):
    @abstractmethod
    def save_response(
        self,
        session_id: str,
        player_id: Optional[str],
        question_id: str,
        value: Any,
        text_id: str,
        device_id: str,
    ) -> None:
        pass

    @abstractmethod
    def get_responses(self, session_id: str) -> List[Dict[str, Any]]:
        pass
