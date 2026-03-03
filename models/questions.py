from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional

QuestionType = Literal["single", "multi", "text"]
QuestionCategory = Literal["perception", "structure", "agency", "integration"]


@dataclass(frozen=True)
class Question:
    id: str
    category: QuestionCategory
    prompt: str
    qtype: QuestionType
    options: Optional[list[str]] = None
    max_select: Optional[int] = None
