from __future__ import annotations

import hashlib
import random
from collections import defaultdict

from models.questions import Question


def question_count_for_depth(depth: int) -> int:
    if depth <= 3:
        return 4
    if depth <= 7:
        return 6
    return 8


def _category_shape(depth: int) -> dict[str, int]:
    if depth <= 3:
        return {"perception": 1, "structure": 1, "agency": 1, "integration": 1}
    if depth <= 7:
        return {"perception": 2, "structure": 2, "agency": 1, "integration": 1}
    return {"perception": 2, "structure": 2, "agency": 2, "integration": 2}


def _make_rng(seed_key: str) -> random.Random:
    digest = hashlib.sha256(seed_key.encode("utf-8")).hexdigest()
    return random.Random(int(digest[:16], 16))


def select_questions(depth: int, seed_key: str, catalog: list[Question]) -> list[Question]:
    rng = _make_rng(seed_key)
    grouped: dict[str, list[Question]] = defaultdict(list)
    for q in catalog:
        grouped[q.category].append(q)

    shape = _category_shape(depth)
    selected: list[Question] = []
    for category, count in shape.items():
        pool = list(grouped.get(category, []))
        if not pool:
            continue
        rng.shuffle(pool)
        selected.extend(pool[: min(count, len(pool))])

    selected_by_id = {q.id: q for q in selected}
    if depth >= 4:
        required_ids = ["EPISTEMIC_FRAME", "ONE_WORD_TRACE"]
        for required_id in required_ids:
            if required_id in selected_by_id:
                continue
            required_q = next((q for q in catalog if q.id == required_id), None)
            if required_q is None:
                continue
            replace_idx = None
            for idx, existing in enumerate(selected):
                if existing.category != required_q.category:
                    continue
                if existing.id in required_ids:
                    continue
                replace_idx = idx
                break
            if replace_idx is not None:
                selected[replace_idx] = required_q
            else:
                selected.append(required_q)
            selected_by_id[required_q.id] = required_q

    order = {q.id: i for i, q in enumerate(catalog)}
    selected = list({q.id: q for q in selected}.values())
    selected.sort(key=lambda q: order.get(q.id, 9999))
    return selected
