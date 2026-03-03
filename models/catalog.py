from __future__ import annotations

from models.questions import Question

QUESTION_CATALOG: list[Question] = [
    Question(
        id="PERCEPTION_ENTRY",
        category="perception",
        prompt="When you hear the word 'glacier', what comes first?",
        qtype="single",
        options=[
            "Landscape",
            "Sound",
            "Scientific object",
            "Symbol",
            "Warning",
            "Other",
        ],
    ),
    Question(
        id="PROXIMITY",
        category="perception",
        prompt="Do glaciers feel distant or present in your life?",
        qtype="single",
        options=[
            "Distant abstraction",
            "Global but real",
            "Personally connected",
            "Structurally linked to my future",
        ],
    ),
    Question(
        id="EPISTEMIC_FRAME",
        category="structure",
        prompt="Which description feels closer to reality?",
        qtype="single",
        options=[
            "Gradual slope",
            "Threshold or tipping point",
        ],
    ),
    Question(
        id="IRREVERSIBILITY_AFFECT",
        category="structure",
        prompt="Irreversibility makes me feel...",
        qtype="multi",
        options=[
            "Fear",
            "Responsibility",
            "Motivation",
            "Powerlessness",
            "Clarity",
            "Curiosity",
        ],
        max_select=2,
    ),
    Question(
        id="PRIORITY_AFTER_NO_RETURN",
        category="agency",
        prompt="When reversal is no longer possible, what becomes most important?",
        qtype="single",
        options=[
            "Speed",
            "Fairness",
            "Coordination",
            "Innovation",
            "Acceptance",
        ],
    ),
    Question(
        id="WHAT_ENABLES_ACTION",
        category="agency",
        prompt="What helps societies act under uncertainty?",
        qtype="multi",
        options=[
            "Data",
            "Narratives",
            "Art",
            "Shared rituals",
            "Institutional rules",
            "Community belonging",
        ],
        max_select=2,
    ),
    Question(
        id="SHIFT_AFTER_PANEL",
        category="integration",
        prompt="After this session, glaciers feel more like...",
        qtype="single",
        options=[
            "Object of study",
            "Political issue",
            "Living system",
            "Mirror of society",
            "Call to responsibility",
        ],
    ),
    Question(
        id="ONE_WORD_TRACE",
        category="integration",
        prompt="One word that remains with you.",
        qtype="text",
    ),
]

QUESTION_BY_ID = {q.id: q for q in QUESTION_CATALOG}
