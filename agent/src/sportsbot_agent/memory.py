"""Conversation memory: resolve follow-up questions against the earlier turns.

A follow-up such as "how old is he?" after "Who did Rooney play for?" cannot be retrieved on
its own: the pronoun carries no searchable term. Before retrieval the follow-up is rewritten
into a standalone search query (the "condense question" step), using the last few turns.

- The rewrite only builds the *search query*. The answer still comes only from retrieved
  articles; earlier answers are context for wording, never evidence.
- Without an LLM, or when the rewrite fails or looks wrong, the previous question is prepended
  to the new one, so the keyword search still sees the names mentioned earlier.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass

from sportsbot_agent.llm_client import LLMError, Message

MAX_TURNS = 4  # earlier question/answer pairs used for context
_ANSWER_CHARS = 400  # each earlier answer is cut to this many characters
_MAX_QUERY_CHARS = 300
_CITATION = re.compile(r"\s*\[\d+\]")


@dataclass(frozen=True)
class Turn:
    question: str
    answer: str


def _clean_answer(text: str) -> str:
    """Earlier answers without their [n] markers: those numbers refer to other articles."""
    flat = " ".join(_CITATION.sub("", text).split())
    return flat if len(flat) <= _ANSWER_CHARS else flat[:_ANSWER_CHARS].rstrip() + "..."


def recent(history: Sequence[Turn]) -> list[Turn]:
    return list(history)[-MAX_TURNS:]


def history_block(history: Sequence[Turn]) -> str:
    """Earlier turns as plain text for a prompt (oldest first)."""
    return "\n".join(
        f"User: {t.question}\nAssistant: {_clean_answer(t.answer)}" for t in recent(history)
    )


def fallback_query(question: str, history: Sequence[Turn]) -> str:
    turns = recent(history)
    return f"{turns[-1].question} {question}" if turns else question


_CONDENSE_PROMPT = (
    "You rewrite the user's latest message into one standalone search query for a sports "
    "news search engine.\n"
    "Rules:\n"
    "1. Replace pronouns and vague references (he, she, they, that team, anh ấy, ông ấy, "
    "cô ấy, họ, đội đó, trận đó...) with the names they refer to in the conversation.\n"
    "2. Keep the language of the latest message. Keep names as written.\n"
    "3. If the latest message is already standalone, or is about a new topic, return it "
    "unchanged.\n"
    "4. Output only the query: one line, no quotes, no explanation, no answer."
)


def condense_question(question: str, history: Sequence[Turn], llm: object | None) -> str:
    """A standalone search query for `question`, given the earlier turns."""
    turns = recent(history)
    if not turns:
        return question
    if llm is None:
        return fallback_query(question, turns)
    messages = [
        Message("system", _CONDENSE_PROMPT),
        Message(
            "user",
            f"Conversation:\n{history_block(turns)}\n\nLatest message: {question}\n\n"
            "Standalone search query:",
        ),
    ]
    try:
        text = llm.chat(messages)  # type: ignore[attr-defined]
    except LLMError:
        return fallback_query(question, turns)
    lines = [line.strip() for line in (text or "").splitlines() if line.strip()]
    query = lines[0].strip("\"'“”` ") if lines else ""
    if not query or len(query) > _MAX_QUERY_CHARS or not any(c.isalnum() for c in query):
        return fallback_query(question, turns)
    return query
