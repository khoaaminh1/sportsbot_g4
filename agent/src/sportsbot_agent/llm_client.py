"""Chat message type and LLM errors shared by the router and the answer code.

The chat models are reached through `clients/llm_router.py` (Gemini, OpenAI-compatible API).
"""

from __future__ import annotations

from dataclasses import dataclass


class LLMError(Exception):
    """Base class: the LLM could not produce an answer."""


class LLMUnavailable(LLMError):
    """No model can be tried right now (all are resting after quota errors)."""


@dataclass(frozen=True)
class Message:
    role: str  # "system" | "user" | "assistant"
    content: str
