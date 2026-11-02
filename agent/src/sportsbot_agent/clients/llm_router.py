"""Chat LLM router: several free Gemini models, tried in turn (ADR 0006).

Why: the Gemini API free tier limits requests per minute and per day *per model* (per project),
and the limits change without notice. When one model answers HTTP 429 (quota) the router puts it
on cooldown and tries the next one, so the chatbot keeps answering. Calls use Gemini's
OpenAI-compatible Chat Completions endpoint (`.../v1beta/openai`), so a target is just
(base URL, model, key). If every model fails, the caller shows article excerpts instead.

Cooldowns: a per-minute limit (429 without "day" in the message) rests the model for
`cooldown_seconds`; a daily limit rests it until the next midnight Pacific time, when Google
resets daily quotas. A model the server does not know (404) is skipped for the session.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import httpx

from sportsbot_agent.llm_client import LLMError, LLMUnavailable, Message

logger = logging.getLogger(__name__)

PACIFIC = ZoneInfo("America/Los_Angeles")


@dataclass(frozen=True)
class ChatTarget:
    name: str  # shown in logs and API responses, e.g. "gemini:gemini-3.8-flash"
    base_url: str
    model: str
    api_key: str = ""


@dataclass
class TargetState:
    cooled_until: float = 0.0  # monotonic seconds
    disabled: bool = False
    failures: int = 0
    successes: int = 0


def seconds_until_pacific_midnight(now: datetime | None = None) -> float:
    now = (now or datetime.now(PACIFIC)).astimezone(PACIFIC)
    tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=5, microsecond=0)
    return (tomorrow - now).total_seconds()


@dataclass
class ChatRouter:
    targets: list[ChatTarget]
    temperature: float = 0.1
    max_tokens: int = 600
    timeout: float = 60.0
    cooldown_seconds: float = 65.0
    # Gemini 3.x models "think" before answering and the thinking uses the max_tokens budget:
    # with a small budget the reply can come back empty. "low" keeps answers fast and complete;
    # "" sends nothing (server default). Dropped automatically if a model rejects it.
    reasoning_effort: str = "low"
    transport: httpx.BaseTransport | None = None
    clock: Callable[[], float] = time.monotonic
    state: dict[str, TargetState] = field(default_factory=dict)
    last_target: str | None = None

    def __post_init__(self) -> None:
        for t in self.targets:
            self.state.setdefault(t.name, TargetState())

    def available(self) -> list[ChatTarget]:
        now = self.clock()
        return [
            t
            for t in self.targets
            if not self.state[t.name].disabled and self.state[t.name].cooled_until <= now
        ]

    def status(self) -> list[dict[str, object]]:
        now = self.clock()
        return [
            {
                "target": t.name,
                "available": not self.state[t.name].disabled
                and self.state[t.name].cooled_until <= now,
                "cooldown_seconds": max(0, round(self.state[t.name].cooled_until - now)),
                "disabled": self.state[t.name].disabled,
                "successes": self.state[t.name].successes,
                "failures": self.state[t.name].failures,
            }
            for t in self.targets
        ]

    def _call(
        self, target: ChatTarget, messages: list[Message], *, with_reasoning: bool = True
    ) -> httpx.Response:
        headers = {"Content-Type": "application/json"}
        if target.api_key:
            headers["Authorization"] = f"Bearer {target.api_key}"
        payload = {
            "model": target.model,
            "messages": [{"role": m.role, "content": m.content} for m in messages],
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
            "stream": False,
        }
        if with_reasoning and self.reasoning_effort:
            payload["reasoning_effort"] = self.reasoning_effort
        with httpx.Client(timeout=self.timeout, transport=self.transport) as client:
            return client.post(
                f"{target.base_url.rstrip('/')}/chat/completions", json=payload, headers=headers
            )

    def chat(self, messages: list[Message]) -> str:
        errors: list[str] = []
        for target in self.available():
            st = self.state[target.name]
            try:
                resp = self._call(target, messages)
                if resp.status_code == 400 and "reasoning" in resp.text.lower():
                    resp = self._call(target, messages, with_reasoning=False)
            except httpx.HTTPError as exc:
                st.failures += 1
                st.cooled_until = self.clock() + 30
                errors.append(f"{target.name}: {type(exc).__name__}")
                continue
            if resp.status_code == 429:
                daily = "day" in resp.text.lower()
                wait = seconds_until_pacific_midnight() if daily else self.cooldown_seconds
                st.failures += 1
                st.cooled_until = self.clock() + wait
                logger.warning(
                    "llm_quota", extra={"target": target.name, "daily": daily, "cooldown": wait}
                )
                errors.append(f"{target.name}: quota")
                continue
            if resp.status_code == 404:
                st.disabled = True
                errors.append(f"{target.name}: model not found")
                continue
            if resp.status_code in (401, 403):
                st.disabled = True
                errors.append(f"{target.name}: HTTP {resp.status_code} (check the API key)")
                continue
            if resp.status_code >= 400:
                st.failures += 1
                st.cooled_until = self.clock() + 30
                errors.append(f"{target.name}: HTTP {resp.status_code}")
                continue
            try:
                choice = resp.json()["choices"][0]
                text = (choice.get("message") or {}).get("content") or ""
            except (ValueError, KeyError, IndexError, TypeError, AttributeError):
                st.failures += 1
                errors.append(f"{target.name}: bad response")
                continue
            if not text.strip():
                # e.g. all tokens spent on thinking (finish_reason "length"): try the next model
                st.failures += 1
                errors.append(f"{target.name}: empty reply ({choice.get('finish_reason')})")
                continue
            st.successes += 1
            self.last_target = target.name
            return text.strip()
        if not errors:
            raise LLMUnavailable("All LLM targets are cooling down; try again in a minute.")
        raise LLMError("No LLM target answered: " + "; ".join(errors))


def build_targets(
    gemini_api_key: str, gemini_base_url: str, gemini_models: list[str]
) -> list[ChatTarget]:
    if not gemini_api_key:
        return []
    return [
        ChatTarget(f"gemini:{m}", gemini_base_url, m, gemini_api_key) for m in gemini_models if m
    ]
