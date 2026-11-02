"""Minimal client for an OpenAI-compatible chat completions API (Ollama by default).

Errors are turned into three clear exceptions so the caller can tell the user what to do:
the server is not running, the model is not downloaded, or the request timed out.
"""

from __future__ import annotations

from dataclasses import dataclass

import httpx

from sportsbot_agent.config import AgentConfig


class LLMError(Exception):
    """Base class: the LLM could not produce an answer."""


class LLMUnavailable(LLMError):
    pass


class LLMModelNotFound(LLMError):
    pass


class LLMTimeout(LLMError):
    pass


@dataclass(frozen=True)
class Message:
    role: str  # "system" | "user" | "assistant"
    content: str


class LLMClient:
    def __init__(self, config: AgentConfig, transport: httpx.BaseTransport | None = None) -> None:
        self.config = config
        headers = {"Content-Type": "application/json"}
        if config.llm_api_key:
            headers["Authorization"] = f"Bearer {config.llm_api_key}"
        self._http = httpx.Client(
            base_url=config.llm_base_url,
            timeout=config.llm_timeout_seconds,
            headers=headers,
            transport=transport,
        )

    def chat(self, messages: list[Message]) -> str:
        payload = {
            "model": self.config.llm_model,
            "messages": [{"role": m.role, "content": m.content} for m in messages],
            "temperature": self.config.llm_temperature,
            "max_tokens": self.config.llm_max_tokens,
            "stream": False,
        }
        try:
            resp = self._http.post("/chat/completions", json=payload)
        except httpx.TimeoutException as exc:
            raise LLMTimeout(
                f"The model did not answer within {self.config.llm_timeout_seconds:g}s."
            ) from exc
        except httpx.TransportError as exc:
            raise LLMUnavailable(
                f"Cannot reach the LLM server at {self.config.llm_base_url}. "
                "Is Ollama running? Start the Ollama app or run `ollama serve`."
            ) from exc

        if resp.status_code == 404 and "model" in resp.text.lower():
            raise LLMModelNotFound(
                f"Model '{self.config.llm_model}' is not available on the server. "
                f"Download it with `ollama pull {self.config.llm_model}` or set LLM_MODEL."
            )
        if resp.status_code >= 400:
            raise LLMError(f"LLM server returned HTTP {resp.status_code}: {resp.text[:200]}")
        try:
            return resp.json()["choices"][0]["message"]["content"].strip()
        except (ValueError, KeyError, IndexError, TypeError) as exc:
            raise LLMError("Unexpected response format from the LLM server.") from exc

    def close(self) -> None:
        self._http.close()
