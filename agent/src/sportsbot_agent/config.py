"""Agent configuration from environment variables (see `.env.example`).

The LLM is reached through an OpenAI-compatible API (AGENTS.md). Ollama serves one at
`http://localhost:11434/v1` on macOS, Windows and Linux, so the same code runs everywhere and a
different model or server is a configuration change only.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]


def _float(env: Mapping[str, str], key: str, default: float) -> float:
    raw = env.get(key, "").strip()
    return float(raw) if raw else default


def _int(env: Mapping[str, str], key: str, default: int) -> int:
    raw = env.get(key, "").strip()
    return int(raw) if raw else default


@dataclass(frozen=True)
class AgentConfig:
    llm_base_url: str = "http://localhost:11434/v1"
    llm_model: str = "llama3:latest"
    llm_api_key: str = ""  # Ollama needs none; kept for other OpenAI-compatible servers
    llm_timeout_seconds: float = 120.0
    llm_max_tokens: int = 400
    llm_temperature: float = 0.1
    articles_dir: Path = REPO_ROOT / "data" / "articles"
    top_k: int = 5
    min_matched_terms: int = 1
    max_question_chars: int = 500

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> AgentConfig:
        env = os.environ if env is None else env
        d = cls()
        return cls(
            llm_base_url=(env.get("LLM_BASE_URL") or d.llm_base_url).rstrip("/"),
            llm_model=env.get("LLM_MODEL") or d.llm_model,
            llm_api_key=env.get("LLM_API_KEY", ""),
            llm_timeout_seconds=_float(env, "LLM_TIMEOUT_SECONDS", d.llm_timeout_seconds),
            llm_max_tokens=_int(env, "LLM_MAX_TOKENS", d.llm_max_tokens),
            llm_temperature=_float(env, "LLM_TEMPERATURE", d.llm_temperature),
            articles_dir=Path(env.get("ARTICLES_DIR") or d.articles_dir),
            top_k=_int(env, "RETRIEVAL_TOP_K", d.top_k),
            min_matched_terms=_int(env, "RETRIEVAL_MIN_MATCHED_TERMS", d.min_matched_terms),
        )
