"""Agent configuration from environment variables (see `.env.example`).

The chat models are free Gemini models reached through Google's OpenAI-compatible API
(ADR 0006); the list is tried in order and a model that hits its quota rests.
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


def _list(env: Mapping[str, str], key: str, default: tuple[str, ...]) -> tuple[str, ...]:
    raw = env.get(key, "").strip()
    return tuple(x.strip() for x in raw.split(",") if x.strip()) if raw else default


@dataclass(frozen=True)
class AgentConfig:
    llm_timeout_seconds: float = 60.0
    llm_max_tokens: int = 600
    llm_temperature: float = 0.1
    articles_dir: Path = REPO_ROOT / "data" / "articles"
    top_k: int = 5
    min_matched_terms: int = 1
    max_question_chars: int = 500

    # Free Gemini models tried in order (ADR 0006).
    gemini_api_key: str = ""
    gemini_base_url: str = "https://generativelanguage.googleapis.com/v1beta/openai"
    gemini_models: tuple[str, ...] = (
        "gemini-3.8-flash",
        "gemini-3.6-flash",
        "gemini-3.5-flash-lite",
        "gemini-3.1-flash-lite",
    )
    llm_cooldown_seconds: float = 65.0
    llm_reasoning_effort: str = "low"

    # Advanced RAG (hybrid.py): PostgreSQL keyword + Qdrant vectors + reranker.
    database_url: str = (
        "postgresql+psycopg://sportsbot:change-me-local-only@localhost:5432/sportsbot"
    )
    qdrant_url: str = "http://localhost:6333"
    embedding_base_url: str = "http://localhost:8091"
    embedding_model: str = "bge-m3"
    rerank_base_url: str = "http://localhost:8092"
    rerank_model: str = "bge-reranker-v2-m3"
    rerank_min_score: float = -2.0
    retrieval_candidates: int = 30

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> AgentConfig:
        env = os.environ if env is None else env
        d = cls()
        return cls(
            llm_timeout_seconds=_float(env, "LLM_TIMEOUT_SECONDS", d.llm_timeout_seconds),
            llm_max_tokens=_int(env, "LLM_MAX_TOKENS", d.llm_max_tokens),
            llm_temperature=_float(env, "LLM_TEMPERATURE", d.llm_temperature),
            articles_dir=Path(env.get("ARTICLES_DIR") or d.articles_dir),
            top_k=_int(env, "RETRIEVAL_TOP_K", d.top_k),
            min_matched_terms=_int(env, "RETRIEVAL_MIN_MATCHED_TERMS", d.min_matched_terms),
            # strip(): a .env saved with Windows line endings must not break the key.
            gemini_api_key=env.get("GEMINI_API_KEY", "").strip(),
            gemini_base_url=(env.get("GEMINI_BASE_URL") or d.gemini_base_url).rstrip("/"),
            gemini_models=_list(env, "GEMINI_MODELS", d.gemini_models),
            llm_cooldown_seconds=_float(env, "LLM_COOLDOWN_SECONDS", d.llm_cooldown_seconds),
            llm_reasoning_effort=env.get("LLM_REASONING_EFFORT", d.llm_reasoning_effort).strip(),
            database_url=env.get("DATABASE_URL") or d.database_url,
            qdrant_url=env.get("QDRANT_URL") or d.qdrant_url,
            embedding_base_url=env.get("EMBEDDING_BASE_URL") or d.embedding_base_url,
            embedding_model=env.get("EMBEDDING_MODEL") or d.embedding_model,
            rerank_base_url=env.get("RERANK_BASE_URL", d.rerank_base_url),
            rerank_model=env.get("RERANK_MODEL") or d.rerank_model,
            rerank_min_score=_float(env, "RERANK_MIN_SCORE", d.rerank_min_score),
            retrieval_candidates=_int(env, "RETRIEVAL_CANDIDATES", d.retrieval_candidates),
        )
