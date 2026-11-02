"""Wire the production RAG: hybrid retriever + LLM router, from `AgentConfig`.

Used by the FastAPI backend (`/api/v1/chat`) and the terminal (`make ask`).
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from sportsbot_agent.answer import Answer, answer_question
from sportsbot_agent.clients.embeddings import EmbeddingClient
from sportsbot_agent.clients.llm_router import ChatRouter, build_targets
from sportsbot_agent.clients.qdrant import QdrantClient
from sportsbot_agent.clients.rerank import RerankClient
from sportsbot_agent.config import AgentConfig
from sportsbot_agent.hybrid import HybridRetriever, PostgresKeywordSearch, QdrantVectorSearch


@dataclass
class RagService:
    retriever: HybridRetriever
    router: ChatRouter | None
    top_k: int = 5
    closers: list[Any] = field(default_factory=list)

    def ask(self, question: str) -> tuple[Answer, dict[str, Any]]:
        t0 = time.perf_counter()
        answer = answer_question(
            question, self.retriever, self.router, top_k=self.top_k, now=datetime.now(UTC)
        )
        meta = {
            "retrieval": self.retriever.last_trace,
            "llm": getattr(self.router, "last_target", None)
            if answer.mode == "generated"
            else None,
            "seconds": round(time.perf_counter() - t0, 2),
        }
        return answer, meta

    def close(self) -> None:
        for c in self.closers:
            c.close()


def build_rag(
    config: AgentConfig, engine: Any | None = None, *, use_llm: bool = True
) -> RagService:
    if engine is None:
        from sqlalchemy import create_engine

        engine = create_engine(config.database_url, pool_pre_ping=True)
    embedder = EmbeddingClient(config.embedding_base_url, config.embedding_model, timeout=30)
    qdrant = QdrantClient(config.qdrant_url)
    reranker = (
        RerankClient(config.rerank_base_url, config.rerank_model, timeout=60)
        if config.rerank_base_url
        else None
    )
    retriever = HybridRetriever(
        keyword=PostgresKeywordSearch(engine),
        vector=QdrantVectorSearch(embedder, qdrant),
        reranker=reranker,
        candidates_per_stage=config.retrieval_candidates,
        min_rerank_score=config.rerank_min_score,
    )
    router = None
    if use_llm:
        targets = build_targets(
            config.gemini_api_key,
            config.gemini_base_url,
            list(config.gemini_models),
            config.llm_base_url,
            config.llm_model,
            config.llm_use_ollama_fallback,
        )
        if targets:
            router = ChatRouter(
                targets,
                temperature=config.llm_temperature,
                max_tokens=config.llm_max_tokens,
                timeout=config.llm_timeout_seconds,
                cooldown_seconds=config.llm_cooldown_seconds,
            )
    closers = [embedder, qdrant] + ([reranker] if reranker else [])
    return RagService(retriever, router, top_k=config.top_k, closers=closers)


def answer_to_dict(answer: Answer, meta: dict[str, Any]) -> dict[str, Any]:
    """JSON shape of a chat answer (API and logs)."""
    return {
        "mode": answer.mode,
        "language": answer.language,
        "answer": answer.text,
        "notice": answer.notice,
        "citations": [
            {
                "number": n,
                "article_id": h.article.article_id,
                "title": h.article.title,
                "source_name": h.article.source_name,
                "url": h.article.url,
                "published_at": h.article.published_at,
                "score": round(h.score, 4),
            }
            for n, h in answer.sources
        ],
        "meta": meta,
    }
