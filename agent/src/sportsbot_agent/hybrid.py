"""Advanced RAG retrieval: keyword (PostgreSQL) + vectors (Qdrant) -> RRF fusion -> reranker.

    question
      |-- PostgreSQL full-text search (accent-insensitive, OR of query terms) -> top N articles
      |-- BGE-M3 query vector -> Qdrant chunk search -> best chunk per article -> top N
      `-> Reciprocal Rank Fusion (k=60) -> top M candidates
            -> bge-reranker-v2-m3 cross-encoder -> keep score >= threshold -> top K

Each stage degrades gracefully: if the embedder or Qdrant is down, keyword results alone are
used; if the reranker is down, the fused order is kept. `last_trace` records what ran, so the
API can show it and evaluation can measure each stage.

`HybridRetriever.search(question, k, min_matched_terms)` returns `Hit`s like the BM25 prototype,
so `answer.answer_question` works with either retriever.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Protocol

from sportsbot_agent.retrieval import Hit, query_terms
from sportsbot_agent.store import Article

logger = logging.getLogger(__name__)


@dataclass
class Candidate:
    article_id: str
    title: str
    passage: str  # text shown to the reranker and the LLM
    source_name: str
    url: str
    published_at: str | None
    language: str | None
    ranks: dict[str, int] = field(default_factory=dict)  # stage -> 1-based rank
    rrf: float = 0.0
    rerank_score: float | None = None

    def to_hit(self) -> Hit:
        article = Article(
            article_id=self.article_id,
            title=self.title,
            summary=self.passage,
            source_name=self.source_name,
            url=self.url,
            published_at=self.published_at,
            collected_at="",
            language=self.language,
        )
        score = self.rerank_score if self.rerank_score is not None else self.rrf
        return Hit(article=article, score=score, matched_terms=len(self.ranks))


class KeywordSearch(Protocol):
    def search(self, terms: list[str], limit: int) -> list[Candidate]: ...


class VectorSearch(Protocol):
    def search(self, question: str, limit: int) -> list[Candidate]: ...


class Reranker(Protocol):
    def rerank(self, query: str, documents: list[str], top_n: int | None = None) -> list[Any]: ...


def rrf_fuse(result_lists: dict[str, list[Candidate]], k: int = 60) -> list[Candidate]:
    """Reciprocal Rank Fusion by article id; keeps the longest passage seen for an article."""
    fused: dict[str, Candidate] = {}
    for stage, results in result_lists.items():
        for rank, cand in enumerate(results, 1):
            current = fused.get(cand.article_id)
            if current is None:
                current = Candidate(**{**cand.__dict__, "ranks": {}, "rrf": 0.0})
                fused[cand.article_id] = current
            elif len(cand.passage) > len(current.passage):
                current.passage = cand.passage
            current.ranks[stage] = rank
            current.rrf += 1.0 / (k + rank)
    return sorted(fused.values(), key=lambda c: c.rrf, reverse=True)


@dataclass
class HybridRetriever:
    keyword: KeywordSearch | None
    vector: VectorSearch | None
    reranker: Reranker | None
    candidates_per_stage: int = 30
    rerank_top: int = 20
    min_rerank_score: float = -2.0
    last_trace: dict[str, Any] = field(default_factory=dict)

    def search(self, question: str, k: int = 5, min_matched_terms: int = 1) -> list[Hit]:
        trace: dict[str, Any] = {"stages": {}, "errors": {}}
        t0 = time.perf_counter()
        lists: dict[str, list[Candidate]] = {}
        terms = query_terms(question)
        if self.keyword is not None and terms:
            try:
                lists["keyword"] = self.keyword.search(terms, self.candidates_per_stage)
            except Exception as exc:  # degrade, do not fail the answer
                trace["errors"]["keyword"] = f"{type(exc).__name__}: {exc}"[:200]
        if self.vector is not None:
            try:
                lists["vector"] = self.vector.search(question, self.candidates_per_stage)
            except Exception as exc:
                trace["errors"]["vector"] = f"{type(exc).__name__}: {exc}"[:200]
        for stage, results in lists.items():
            trace["stages"][stage] = len(results)
        fused = rrf_fuse(lists)[: self.rerank_top]
        trace["stages"]["fused"] = len(fused)

        if fused and self.reranker is not None:
            try:
                docs = [f"{c.title}\n{c.passage}" for c in fused]
                scored = self.reranker.rerank(question, docs)
                for r in scored:
                    fused[r.index].rerank_score = r.score
                fused = sorted(
                    (c for c in fused if c.rerank_score is not None),
                    key=lambda c: c.rerank_score,
                    reverse=True,
                )
                fused = [c for c in fused if c.rerank_score >= self.min_rerank_score]
                trace["stages"]["reranked_kept"] = len(fused)
            except Exception as exc:
                trace["errors"]["rerank"] = f"{type(exc).__name__}: {exc}"[:200]
        elif fused:
            # Without a reranker, a keyword-only hit list must still match enough terms.
            fused = [c for c in fused if "vector" in c.ranks or len(terms) >= min_matched_terms]
        trace["ms"] = round((time.perf_counter() - t0) * 1000)
        self.last_trace = trace
        return [c.to_hit() for c in fused[:k]]


# ----------------------------------------------------------------- adapters -----


class PostgresKeywordSearch:
    """Full-text search on articles.search_tsv (migration 003)."""

    SQL = """
        SELECT article_id, title, left(content, 1500) AS passage, source_name, source_url,
               published_at, language, ts_rank_cd(search_tsv, q) AS rank
        FROM articles, to_tsquery('simple', :q) AS q
        WHERE search_tsv @@ q AND content IS NOT NULL
        ORDER BY rank DESC, published_at DESC NULLS LAST
        LIMIT :limit
    """

    def __init__(self, engine: Any) -> None:
        self.engine = engine

    def search(self, terms: list[str], limit: int) -> list[Candidate]:
        from sqlalchemy import text

        # Terms are already accent-stripped [0-9a-z] tokens (retrieval.tokenize): safe in tsquery.
        query = " | ".join(dict.fromkeys(terms))
        with self.engine.connect() as conn:
            rows = conn.execute(text(self.SQL), {"q": query, "limit": limit}).mappings().all()
        return [
            Candidate(
                article_id=r["article_id"],
                title=r["title"],
                passage=r["passage"] or "",
                source_name=r["source_name"],
                url=r["source_url"],
                published_at=r["published_at"].isoformat() if r["published_at"] else None,
                language=r["language"],
            )
            for r in rows
        ]


class QdrantVectorSearch:
    """BGE-M3 query vector -> Qdrant chunks -> best chunk per article."""

    def __init__(self, embedder: Any, qdrant: Any) -> None:
        self.embedder = embedder
        self.qdrant = qdrant

    def search(self, question: str, limit: int) -> list[Candidate]:
        vector = self.embedder.embed_one(question)
        points = self.qdrant.search(vector, limit=limit * 3)
        best: dict[str, Candidate] = {}
        for p in points:  # Qdrant returns best first
            pl = p.payload
            aid = pl.get("article_id")
            if not aid or aid in best:
                continue
            best[aid] = Candidate(
                article_id=aid,
                title=pl.get("title", ""),
                passage=pl.get("text", ""),
                source_name=pl.get("source_name", ""),
                url=pl.get("url", ""),
                published_at=pl.get("published_at"),
                language=pl.get("language"),
            )
            if len(best) >= limit:
                break
        return list(best.values())
