"""bge-reranker-v2-m3 (GGUF) through llama.cpp `llama-server --reranking` (`/v1/rerank`).

A cross-encoder reads the question and each candidate passage together, so it orders candidates
much better than vector or keyword scores alone. It is the last retrieval step before the LLM.
"""

from __future__ import annotations

from dataclasses import dataclass

import httpx

from sportsbot_agent.clients.embeddings import flatten


class RerankError(Exception):
    pass


@dataclass(frozen=True)
class RerankResult:
    index: int  # position in the documents list that was sent
    score: float  # model logit: higher = more relevant; compare within one request only


class RerankClient:
    def __init__(
        self,
        base_url: str,
        model: str = "bge-reranker-v2-m3",
        *,
        timeout: float = 60.0,
        max_chars: int = 2000,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.model = model
        self.max_chars = max_chars
        self._http = httpx.Client(
            base_url=base_url.rstrip("/"), timeout=timeout, transport=transport
        )

    def rerank(
        self, query: str, documents: list[str], top_n: int | None = None
    ) -> list[RerankResult]:
        if not documents:
            return []
        payload = {
            "model": self.model,
            "query": flatten(query),
            "documents": [flatten(d)[: self.max_chars] for d in documents],
            "top_n": top_n or len(documents),
        }
        try:
            resp = self._http.post("/v1/rerank", json=payload)
        except httpx.HTTPError as exc:
            raise RerankError(f"reranker unreachable: {exc}") from exc
        if resp.status_code >= 400:
            raise RerankError(f"reranker HTTP {resp.status_code}: {resp.text[:200]}")
        results = [
            RerankResult(int(r["index"]), float(r["relevance_score"]))
            for r in resp.json()["results"]
        ]
        return sorted(results, key=lambda r: r.score, reverse=True)

    def close(self) -> None:
        self._http.close()
