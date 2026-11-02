"""BGE-M3 embeddings through an OpenAI-compatible `/v1/embeddings` endpoint.

The stack serves BGE-M3 (GGUF, Q8_0) with llama.cpp `llama-server --embedding --pooling cls`
(see docker-compose.yml, service `embedder`). A native llama-server on the Mac (Metal)
serves the same API faster.
BGE-M3 needs no query/passage prefix; vectors have 1024 dimensions.
"""

from __future__ import annotations

import math

import httpx

BGE_M3_DIM = 1024


class EmbeddingError(Exception):
    pass


class EmbeddingClient:
    def __init__(
        self,
        base_url: str,
        model: str = "bge-m3",
        *,
        timeout: float = 60.0,
        batch_size: int = 16,
        max_chars: int = 6000,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.model = model
        self.batch_size = batch_size
        self.max_chars = max_chars  # BGE-M3 reads 8192 tokens; keep requests small and fast
        self._http = httpx.Client(
            base_url=base_url.rstrip("/"), timeout=timeout, transport=transport
        )

    def embed(self, texts: list[str]) -> list[list[float]]:
        vectors: list[list[float]] = []
        for start in range(0, len(texts), self.batch_size):
            batch = [t[: self.max_chars] for t in texts[start : start + self.batch_size]]
            try:
                resp = self._http.post("/v1/embeddings", json={"model": self.model, "input": batch})
            except httpx.HTTPError as exc:
                raise EmbeddingError(f"embedding server unreachable: {exc}") from exc
            if resp.status_code >= 400:
                raise EmbeddingError(f"embedding server HTTP {resp.status_code}: {resp.text[:200]}")
            data = sorted(resp.json()["data"], key=lambda d: d["index"])
            vectors.extend(_normalise(d["embedding"]) for d in data)
        return vectors

    def embed_one(self, text: str) -> list[float]:
        return self.embed([text])[0]

    def close(self) -> None:
        self._http.close()


def _normalise(vector: list[float]) -> list[float]:
    norm = math.sqrt(sum(x * x for x in vector)) or 1.0
    return [x / norm for x in vector]
