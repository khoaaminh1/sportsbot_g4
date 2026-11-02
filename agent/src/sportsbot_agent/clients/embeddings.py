"""BGE-M3 embeddings through an OpenAI-compatible `/v1/embeddings` endpoint.

The stack serves BGE-M3 (GGUF, Q8_0) with llama.cpp `llama-server --embedding --pooling cls`
(see docker-compose.yml, service `embedder`). A native llama-server on the Mac (Metal)
serves the same API faster.
BGE-M3 needs no query/passage prefix; vectors have 1024 dimensions.
"""

from __future__ import annotations

import math
import unicodedata

import httpx

BGE_M3_DIM = 1024


def flatten(text: str) -> str:
    """One line, single spaces. Some GGUF conversions of BGE-M3 / bge-reranker have no newline
    token, and llama.cpp then fails on any input with a line break (HTTP 500
    "unordered_map::at"). Line breaks carry no meaning for these models, so drop them."""
    return " ".join(text.split())


_DROP_CATEGORIES = {"So", "Sk", "Cs", "Co", "Cn", "Cc", "Cf"}


def safe_text(text: str) -> str:
    """Fallback for inputs the tokenizer cannot handle: keep letters, digits, punctuation and
    spaces; drop emoji, symbols and control/format characters (NFC first, so Vietnamese
    diacritics stay)."""
    text = unicodedata.normalize("NFC", text)
    kept = (c if unicodedata.category(c) not in _DROP_CATEGORIES else " " for c in text)
    return flatten("".join(kept))


class EmbeddingError(Exception):
    """The embedding server is unreachable or failing for every input (retry later)."""


class InputRejected(EmbeddingError):
    """The server rejected one specific input (HTTP 4xx/500 on that text alone)."""


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
        self.skipped = 0  # inputs that could not be embedded even after cleaning
        self._http = httpx.Client(
            base_url=base_url.rstrip("/"), timeout=timeout, transport=transport
        )

    def _post(self, batch: list[str]) -> list[list[float]]:
        try:
            resp = self._http.post("/v1/embeddings", json={"model": self.model, "input": batch})
        except httpx.HTTPError as exc:
            raise EmbeddingError(f"embedding server unreachable: {exc}") from exc
        if resp.status_code >= 500 and "unordered_map" not in resp.text:
            raise EmbeddingError(f"embedding server HTTP {resp.status_code}: {resp.text[:200]}")
        if resp.status_code >= 400:
            raise InputRejected(f"embedding server HTTP {resp.status_code}: {resp.text[:200]}")
        data = sorted(resp.json()["data"], key=lambda d: d["index"])
        return [_normalise(d["embedding"]) for d in data]

    def _one(self, text: str) -> list[float] | None:
        """Embed one input; if the server rejects it, retry once with `safe_text`."""
        try:
            return self._post([text])[0]
        except InputRejected:
            cleaned = safe_text(text)
            if cleaned and cleaned != text:
                try:
                    return self._post([cleaned])[0]
                except InputRejected:
                    pass
        self.skipped += 1
        return None

    def embed(self, texts: list[str]) -> list[list[float] | None]:
        """One vector per input, in order. None for an input the server cannot embed (a bad
        character for this GGUF's tokenizer); the rest still succeed. Raises EmbeddingError
        only when the server itself is down."""
        out: list[list[float] | None] = []
        for start in range(0, len(texts), self.batch_size):
            batch = [flatten(t)[: self.max_chars] for t in texts[start : start + self.batch_size]]
            try:
                out.extend(self._post(batch))
            except InputRejected:
                # One bad input fails the whole batch: find it by sending them one by one.
                out.extend(self._one(t) for t in batch)
        return out

    def embed_one(self, text: str) -> list[float]:
        vector = self.embed([text])[0]
        if vector is None:
            raise EmbeddingError("the embedding server could not embed this text")
        return vector

    def close(self) -> None:
        self._http.close()


def _normalise(vector: list[float]) -> list[float]:
    norm = math.sqrt(sum(x * x for x in vector)) or 1.0
    return [x / norm for x in vector]
