"""Protection for a public URL: access code, per-client rate limit, concurrency caps.

Every chat question spends the team's free Gemini quota and 20-50 s of the crawler PC's CPU, so a
public link (Cloudflare Tunnel, docs/runbooks/public-access.md) needs limits:

- `CHAT_ACCESS_CODE`: when set, /chat and /chat/stream need the header `X-Access-Code`
  (compared in constant time). Empty: no code (local use).
- `CHAT_RATE_LIMIT` questions per `CHAT_RATE_WINDOW_SECONDS` per client (0 disables).
- `CHAT_MAX_CONCURRENT` questions answered at the same time for everyone (0 disables).
- `WS_MAX_CONNECTIONS` open /ws/news connections (0 disables).

The client is the `CF-Connecting-IP` header (set by Cloudflare's edge, which overwrites any value
the visitor sends) or else the TCP peer address. State is in memory: one backend process.
"""

from __future__ import annotations

import hmac
import math
import time
from collections import deque
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager

from app.core.errors import AppError

ACCESS_HEADER = "x-access-code"


def client_key(headers: Mapping[str, str], peer: str | None) -> str:
    """Who is asking: Cloudflare's visitor address when behind the tunnel, else the peer."""
    forwarded = (headers.get("cf-connecting-ip") or "").strip()
    return forwarded or peer or "unknown"


class ChatGuard:
    def __init__(
        self,
        *,
        access_code: str = "",
        rate_limit: int = 0,
        window_seconds: float = 600.0,
        max_concurrent: int = 0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.access_code = access_code
        self.rate_limit = rate_limit
        self.window_seconds = window_seconds
        self.max_concurrent = max_concurrent
        self.clock = clock
        self.active = 0
        self._hits: dict[str, deque[float]] = {}

    @property
    def code_required(self) -> bool:
        return bool(self.access_code)

    def check_code(self, supplied: str | None) -> None:
        if not self.access_code:
            return
        if not supplied or not hmac.compare_digest(
            supplied.strip().encode(), self.access_code.encode()
        ):
            raise AppError(
                "access_code_required", "A valid access code is required to ask questions", 401
            )

    def check_rate(self, client: str) -> None:
        """Count one question for `client`, or refuse it with the seconds to wait."""
        if self.rate_limit <= 0:
            return
        now = self.clock()
        hits = self._hits.setdefault(client, deque())
        while hits and now - hits[0] >= self.window_seconds:
            hits.popleft()
        if len(hits) >= self.rate_limit:
            wait = math.ceil(self.window_seconds - (now - hits[0]))
            raise AppError(
                "rate_limited",
                f"Too many questions; try again in {wait} seconds",
                429,
                {"retry_after_seconds": wait},
            )
        hits.append(now)
        if len(self._hits) > 10_000:  # forget idle clients so memory stays bounded
            for key in [
                k for k, v in self._hits.items() if not v or now - v[-1] >= self.window_seconds
            ]:
                del self._hits[key]

    @contextmanager
    def slot(self) -> Iterator[None]:
        """Hold one of the concurrent answer slots, or refuse when all are busy."""
        if self.max_concurrent > 0 and self.active >= self.max_concurrent:
            raise AppError(
                "busy", "The chatbot is answering other questions; try again shortly", 503
            )
        self.active += 1
        try:
            yield
        finally:
            self.active -= 1


class ConnectionLimiter:
    """Caps open WebSocket connections."""

    def __init__(self, limit: int = 0) -> None:
        self.limit = limit
        self.open = 0

    def try_open(self) -> bool:
        if self.limit > 0 and self.open >= self.limit:
            return False
        self.open += 1
        return True

    def close(self) -> None:
        self.open = max(0, self.open - 1)
