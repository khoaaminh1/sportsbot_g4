"""Access code, rate limit and connection caps for a public URL (core/guard.py)."""

from __future__ import annotations

import pytest

from app.core.errors import AppError
from app.core.guard import ChatGuard, ConnectionLimiter, client_key


class FakeRepo:
    def since(self, after, limit):
        return []

    def runs_since(self, after, limit):
        return []

    def stats(self):
        return {"articles": 1, "last_hour": 0, "sources_active": 1}


class FakeRag:
    def __init__(self) -> None:
        self.questions: list[str] = []

    def ask(self, question, history=()):
        from sportsbot_agent.answer import Answer

        self.questions.append(question)
        return Answer("no_evidence", "Không tìm thấy bài viết phù hợp.", "vi"), {"llm": None}


class Clock:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t


def test_access_code_is_checked_only_when_set():
    ChatGuard().check_code(None)
    guard = ChatGuard(access_code="san-van-dong")
    guard.check_code(" san-van-dong ")
    for wrong in (None, "", "san-van"):
        with pytest.raises(AppError) as err:
            guard.check_code(wrong)
        assert err.value.status_code == 401 and err.value.code == "access_code_required"


def test_rate_limit_per_client_with_sliding_window():
    clock = Clock()
    guard = ChatGuard(rate_limit=2, window_seconds=60, clock=clock)
    guard.check_rate("a")
    guard.check_rate("a")
    guard.check_rate("b")  # another visitor has its own budget
    with pytest.raises(AppError) as err:
        guard.check_rate("a")
    assert err.value.status_code == 429 and err.value.details == {"retry_after_seconds": 60}
    clock.t += 60
    guard.check_rate("a")  # the window has moved on
    ChatGuard(rate_limit=0).check_rate("a")  # 0 disables


def test_concurrency_slots_are_released():
    guard = ChatGuard(max_concurrent=1)
    with guard.slot():
        with pytest.raises(AppError) as err, guard.slot():
            pass
        assert err.value.status_code == 503
    with guard.slot():
        assert guard.active == 1
    assert guard.active == 0


def test_client_key_prefers_cloudflare_header():
    assert client_key({"cf-connecting-ip": "203.0.113.7"}, "172.18.0.9") == "203.0.113.7"
    assert client_key({}, "172.18.0.9") == "172.18.0.9"
    assert client_key({}, None) == "unknown"


def test_connection_limiter():
    limiter = ConnectionLimiter(1)
    assert limiter.try_open() and not limiter.try_open()
    limiter.close()
    assert limiter.try_open()
    assert all(ConnectionLimiter(0).try_open() for _ in range(5))


def test_chat_endpoints_enforce_code_and_rate(client_factory):
    rag = FakeRag()
    client = client_factory(
        checks=[], news_repo=FakeRepo(), rag=rag, chat_access_code="ma-demo", chat_rate_limit=1
    )
    denied = client.post("/chat", json={"message": "Tin tennis?"})
    assert denied.status_code == 401
    assert denied.json()["error"]["code"] == "access_code_required"
    assert client.post("/chat/stream", json={"message": "Tin tennis?"}).status_code == 401
    assert rag.questions == []  # refused before any work

    ok = client.post("/chat", json={"message": "Tin tennis?"}, headers={"X-Access-Code": "ma-demo"})
    assert ok.status_code == 200
    limited = client.post(
        "/chat", json={"message": "Tin F1?"}, headers={"X-Access-Code": "ma-demo"}
    )
    assert limited.status_code == 429 and limited.json()["error"]["code"] == "rate_limited"
    assert rag.questions == ["Tin tennis?"]


def test_news_feed_refuses_connections_over_the_cap(client_factory):
    from starlette.websockets import WebSocketDisconnect

    client = client_factory(checks=[], news_repo=FakeRepo(), rag=FakeRag(), ws_max_connections=1)
    with client.websocket_connect("/ws/news") as first:
        assert first.receive_json()["type"] in {"articles", "runs", "stats"}
        with (
            pytest.raises(WebSocketDisconnect) as closed,
            client.websocket_connect("/ws/news") as ws,
        ):
            ws.receive_json()
        assert closed.value.code == 1013
