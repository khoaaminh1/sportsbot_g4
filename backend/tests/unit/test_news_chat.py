"""News, chat (JSON + SSE) and the live-news WebSocket, with fake repository and RAG."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator, FormatChecker

from app.services.news import decode_cursor, encode_cursor, to_summary
from sportsbot_agent.answer import Answer
from sportsbot_agent.retrieval import Hit
from sportsbot_agent.store import Article

ROOT = Path(__file__).resolve().parents[3]
OPENAPI = yaml.safe_load((ROOT / "docs/contracts/api.openapi.yaml").read_text(encoding="utf-8"))
TS = datetime(2026, 11, 1, 13, 15, tzinfo=UTC)
AID = "a" * 64


def validator(name: str) -> Draft202012Validator:
    schema = {"$ref": f"#/components/schemas/{name}", "components": OPENAPI["components"]}
    return Draft202012Validator(schema, format_checker=FormatChecker())


def row(i: int = 0) -> dict:
    return {
        "article_id": f"{i:x}" * 64 if i else AID,
        "title": f"Tin {i}",
        "excerpt": "Đội tuyển Việt Nam thắng. " * 30,
        "source_name": "Fixture",
        "source_url": f"https://news.fixture.test/{i}",
        "published_at": TS,
        "collected_at": TS,
        "ingested_at": TS.replace(minute=i),
        "language": "vi",
        "sport_category": None,
        "teams": [],
        "athletes": [],
        "competition": None,
    }


class FakeRepo:
    def __init__(self) -> None:
        self.rows = [row(0)]
        self.calls: list[dict] = []

    def latest(self, *, limit, sport, language, cursor):
        self.calls.append({"limit": limit, "sport": sport, "language": language, "cursor": cursor})
        if cursor:
            decode_cursor(cursor)
        return {"items": [to_summary(r) for r in self.rows], "next_cursor": None}

    def get(self, article_id):
        return to_summary(self.rows[0]) if article_id == AID else None

    def since(self, after, limit):
        return [r for r in self.rows if after is None or r["ingested_at"] > after]

    def categories(self):
        return [{"sport_category": "football", "article_count": 3, "latest_published_at": None}]

    def sources(self):
        return [{"source_id": "s", "name": "S", "kind": "rss", "language": "vi", "enabled": True}]

    runs: list[dict] = [
        {"run_id": 1, "source_id": "s", "source_name": "S", "status": "success", "items_new": 2}
    ]

    def runs_since(self, after, limit):
        return [r for r in self.runs if after is None or r["run_id"] > after]

    def stats(self):
        return {"articles": len(self.rows), "last_hour": 1, "sources_active": 1}


class FakeRag:
    def __init__(self, mode: str = "generated") -> None:
        self.mode = mode
        self.questions: list[str] = []

    def ask(self, question):
        self.questions.append(question)
        article = Article(
            AID, "Tin 0", "x", "Fixture", "https://news.fixture.test/0", TS.isoformat(), "", "vi"
        )
        if self.mode == "no_evidence":
            return Answer("no_evidence", "Không tìm thấy bài viết phù hợp.", "vi"), {"llm": None}
        text = "Việt Nam thắng Thái Lan 2-1 [1]. " * 4
        return Answer("generated", text, "vi", [(1, Hit(article, 3.2, 2))]), {"llm": "gemini:x"}


def test_latest_news_matches_contract(client_factory):
    repo = FakeRepo()
    client = client_factory(checks=[], news_repo=repo)
    resp = client.get("/news/latest", params={"limit": 5, "language": "vi"})
    assert resp.status_code == 200
    validator("ArticlePage").validate(resp.json())
    assert len(resp.json()["items"][0]["excerpt"]) <= 400
    assert repo.calls[0] == {"limit": 5, "sport": None, "language": "vi", "cursor": None}
    assert client.get("/news/latest", params={"sport": "cricket"}).status_code == 422
    assert client.get("/news/latest", params={"cursor": "%%%"}).status_code == 422


def test_cursor_round_trip():
    assert decode_cursor(encode_cursor("2026-11-01T13:15:00+00:00", "abc")) == (
        "2026-11-01T13:15:00+00:00",
        "abc",
    )


def test_article_categories_sources(client_factory):
    client = client_factory(checks=[], news_repo=FakeRepo())
    validator("ArticleSummary").validate(client.get(f"/articles/{AID}").json())
    missing = client.get(f"/articles/{'b' * 64}")
    assert missing.status_code == 404 and missing.json()["error"]["code"] == "not_found"
    assert client.get("/articles/not-an-id").status_code == 422
    for item in client.get("/categories").json():
        validator("Category").validate(item)
    for item in client.get("/sources").json():
        validator("SourceStatus").validate(item)


def test_chat_returns_contract_response_with_citations(client_factory):
    rag = FakeRag()
    client = client_factory(checks=[], news_repo=FakeRepo(), rag=rag)
    resp = client.post("/chat", json={"message": "  Việt Nam   thắng mấy bàn? "})
    assert resp.status_code == 200
    body = resp.json()
    validator("ChatResponse").validate(body)
    assert body["citations"][0]["source_url"] == "https://news.fixture.test/0"
    assert body["insufficient_evidence"] is False and body["meta"]["llm"] == "gemini:x"
    assert rag.questions == ["Việt Nam thắng mấy bàn?"]


def test_chat_without_evidence_says_so(client_factory):
    client = client_factory(checks=[], news_repo=FakeRepo(), rag=FakeRag("no_evidence"))
    body = client.post("/chat", json={"message": "Ai vô địch?"}).json()
    validator("ChatResponse").validate(body)
    assert body["insufficient_evidence"] is True and body["citations"] == []


def test_chat_validation_and_unconfigured(client_factory):
    client = client_factory(checks=[], news_repo=FakeRepo(), rag=FakeRag())
    assert client.post("/chat", json={"message": ""}).status_code == 422
    assert client.post("/chat", json={"message": "???"}).json()["error"]["code"] == (
        "invalid_question"
    )
    off = client_factory(checks=[], news_repo=FakeRepo(), rag=None)
    resp = off.post("/chat", json={"message": "hello"})
    assert resp.status_code == 503 and resp.json()["error"]["code"] == "chat_unavailable"
    assert off.post("/chat/stream", json={"message": "hello"}).status_code == 503


def parse_sse(text: str) -> list[tuple[str, object]]:
    events = []
    for block in text.strip().split("\n\n"):
        lines = dict(line.split(": ", 1) for line in block.splitlines())
        events.append((lines["event"], json.loads(lines["data"])))
    return events


def test_chat_stream_sends_checked_answer_in_order(client_factory):
    client = client_factory(checks=[], news_repo=FakeRepo(), rag=FakeRag())
    resp = client.post("/chat/stream", json={"message": "Việt Nam thắng mấy bàn?"})
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/event-stream")
    events = parse_sse(resp.text)
    names = [e for e, _ in events]
    assert names[0] == "status" and names[-2:] == ["citations", "done"]
    answer = "".join(d["text"] for e, d in events if e == "delta")
    assert answer.startswith("Việt Nam thắng Thái Lan 2-1 [1].")
    assert events[-1][1]["mode"] == "generated"


def test_news_websocket_pushes_latest_then_new(client_factory):
    repo = FakeRepo()
    client = client_factory(checks=[], news_repo=repo, news_poll_seconds=0.05)
    with client.websocket_connect("/ws/news") as ws:
        first = [ws.receive_json() for _ in range(3)]
        assert [m["type"] for m in first] == ["articles", "runs", "stats"]
        assert first[0]["items"][0]["article_id"] == AID
        assert first[1]["items"][0]["items_new"] == 2 and first[2]["articles"] == 1
        repo.rows.append(row(5))
        repo.runs = [*repo.runs, {"run_id": 2, "source_id": "s", "status": "failed"}]
        seen: dict[str, dict] = {}
        while not {"articles", "runs"} <= seen.keys():
            msg = ws.receive_json()
            seen.setdefault(msg["type"], msg)
        assert [i["title"] for i in seen["articles"]["items"]] == ["Tin 5"]
        assert [r["run_id"] for r in seen["runs"]["items"]] == [2]


def test_frontend_is_served(client_factory):
    client = client_factory(checks=[], news_repo=FakeRepo())
    resp = client.get("/app/")
    assert resp.status_code == 200 and "SportsBot" in resp.text
