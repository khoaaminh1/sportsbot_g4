"""Advanced RAG: fusion, reranking, graceful degradation, Gemini rotation, AI clients."""

from __future__ import annotations

import json

import httpx
import pytest

from sportsbot_agent.answer import answer_question
from sportsbot_agent.clients.embeddings import EmbeddingClient
from sportsbot_agent.clients.llm_router import (
    ChatRouter,
    ChatTarget,
    build_targets,
    seconds_until_pacific_midnight,
)
from sportsbot_agent.clients.qdrant import QdrantClient, point_id
from sportsbot_agent.clients.rerank import RerankClient, RerankResult
from sportsbot_agent.config import AgentConfig
from sportsbot_agent.hybrid import Candidate, HybridRetriever, rrf_fuse
from sportsbot_agent.llm_client import LLMError, Message
from sportsbot_agent.rag import RagService, answer_to_dict


def cand(aid: str, passage: str = "p", title: str | None = None) -> Candidate:
    return Candidate(
        article_id=aid,
        title=title or f"Title {aid}",
        passage=passage,
        source_name="Src",
        url=f"https://x.test/{aid}",
        published_at="2026-11-01T10:00:00+00:00",
        language="vi",
    )


class StaticSearch:
    def __init__(self, results: list[Candidate] | Exception) -> None:
        self.results = results
        self.calls: list[object] = []

    def search(self, query, limit):
        self.calls.append(query)
        if isinstance(self.results, Exception):
            raise self.results
        return self.results[:limit]


class ScoreReranker:
    """Scores documents by a dict lookup on the article title."""

    def __init__(self, scores: dict[str, float]) -> None:
        self.scores = scores

    def rerank(self, query, documents, top_n=None):
        out = [
            RerankResult(i, self.scores.get(d.split("\n", 1)[0], -10.0))
            for i, d in enumerate(documents)
        ]
        return sorted(out, key=lambda r: r.score, reverse=True)


# --------------------------------------------------------------- retrieval -----


def test_rrf_rewards_articles_found_by_both_stages():
    fused = rrf_fuse(
        {"keyword": [cand("a"), cand("b")], "vector": [cand("c"), cand("b", passage="longer one")]}
    )
    assert fused[0].article_id == "b" and set(fused[0].ranks) == {"keyword", "vector"}
    assert fused[0].passage == "longer one"
    assert [c.article_id for c in fused] == ["b", "a", "c"]


def test_reranker_orders_and_filters_candidates():
    retriever = HybridRetriever(
        keyword=StaticSearch([cand("a"), cand("b"), cand("c")]),
        vector=StaticSearch([cand("c")]),
        reranker=ScoreReranker({"Title a": 3.0, "Title b": -9.0, "Title c": 1.0}),
        min_rerank_score=-2.0,
    )
    hits = retriever.search("doi tuyen viet nam", k=5)
    assert [h.article.article_id for h in hits] == ["a", "c"]  # b is below the threshold
    assert hits[0].score == 3.0
    assert retriever.last_trace["stages"] == {
        "keyword": 3,
        "vector": 1,
        "fused": 3,
        "reranked_kept": 2,
    }


def test_retriever_degrades_when_services_fail():
    retriever = HybridRetriever(
        keyword=StaticSearch([cand("a")]),
        vector=StaticSearch(ConnectionError("embedder down")),
        reranker=None,
    )
    hits = retriever.search("viet nam thai lan", k=3)
    assert [h.article.article_id for h in hits] == ["a"]
    assert "vector" in retriever.last_trace["errors"]

    broken_rerank = HybridRetriever(
        keyword=StaticSearch([cand("a"), cand("b")]),
        vector=None,
        reranker=StaticSearch(RuntimeError("x")),  # has no rerank(): AttributeError, caught
    )
    assert len(broken_rerank.search("viet nam", k=3)) == 2
    assert "rerank" in broken_rerank.last_trace["errors"]


def test_keyword_search_gets_accent_free_terms():
    keyword = StaticSearch([])
    HybridRetriever(keyword=keyword, vector=None, reranker=None).search("Đội tuyển Việt Nam?")
    assert keyword.calls == [["doi", "tuyen", "viet", "nam"]]


class FakeLLM:
    def __init__(self, reply: str) -> None:
        self.reply = reply
        self.prompts: list[list[Message]] = []

    def chat(self, messages):
        self.prompts.append(messages)
        return self.reply


def test_answer_with_hybrid_retriever_cites_links():
    retriever = HybridRetriever(
        keyword=StaticSearch([cand("a", "Việt Nam thắng Thái Lan 2-1.")]),
        vector=None,
        reranker=None,
    )
    llm = FakeLLM("Việt Nam thắng Thái Lan 2-1 [1].")
    rag = RagService(retriever, llm)  # type: ignore[arg-type]
    answer, meta = rag.ask("Việt Nam thắng Thái Lan mấy bàn?")
    data = answer_to_dict(answer, meta)
    assert data["mode"] == "generated" and data["citations"][0]["url"] == "https://x.test/a"
    assert data["citations"][0]["published_at"].startswith("2026-11-01")
    assert "Việt Nam thắng Thái Lan 2-1." in llm.prompts[0][1].content


def test_no_evidence_means_no_llm_call():
    llm = FakeLLM("should not be called")
    retriever = HybridRetriever(keyword=StaticSearch([]), vector=None, reranker=None)
    answer = answer_question("Ai vô địch Wimbledon?", retriever, llm)
    assert answer.mode == "no_evidence" and llm.prompts == []


# ---------------------------------------------------------------- LLM router -----


def router_with(responses: dict[str, list[httpx.Response]], clock) -> ChatRouter:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        model = json.loads(request.content)["model"]
        calls.append(model)
        return responses[model].pop(0)

    router = ChatRouter(
        [
            ChatTarget("gemini:a", "https://g.test/v1beta/openai", "a", "KEY"),
            ChatTarget("gemini:b", "https://g.test/v1beta/openai", "b", "KEY"),
            ChatTarget("gemini:c", "https://g.test/v1beta/openai", "c", "KEY"),
        ],
        transport=httpx.MockTransport(handler),
        clock=clock,
        cooldown_seconds=60,
    )
    router.calls = calls  # type: ignore[attr-defined]
    return router


def ok(text: str) -> httpx.Response:
    return httpx.Response(200, json={"choices": [{"message": {"content": text}}]})


def test_router_rotates_on_quota_and_recovers_after_cooldown():
    now = [1000.0]
    router = router_with(
        {
            "a": [httpx.Response(429, text="Resource exhausted: requests per minute"), ok("A2")],
            "b": [ok("B1")],
            "c": [],
        },
        clock=lambda: now[0],
    )
    msgs = [Message("user", "hi")]
    assert router.chat(msgs) == "B1" and router.last_target == "gemini:b"
    assert [s["available"] for s in router.status()] == [False, True, True]
    now[0] += 61
    assert router.chat(msgs) == "A2" and router.last_target == "gemini:a"
    assert router.calls == ["a", "b", "a"]  # type: ignore[attr-defined]


def test_router_daily_quota_waits_until_pacific_midnight_and_uses_next_model():
    router = router_with(
        {
            "a": [httpx.Response(429, text="Quota exceeded: GenerateRequestsPerDay")],
            "b": [httpx.Response(404, text="model not found")],
            "c": [ok("third")],
        },
        clock=lambda: 0.0,
    )
    assert router.chat([Message("user", "hi")]) == "third"
    status = {s["target"]: s for s in router.status()}
    assert status["gemini:a"]["cooldown_seconds"] > 60
    assert status["gemini:b"]["disabled"] is True


def test_router_raises_when_nothing_answers():
    router = router_with(
        {"a": [httpx.Response(500)], "b": [httpx.Response(401)], "c": [httpx.Response(503)]},
        clock=lambda: 0.0,
    )
    with pytest.raises(LLMError, match="No LLM target answered"):
        router.chat([Message("user", "hi")])


def test_build_targets_and_midnight():
    targets = build_targets("K", "https://g", ["m1", "", "m2"])
    assert [t.name for t in targets] == ["gemini:m1", "gemini:m2"]
    assert build_targets("", "https://g", ["m1"]) == []
    assert 0 < seconds_until_pacific_midnight() <= 86_405


def test_config_reads_gemini_models_list():
    cfg = AgentConfig.from_env({"GEMINI_MODELS": "x-flash, y-lite ,"})
    assert cfg.gemini_models == ("x-flash", "y-lite")
    assert AgentConfig.from_env({}).gemini_models[0].startswith("gemini-")


# ------------------------------------------------------------------ clients -----


def test_embedding_client_batches_and_normalises():
    seen: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        seen.append(len(body["input"]))
        data = [{"index": i, "embedding": [3.0, 4.0]} for i in range(len(body["input"]))]
        return httpx.Response(200, json={"data": data})

    client = EmbeddingClient("http://e.test", batch_size=2, transport=httpx.MockTransport(handler))
    vectors = client.embed(["a", "b", "c"])
    assert seen == [2, 1] and vectors[0] == pytest.approx([0.6, 0.8])


def test_rerank_client_sorts_by_score():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/rerank"
        return httpx.Response(
            200,
            json={
                "results": [
                    {"index": 0, "relevance_score": -3.2},
                    {"index": 1, "relevance_score": 5.1},
                ]
            },
        )

    client = RerankClient("http://r.test", transport=httpx.MockTransport(handler))
    assert [r.index for r in client.rerank("q", ["x", "y"])] == [1, 0]
    assert client.rerank("q", []) == []


def test_qdrant_client_creates_collection_once_and_searches():
    calls: list[tuple[str, str]] = []
    created = {"done": False}

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path))
        if request.method == "GET":
            return httpx.Response(200 if created["done"] else 404, json={})
        if request.url.path.endswith("/points/query"):
            return httpx.Response(
                200, json={"result": {"points": [{"id": "u1", "score": 0.9, "payload": {"a": 1}}]}}
            )
        created["done"] = True
        return httpx.Response(200, json={"result": True})

    client = QdrantClient("http://q.test", transport=httpx.MockTransport(handler))
    assert client.ensure_collection(1024) is True
    assert client.ensure_collection(1024) is False
    [hit] = client.search([0.1, 0.2])
    assert hit.score == 0.9 and hit.payload == {"a": 1}
    assert point_id("abc", 0) == point_id("abc", 0) != point_id("abc", 1)
