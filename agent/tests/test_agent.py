"""Agent prototype tests. Articles are SYNTHETIC fixtures; the LLM is a fake server."""

from __future__ import annotations

import gzip
import json
import shutil
from pathlib import Path

import httpx
import pytest

from sportsbot_agent.answer import answer_question, detect_language, validate_citations
from sportsbot_agent.config import AgentConfig
from sportsbot_agent.llm_client import (
    LLMClient,
    LLMError,
    LLMModelNotFound,
    LLMTimeout,
    LLMUnavailable,
)
from sportsbot_agent.main import format_answer, run, validate_question
from sportsbot_agent.retrieval import BM25Index, normalize, query_terms
from sportsbot_agent.store import html_to_text, load_articles

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def articles():
    return load_articles(FIXTURES)


@pytest.fixture
def index(articles):
    return BM25Index(articles)


def fake_llm(reply: str | None = None, status: int = 200, error: Exception | None = None):
    calls: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(json.loads(request.content))
        if error:
            raise error
        if status != 200:
            return httpx.Response(status, text="model 'llama3:latest' not found")
        return httpx.Response(200, json={"choices": [{"message": {"content": reply}}]})

    client = LLMClient(AgentConfig(), transport=httpx.MockTransport(handler))
    return client, calls


# ------------------------------------------------------------------- config ----


def test_config_from_env_and_defaults():
    default = AgentConfig.from_env({})
    assert default.llm_base_url == "http://localhost:11434/v1"
    assert default.llm_model == "llama3:latest"
    custom = AgentConfig.from_env(
        {"LLM_BASE_URL": "http://pc:11434/v1/", "LLM_MODEL": "qwen2.5:7b", "RETRIEVAL_TOP_K": "3"}
    )
    assert custom.llm_base_url == "http://pc:11434/v1"
    assert custom.llm_model == "qwen2.5:7b" and custom.top_k == 3


# -------------------------------------------------------------------- store ----


def test_store_dedupes_skips_bad_lines_and_reads_gzip(tmp_path, articles):
    assert {a.article_id for a in articles} == {"a1", "a2", "a3"}
    a3 = next(a for a in articles if a.article_id == "a3")
    assert "older copy" not in a3.title  # latest collection wins
    gz = tmp_path / "part-1.jsonl.gz"
    gz.write_bytes(gzip.compress((FIXTURES / "articles.jsonl").read_bytes()))
    assert len(load_articles(tmp_path)) == 3
    assert load_articles(tmp_path / "missing") == []


def test_html_to_text():
    assert html_to_text("<p>A <b>b</b> &amp; c</p>") == "A b & c"
    assert html_to_text(None) == ""


# ---------------------------------------------------------------- retrieval ----


def test_accent_insensitive_search_and_aliases(index):
    assert normalize("Đội Tuyển") == "doi tuyen"
    assert index.search("doi tuyen viet nam")[0].article.article_id == "a1"
    assert index.search("Đội tuyển VN")[0].article.article_id == "a1"  # alias vn -> viet nam
    assert "bong" in query_terms("volleyball") and "chuyen" in query_terms("volleyball")
    assert index.search("Manchester United result")[0].article.article_id == "a3"


def test_no_match_returns_nothing(index):
    assert index.search("cricket in Australia") == []
    assert index.search("   ") == []


# ------------------------------------------------------------------- answer ----


def test_no_evidence_does_not_call_llm(index):
    llm, calls = fake_llm("should not be used [1]")
    answer = answer_question("cricket Australia", index, llm)
    assert answer.mode == "no_evidence" and calls == []
    assert "No matching article" in answer.text


def test_generated_answer_lists_only_cited_sources_with_their_numbers(index):
    llm, calls = fake_llm("Manchester United won 2-1 at home [1].")
    answer = answer_question("Manchester United latest match", index, llm)
    assert answer.mode == "generated"
    assert [(n, h.article.article_id) for n, h in answer.sources] == [(1, "a3")]
    prompt = calls[0]["messages"][0]["content"]
    assert "ONLY" in prompt and "INSUFFICIENT_EVIDENCE" in prompt and "English" in prompt
    assert "Manchester United win 2-1" in calls[0]["messages"][1]["content"]


def test_vietnamese_question_gets_vietnamese_rules(index):
    llm, calls = fake_llm("Đội tuyển triệu tập 25 cầu thủ [1].")
    answer = answer_question("Đội tuyển Việt Nam triệu tập bao nhiêu cầu thủ?", index, llm)
    assert answer.mode == "generated" and answer.language == "vi"
    assert "Answer in Vietnamese" in calls[0]["messages"][0]["content"]


@pytest.mark.parametrize(
    "reply",
    [
        "Manchester United won 2-1.",  # no citation
        "Manchester United won 2-1 [7].",  # citation out of range
        "Manchester United won [1]. See https://example.com",  # URL added by the model
    ],
)
def test_invalid_answers_fall_back_to_excerpts(index, reply):
    llm, _ = fake_llm(reply)
    answer = answer_question("Manchester United", index, llm)
    assert answer.mode == "excerpts" and answer.notice
    assert answer.sources and answer.sources[0][0] == 1
    assert "SYNTHETIC: Manchester United" in answer.text


def test_insufficient_evidence(index):
    llm, _ = fake_llm("INSUFFICIENT_EVIDENCE")
    answer = answer_question("Manchester United injuries", index, llm)
    assert answer.mode == "insufficient" and answer.sources


def test_llm_down_still_shows_sources(index):
    llm, _ = fake_llm(error=httpx.ConnectError("refused"))
    answer = answer_question("Manchester United", index, llm)
    assert answer.mode == "excerpts" and "Ollama" in answer.notice


def test_detect_language_and_citation_check():
    assert detect_language("Kết quả trận đấu") == "vi"
    assert detect_language("latest result") == "en"
    assert validate_citations("A [2]. B [1][2].", 3) == [2, 1]
    assert validate_citations("A [0].", 3) is None


# --------------------------------------------------------------- llm client ----


def test_llm_client_errors():
    with pytest.raises(LLMUnavailable, match="Ollama"):
        fake_llm(error=httpx.ConnectError("refused"))[0].chat([])
    with pytest.raises(LLMTimeout):
        fake_llm(error=httpx.ReadTimeout("slow"))[0].chat([])
    with pytest.raises(LLMModelNotFound, match="ollama pull"):
        fake_llm(status=404)[0].chat([])
    with pytest.raises(LLMError):
        fake_llm(status=500)[0].chat([])


def test_llm_client_sends_openai_compatible_request():
    llm, calls = fake_llm("ok")
    from sportsbot_agent.llm_client import Message

    assert llm.chat([Message("user", "hi")]) == "ok"
    assert calls[0]["model"] == "llama3:latest" and calls[0]["stream"] is False
    assert calls[0]["messages"] == [{"role": "user", "content": "hi"}]


# --------------------------------------------------------------------- main ----


@pytest.mark.parametrize(
    ("raw", "ok"),
    [("  Kết quả   trận  ", True), ("", False), ("   ", False), ("???", False), ("x" * 501, False)],
)
def test_validate_question(raw, ok):
    question, error = validate_question(raw, 500)
    assert (question is not None) is ok and (error is None) is ok


def test_run_without_llm_prints_answer_with_links(tmp_path, capsys):
    shutil.copy(FIXTURES / "articles.jsonl", tmp_path / "articles.jsonl")
    config = AgentConfig(articles_dir=tmp_path)
    assert run(config, ["doi tuyen viet nam"], no_llm=True) == 0
    out = capsys.readouterr().out
    assert "Sources:" in out and "https://news.fixture.test/bong-da/a1.html" in out
    assert "published 2026-11-01T03:00:00+00:00" in out


def test_run_with_no_articles_explains_what_to_do(tmp_path, capsys):
    assert run(AgentConfig(articles_dir=tmp_path), ["anything"], no_llm=True) == 2
    assert "make articles-export" in capsys.readouterr().err


def test_format_answer_no_sources(index):
    answer = answer_question("cricket", index, None)
    assert "Sources:" not in format_answer(answer, 0.1)
