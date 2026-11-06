"""Golden set format and evaluation metrics (evaluation/sportsbot_eval.py)."""

from __future__ import annotations

import csv
import io

import sportsbot_eval as ev

from sportsbot_pipelines.collectors.urls import article_id, canonical_url


def test_golden_set_is_valid_and_balanced():
    rows = ev.read_questions()
    assert len(rows) == 50
    assert sum(r["answerable"] == "no" for r in rows) == 15
    assert sum(r["language"] == "en" for r in rows) >= 15
    assert all(r["question_id"] == f"q{i:03d}" for i, r in enumerate(rows, 1))
    # unanswerable questions never list articles
    assert all(not r["expected_urls"] for r in rows if r["answerable"] == "no")


def test_expected_ids_use_the_pipeline_canonical_form():
    url = "https://VNExpress.net/abc-123.html?utm_source=x"
    row = {"expected_urls": f"{url}; https://example.test/b-2.html", "question_id": "q1"}
    ids = ev.expected_ids(row)
    assert article_id(canonical_url(url)) in ids and len(ids) == 2


def test_summary_metrics():
    results = [
        ev.Result(
            "q1",
            "vi",
            "result",
            True,
            2,
            rank=1,
            found_in_top5=2,
            mode="generated",
            cites_expected=True,
            seconds=10.0,
        ),
        ev.Result(
            "q2",
            "en",
            "player",
            True,
            1,
            rank=4,
            found_in_top5=1,
            mode="excerpts",
            cites_expected=False,
            seconds=20.0,
        ),
        ev.Result(
            "q3",
            "vi",
            "team",
            True,
            1,
            rank=None,
            found_in_top5=0,
            mode="no_evidence",
            cites_expected=False,
            seconds=2.0,
        ),
        ev.Result(
            "q4",
            "vi",
            "other",
            False,
            0,
            rank=None,
            found_in_top5=0,
            mode="no_evidence",
            seconds=1.0,
        ),
        ev.Result(
            "q5",
            "en",
            "other",
            False,
            0,
            rank=None,
            found_in_top5=0,
            mode="generated",
            seconds=30.0,
        ),
    ]
    m = ev.summarize(results)
    assert m["answerable_with_judgements"] == 3 and m["unanswerable"] == 2
    r = m["retrieval"]
    assert r["hit_at_1"] == round(1 / 3, 4)
    assert r["hit_at_5"] == round(2 / 3, 4)
    assert r["recall_at_5"] == round((1 + 1 + 0) / 3, 4)
    assert r["mrr_at_10"] == round((1 + 1 / 4) / 3, 4)
    a = m["answers"]
    assert a["answerable_answered"] == round(2 / 3, 4)
    assert a["answerable_cites_expected"] == round(1 / 3, 4)
    assert a["unanswerable_correct_refusal"] == 0.5
    assert a["seconds_p50"] == 10.0


def test_apply_pool_adds_marked_links_only():
    rows = [
        {"question_id": "q1", "expected_urls": "", "answerable": "yes"},
        {"question_id": "q2", "expected_urls": "https://a.test/x", "answerable": "yes"},
        {"question_id": "q3", "expected_urls": "", "answerable": "yes"},
    ]
    pool = io.StringIO()
    w = csv.DictWriter(pool, fieldnames=ev.POOL_COLUMNS)
    w.writeheader()
    w.writerow({"question_id": "q1", "rank": 1, "relevant": "1", "url": "https://a.test/1"})
    w.writerow({"question_id": "q1", "rank": 2, "relevant": "", "url": "https://a.test/2"})
    w.writerow({"question_id": "q2", "rank": 1, "relevant": "x", "url": "https://a.test/x"})
    pool.seek(0)
    unjudged = ev.apply_pool(rows, csv.DictReader(pool))
    assert rows[0]["expected_urls"] == "https://a.test/1"
    assert rows[1]["expected_urls"] == "https://a.test/x"  # no duplicate
    assert unjudged == ["q3"]


def test_run_writes_a_report_without_article_text(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from sportsbot_agent.answer import Answer

    url = "https://vnexpress.net/doi-tuyen-123.html"
    golden = tmp_path / "golden.csv"
    ev.write_questions(
        [
            {
                "question_id": "q001",
                "language": "vi",
                "question": "Tin đội tuyển?",
                "expected_urls": url,
                "answerable": "yes",
                "question_type": "latest",
                "author": "TMK",
            },
            {
                "question_id": "q002",
                "language": "en",
                "question": "2040 Super Bowl?",
                "expected_urls": "",
                "answerable": "no",
                "question_type": "other",
                "author": "TMK",
            },
        ],
        golden,
    )
    hit = SimpleNamespace(article=SimpleNamespace(article_id=article_id(canonical_url(url))))

    class Rag:
        retriever = SimpleNamespace(search=lambda q, k: [hit] if "đội" in q else [])

        def ask(self, q):
            if "đội" in q:
                return Answer("generated", "Bài viết [1].", "vi", sources=[(1, hit)]), {}
            return Answer("no_evidence", "Không tìm thấy.", "en"), {}

        def close(self):
            pass

    config = SimpleNamespace(
        retrieval_candidates=30, rerank_top=12, rerank_min_score=-2.0, top_k=5, gemini_models=("m",)
    )
    monkeypatch.setattr(ev, "GOLDEN", golden)
    monkeypatch.setattr(ev, "ROOT", tmp_path)
    monkeypatch.setattr(ev, "REPORTS", tmp_path / "runs")
    monkeypatch.setattr(ev, "_rag", lambda use_llm: (config, Rag()))
    assert ev.main(["run"]) == 0
    report = next((tmp_path / "runs").glob("eval-*.json")).read_text(encoding="utf-8")
    import json

    data = json.loads(report)
    assert data["metrics"]["retrieval"]["hit_at_1"] == 1.0
    assert data["metrics"]["answers"]["unanswerable_correct_refusal"] == 1.0
    assert "Bài viết" not in report  # numbers only, no answer or article text
