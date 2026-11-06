"""Evaluate SportsBot on the golden questions (plan task 8).

Three steps, all run on the machine with the stack (PostgreSQL, Qdrant, embedder, reranker):

1. `make eval-pool`: for every answerable question without `expected_urls`, retrieve the top 10
   articles and write `data/evaluation/pool.csv`. A person marks `relevant` = 1 for the articles
   that really answer the question (pooling, the usual way to build relevance judgements).
2. `make eval-apply`: copy the marked links into `evaluation/golden_questions.csv`.
3. `make eval`: retrieval metrics (hit@1, hit@5, recall@5, MRR@10) on answerable questions;
   with answers (default) also the answer mode, whether an expected article is cited, correct
   refusals on unanswerable questions, and seconds per question. Writes
   `docs/reports/runs/eval-<run_id>.json` (question ids and numbers only, no article text).

Pooling only judges what the system retrieved, so recall is relative to that pool; the report
says so.
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import sys
import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
GOLDEN = ROOT / "evaluation" / "golden_questions.csv"
POOL = ROOT / "data" / "evaluation" / "pool.csv"
REPORTS = ROOT / "docs" / "reports" / "runs"
COLUMNS = [
    "question_id",
    "language",
    "question",
    "expected_urls",
    "answerable",
    "question_type",
    "author",
]
POOL_COLUMNS = [
    "question_id",
    "question",
    "rank",
    "relevant",
    "title",
    "source",
    "published_at",
    "url",
]
ANSWERED = {"generated", "excerpts"}
REFUSED = {"no_evidence", "insufficient"}


# ------------------------------------------------------------------- golden set -----


def read_questions(path: Path | None = None) -> list[dict[str, str]]:
    path = path or GOLDEN
    with path.open(encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    for row in rows:
        missing = [c for c in COLUMNS if c not in row]
        if missing:
            raise ValueError(f"{path}: missing columns {missing}")
        if row["answerable"] not in {"yes", "no"}:
            raise ValueError(f"{row['question_id']}: answerable must be yes or no")
    ids = [r["question_id"] for r in rows]
    if len(ids) != len(set(ids)):
        raise ValueError("duplicate question_id")
    return rows


def write_questions(rows: Sequence[dict[str, str]], path: Path | None = None) -> None:
    path = path or GOLDEN
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({c: row.get(c, "") for c in COLUMNS})


def split_urls(value: str) -> list[str]:
    return [u.strip() for u in (value or "").split(";") if u.strip()]


def expected_ids(row: dict[str, str]) -> set[str]:
    """Article ids of the expected links (same canonical form as the pipeline)."""
    from sportsbot_pipelines.collectors.urls import article_id, canonical_url

    ids = set()
    for url in split_urls(row["expected_urls"]):
        canonical = canonical_url(url)
        if canonical:
            ids.add(article_id(canonical))
    return ids


# --------------------------------------------------------------------- metrics -----


@dataclass
class Result:
    question_id: str
    language: str
    question_type: str
    answerable: bool
    expected: int  # number of expected articles
    rank: int | None  # 1-based rank of the first expected article in the top 10
    found_in_top5: int  # expected articles among the top 5
    mode: str | None = None
    cites_expected: bool | None = None
    seconds: float | None = None


def first_rank(retrieved: Sequence[str], expected: set[str]) -> int | None:
    for i, aid in enumerate(retrieved, 1):
        if aid in expected:
            return i
    return None


def summarize(results: Iterable[Result]) -> dict[str, Any]:
    results = list(results)
    judged = [r for r in results if r.answerable and r.expected]
    unanswerable = [r for r in results if not r.answerable]
    answerable = [r for r in results if r.answerable]

    def share(items: list[Any], test: Any) -> float | None:
        return round(sum(1 for r in items if test(r)) / len(items), 4) if items else None

    out: dict[str, Any] = {
        "questions": len(results),
        "answerable": len(answerable),
        "answerable_with_judgements": len(judged),
        "unanswerable": len(unanswerable),
        "retrieval": {
            "hit_at_1": share(judged, lambda r: r.rank == 1),
            "hit_at_5": share(judged, lambda r: r.rank is not None and r.rank <= 5),
            "recall_at_5": (
                round(sum(r.found_in_top5 / r.expected for r in judged) / len(judged), 4)
                if judged
                else None
            ),
            "mrr_at_10": (
                round(sum(1 / r.rank for r in judged if r.rank) / len(judged), 4)
                if judged
                else None
            ),
        },
    }
    with_answers = [r for r in results if r.mode is not None]
    if with_answers:
        seconds = sorted(r.seconds for r in with_answers if r.seconds is not None)
        answered_judged = [r for r in judged if r.mode is not None]
        out["answers"] = {
            "answerable_answered": share(
                [r for r in answerable if r.mode is not None], lambda r: r.mode in ANSWERED
            ),
            "answerable_cites_expected": share(answered_judged, lambda r: bool(r.cites_expected)),
            "unanswerable_correct_refusal": share(
                [r for r in unanswerable if r.mode is not None], lambda r: r.mode in REFUSED
            ),
            "seconds_p50": round(statistics.median(seconds), 2) if seconds else None,
            "seconds_p90": round(seconds[int(0.9 * (len(seconds) - 1))], 2) if seconds else None,
        }
    return out


# -------------------------------------------------------------------- commands -----


def _rag(use_llm: bool) -> Any:
    from sportsbot_agent.config import AgentConfig
    from sportsbot_agent.rag import build_rag

    config = AgentConfig.from_env()
    return config, build_rag(config, use_llm=use_llm)


def cmd_pool(k: int) -> int:
    rows = read_questions()
    todo = [r for r in rows if r["answerable"] == "yes" and not split_urls(r["expected_urls"])]
    if not todo:
        print("Every answerable question already has expected_urls.")
        return 0
    _, rag = _rag(use_llm=False)
    POOL.parent.mkdir(parents=True, exist_ok=True)
    try:
        with POOL.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=POOL_COLUMNS, lineterminator="\n")
            writer.writeheader()
            for row in todo:
                hits = rag.retriever.search(row["question"], k=k)
                for rank, hit in enumerate(hits, 1):
                    a = hit.article
                    writer.writerow(
                        {
                            "question_id": row["question_id"],
                            "question": row["question"] if rank == 1 else "",
                            "rank": rank,
                            "relevant": "",
                            "title": a.title,
                            "source": a.source_name,
                            "published_at": a.published_at or "",
                            "url": a.url,
                        }
                    )
                print(f"{row['question_id']}: {len(hits)} candidates", file=sys.stderr)
    finally:
        rag.close()
    print(f"Wrote {POOL}. Mark relevant = 1 for the articles that answer, then make eval-apply.")
    return 0


def apply_pool(rows: list[dict[str, str]], pool_rows: Iterable[dict[str, str]]) -> list[str]:
    """Add the marked links to `expected_urls`; return question ids still without any."""
    marked: dict[str, list[str]] = {}
    for p in pool_rows:
        if (p.get("relevant") or "").strip() in {"1", "x", "yes", "y"}:
            marked.setdefault(p["question_id"], []).append(p["url"].strip())
    for row in rows:
        urls = split_urls(row["expected_urls"])
        for url in marked.get(row["question_id"], []):
            if url not in urls:
                urls.append(url)
        row["expected_urls"] = ";".join(urls)
    return [r["question_id"] for r in rows if r["answerable"] == "yes" and not r["expected_urls"]]


def cmd_apply(pool_path: Path) -> int:
    rows = read_questions()
    with pool_path.open(encoding="utf-8", newline="") as f:
        unjudged = apply_pool(rows, csv.DictReader(f))
    write_questions(rows)
    print(f"Updated {GOLDEN}.")
    if unjudged:
        print(
            "Answerable but no relevant article marked (search the web app, or set answerable=no): "
            + ", ".join(unjudged)
        )
    return 0


def cmd_run(with_answers: bool, limit: int | None) -> int:
    rows = read_questions()[:limit] if limit else read_questions()
    config, rag = _rag(use_llm=with_answers)
    results: list[Result] = []
    started = datetime.now(UTC)
    try:
        for row in rows:
            expected = expected_ids(row)
            retrieved = [h.article.article_id for h in rag.retriever.search(row["question"], k=10)]
            result = Result(
                question_id=row["question_id"],
                language=row["language"],
                question_type=row["question_type"],
                answerable=row["answerable"] == "yes",
                expected=len(expected),
                rank=first_rank(retrieved, expected),
                found_in_top5=len(expected & set(retrieved[:5])),
            )
            if with_answers:
                t0 = time.perf_counter()
                answer, _meta = rag.ask(row["question"])
                result.seconds = round(time.perf_counter() - t0, 2)
                result.mode = answer.mode
                cited = {hit.article.article_id for _, hit in answer.sources}
                result.cites_expected = bool(cited & expected) if expected else None
            results.append(result)
            print(f"{row['question_id']}: rank={result.rank} mode={result.mode}", file=sys.stderr)
    finally:
        rag.close()
    run_id = started.strftime("%Y%m%dT%H%M%SZ")
    report = {
        "job": "evaluation",
        "run_id": run_id,
        "started_at": started.isoformat(),
        "golden_file": str(GOLDEN.relative_to(ROOT)),
        "with_answers": with_answers,
        "config": {
            "retrieval_candidates": config.retrieval_candidates,
            "rerank_top": config.rerank_top,
            "rerank_min_score": config.rerank_min_score,
            "top_k": config.top_k,
            "gemini_models": list(config.gemini_models) if with_answers else [],
        },
        "note": "Relevance judged by pooling the system's own top 10: recall is relative to it.",
        "metrics": summarize(results),
        "questions": [r.__dict__ for r in results],
    }
    REPORTS.mkdir(parents=True, exist_ok=True)
    out = REPORTS / f"eval-{run_id}.json"
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report["metrics"], ensure_ascii=False, indent=2))
    print(f"Report: {out.relative_to(ROOT)}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="SportsBot evaluation (plan task 8)")
    sub = parser.add_subparsers(dest="cmd", required=True)
    pool = sub.add_parser("pool", help="top-k candidates to judge")
    pool.add_argument("-k", type=int, default=10)
    apply = sub.add_parser("apply", help="copy judged links into the golden set")
    apply.add_argument("--file", type=Path, default=POOL)
    run = sub.add_parser("run", help="compute metrics and write a run report")
    run.add_argument("--no-answers", action="store_true", help="retrieval metrics only (no Gemini)")
    run.add_argument("--limit", type=int, default=None)
    args = parser.parse_args(argv)
    if args.cmd == "pool":
        return cmd_pool(args.k)
    if args.cmd == "apply":
        return cmd_apply(args.file)
    return cmd_run(with_answers=not args.no_answers, limit=args.limit)


if __name__ == "__main__":
    raise SystemExit(main())
