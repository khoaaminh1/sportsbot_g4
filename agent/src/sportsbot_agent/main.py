"""Terminal prototype: ask a sports question, get an answer from stored articles with links.

    PYTHONPATH=agent/src python -m sportsbot_agent.main            # interactive
    PYTHONPATH=agent/src python -m sportsbot_agent.main "câu hỏi"  # one question
    make chat                                                      # same, via make

The answer comes only from articles collected by SportsBot (see store.py). Without matching
articles it says so and does not call the model.
"""

from __future__ import annotations

import argparse
import sys
import time

from sportsbot_agent.answer import Answer, answer_question
from sportsbot_agent.config import AgentConfig
from sportsbot_agent.llm_client import LLMClient
from sportsbot_agent.retrieval import BM25Index
from sportsbot_agent.store import load_articles

EXIT_WORDS = {"/exit", "/quit", "exit", "quit"}


def validate_question(raw: str, max_chars: int) -> tuple[str | None, str | None]:
    """Return (question, error)."""
    question = " ".join(raw.split())
    if not question:
        return None, "Please type a question."
    if len(question) > max_chars:
        return None, f"The question is too long ({len(question)} characters; max {max_chars})."
    if not any(c.isalnum() for c in question):
        return None, "The question must contain letters or numbers."
    return question, None


def format_answer(answer: Answer, seconds: float) -> str:
    lines = [answer.text]
    if answer.notice:
        lines += ["", f"Note: {answer.notice}"]
    if answer.sources:
        lines += ["", "Sources:"]
        for number, hit in answer.sources:
            a = hit.article
            published = a.published_at or "unknown"
            lines.append(f"  [{number}] {a.title}")
            lines.append(f"      {a.source_name} | published {published}")
            lines.append(f"      {a.url}")
    lines += ["", f"({answer.mode}, {seconds:.1f}s)"]
    return "\n".join(lines)


def run(config: AgentConfig, questions: list[str] | None = None, no_llm: bool = False) -> int:
    articles = load_articles(config.articles_dir)
    if not articles:
        print(
            f"No stored articles found in {config.articles_dir}.\n"
            "Collect and export them first (see docs/runbooks/chat-prototype.md):\n"
            "  make up-collect && make collect-once && make articles-export",
            file=sys.stderr,
        )
        return 2
    index = BM25Index(articles)
    llm = None if no_llm else LLMClient(config)
    print(
        f"SportsBot prototype: {len(articles)} stored articles | "
        f"model {'(none: excerpts only)' if no_llm else config.llm_model} | /exit to quit"
    )

    def ask(raw: str) -> None:
        question, error = validate_question(raw, config.max_question_chars)
        if error:
            print(error)
            return
        t0 = time.perf_counter()
        answer = answer_question(
            question, index, llm, top_k=config.top_k, min_matched_terms=config.min_matched_terms
        )
        print(format_answer(answer, time.perf_counter() - t0))

    try:
        if questions:
            for q in questions:
                print(f"\n> {q}")
                ask(q)
            return 0
        while True:
            try:
                raw = input("\nYou: ")
            except (EOFError, KeyboardInterrupt):
                print()
                return 0
            if raw.strip().lower() in EXIT_WORDS:
                return 0
            ask(raw)
    finally:
        if llm:
            llm.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="SportsBot terminal prototype")
    parser.add_argument("question", nargs="*", help="ask one question and exit")
    parser.add_argument(
        "--no-llm", action="store_true", help="show matching article excerpts without the model"
    )
    args = parser.parse_args(argv)
    questions = [" ".join(args.question)] if args.question else None
    return run(AgentConfig.from_env(), questions, no_llm=args.no_llm)


if __name__ == "__main__":
    sys.exit(main())
