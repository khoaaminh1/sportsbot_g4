"""Terminal client for the production RAG (PostgreSQL + Qdrant + reranker + Gemini/Ollama).

    PYTHONPATH=agent/src python -m sportsbot_agent.ask "Ai ghi bàn cho Việt Nam tối qua?"
    make ask Q="Ai ghi bàn cho Việt Nam tối qua?"

Needs the stack: `make up-stream` (PostgreSQL, Qdrant, Kafka, workers) and `make up-ai`
(embedder + reranker). Set GEMINI_API_KEY in .env, or keep Ollama running as the fallback.
"""

from __future__ import annotations

import argparse
import json
import sys

from sportsbot_agent.config import AgentConfig
from sportsbot_agent.main import format_answer, validate_question
from sportsbot_agent.rag import answer_to_dict, build_rag


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ask SportsBot (hybrid RAG)")
    parser.add_argument("question", nargs="+")
    parser.add_argument("--json", action="store_true", help="print the API-shaped JSON")
    parser.add_argument("--no-llm", action="store_true", help="retrieval only: show excerpts")
    args = parser.parse_args(argv)
    config = AgentConfig.from_env()
    question, error = validate_question(" ".join(args.question), config.max_question_chars)
    if error:
        print(error, file=sys.stderr)
        return 2
    rag = build_rag(config, use_llm=not args.no_llm)
    try:
        answer, meta = rag.ask(question)
    finally:
        rag.close()
    if args.json:
        print(json.dumps(answer_to_dict(answer, meta), ensure_ascii=False, indent=2))
    else:
        print(format_answer(answer, meta["seconds"]))
        print(
            f"retrieval: {json.dumps(meta['retrieval'], ensure_ascii=False)} | llm: {meta['llm']}"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
