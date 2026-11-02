"""Check which configured chat models answer right now (Gemini models, then Ollama).

    make llm-check

Sends one tiny request per target (a few tokens; free-tier quota cost is negligible) and prints
one line per target. Use it after setting GEMINI_API_KEY, and when answers fall back to Ollama.
"""

from __future__ import annotations

import json
import sys

from sportsbot_agent.clients.llm_router import ChatRouter, build_targets
from sportsbot_agent.config import AgentConfig
from sportsbot_agent.llm_client import LLMError, Message


def main() -> int:
    config = AgentConfig.from_env()
    targets = build_targets(
        config.gemini_api_key,
        config.gemini_base_url,
        list(config.gemini_models),
        config.llm_base_url,
        config.llm_model,
        config.llm_use_ollama_fallback,
    )
    if not targets:
        print("No targets: set GEMINI_API_KEY in .env or enable LLM_USE_OLLAMA_FALLBACK.")
        return 2
    if not config.gemini_api_key:
        print("GEMINI_API_KEY is empty: only Ollama is checked.")
    working = 0
    for target in targets:
        router = ChatRouter([target], max_tokens=5, timeout=30)
        try:
            reply = router.chat([Message("user", "Reply with the single word: ok")])
            status = {"target": target.name, "ok": True, "reply": reply[:20]}
            working += 1
        except LLMError as exc:
            status = {"target": target.name, "ok": False, "error": str(exc)[:160]}
        print(json.dumps(status, ensure_ascii=False), flush=True)
    print(json.dumps({"summary": {"targets": len(targets), "working": working}}))
    return 0 if working else 1


if __name__ == "__main__":
    sys.exit(main())
