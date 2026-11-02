"""Check which configured Gemini models answer right now with the key in .env.

    make llm-check

Sends one tiny request per model (a few tokens; free-tier quota cost is negligible) and prints
one line per model. Remove models that fail from GEMINI_MODELS in .env.
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
        config.gemini_api_key, config.gemini_base_url, list(config.gemini_models)
    )
    if not targets:
        print("GEMINI_API_KEY is empty: put your key in .env (https://aistudio.google.com/apikey).")
        return 2
    working = 0
    for target in targets:
        router = ChatRouter([target], max_tokens=5, timeout=30)
        try:
            reply = router.chat([Message("user", "Reply with the single word: ok")])
            status = {"model": target.model, "ok": True, "reply": reply[:20]}
            working += 1
        except LLMError as exc:
            status = {"model": target.model, "ok": False, "error": str(exc)[:160]}
        print(json.dumps(status, ensure_ascii=False), flush=True)
    print(json.dumps({"summary": {"models": len(targets), "working": working}}))
    return 0 if working else 1


if __name__ == "__main__":
    sys.exit(main())
