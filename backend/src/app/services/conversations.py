"""Save chat turns in `conversations` / `messages` (001_init.sql)."""

from __future__ import annotations

import json
from typing import Any

from sqlalchemy import Engine, text


class ConversationStore:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def save(self, question: str, response: dict[str, Any]) -> None:
        cid = response["conversation_id"]
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO conversations (conversation_id, language) VALUES (:id, :lang) "
                    "ON CONFLICT (conversation_id) DO UPDATE SET updated_at = now()"
                ),
                {"id": cid, "lang": response["language"]},
            )
            conn.execute(
                text(
                    "INSERT INTO messages (conversation_id, role, content) VALUES (:id, 'user', :c)"
                ),
                {"id": cid, "c": question},
            )
            conn.execute(
                text(
                    "INSERT INTO messages (conversation_id, role, content, citations) "
                    "VALUES (:id, 'assistant', :c, CAST(:cit AS jsonb))"
                ),
                {"id": cid, "c": response["answer"], "cit": json.dumps(response["citations"])},
            )
