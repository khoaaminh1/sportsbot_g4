"""Save chat turns in `conversations` / `messages` (001_init.sql)."""

from __future__ import annotations

import json
from typing import Any

from sqlalchemy import Engine, text


class ConversationStore:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def history(self, conversation_id: str, turns: int = 4) -> list[tuple[str, str]]:
        """The last `turns` (question, answer) pairs of a conversation, oldest first."""
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT role, content FROM messages WHERE conversation_id = :id "
                    "ORDER BY created_at DESC, message_id DESC LIMIT :n"
                ),
                {"id": conversation_id, "n": turns * 2},
            ).all()
        pairs: list[tuple[str, str]] = []
        pending: str | None = None  # assistant reply waiting for its question (newest first)
        for role, content in rows:
            if role == "assistant":
                pending = content
            elif pending is not None:
                pairs.append((content, pending))
                pending = None
        return pairs[::-1]

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
