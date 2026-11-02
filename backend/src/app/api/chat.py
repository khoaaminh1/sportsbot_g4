"""Chat endpoints: JSON (`POST /chat`) and Server-Sent Events (`POST /chat/stream`).

Answers come only from stored articles (agent/answer.py): no matching article -> the bot says so
and the LLM is not called; generated answers must cite [n] or they are replaced by excerpts.

Streaming note: the answer is checked for valid citations *before* any of it is shown, so the
stream sends progress events (`status`), then the checked answer in pieces (`delta`), then
`citations` and `done`. It does not stream raw model tokens that might fail the check.
"""

from __future__ import annotations

import json
import logging
import uuid
from collections.abc import AsyncIterator
from typing import Any

from fastapi import APIRouter, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.core.errors import AppError

logger = logging.getLogger(__name__)

router = APIRouter(tags=["chat"])


class ChatRequest(BaseModel):
    message: str = Field(min_length=1, max_length=2000)
    conversation_id: uuid.UUID | None = None
    language: str | None = Field(default=None, pattern="^[a-z]{2}$")


def _rag(request: Request) -> Any:
    rag = request.app.state.rag
    if rag is None:
        raise AppError("chat_unavailable", "The chat service is not configured", 503)
    return rag


def to_chat_response(result: dict[str, Any], conversation_id: uuid.UUID) -> dict[str, Any]:
    """Map the agent's answer dict to the ChatResponse contract (plus mode/meta extras)."""
    return {
        "conversation_id": str(conversation_id),
        "answer": result["answer"],
        "language": result["language"],
        "citations": [
            {
                "index": c["number"],
                "article_id": c["article_id"],
                "title": c["title"],
                "source_name": c["source_name"],
                "source_url": c["url"],
                "published_at": c["published_at"],
            }
            for c in result["citations"]
        ],
        "insufficient_evidence": result["mode"] in ("no_evidence", "insufficient"),
        "conflicting_sources": False,
        "clarification_question": None,
        "mode": result["mode"],
        "notice": result["notice"],
        "meta": result["meta"],
    }


async def _answer(request: Request, body: ChatRequest) -> dict[str, Any]:
    rag = _rag(request)
    question = " ".join(body.message.split())
    if not any(c.isalnum() for c in question):
        raise AppError("invalid_question", "The question must contain letters or numbers", 422)
    from sportsbot_agent.memory import Turn
    from sportsbot_agent.rag import answer_to_dict

    store = request.app.state.conversation_store
    history: list[Turn] = []
    if store is not None and body.conversation_id is not None:
        try:
            pairs = await run_in_threadpool(store.history, str(body.conversation_id))
            history = [Turn(q, a) for q, a in pairs]
        except Exception:  # memory is best effort; answer the question on its own
            logger.exception("conversation_history_failed")
    answer, meta = await run_in_threadpool(rag.ask, question, history)
    response = to_chat_response(answer_to_dict(answer, meta), body.conversation_id or uuid.uuid4())
    if store is not None:
        try:
            await run_in_threadpool(store.save, question, response)
        except Exception:  # history is best effort; never fail the answer because of it
            logger.exception("conversation_save_failed")
    return response


@router.post("/chat", summary="Ask a question; answer grounded in stored articles with citations")
async def chat(request: Request, body: ChatRequest) -> dict[str, Any]:
    return await _answer(request, body)


def _sse(event: str, data: Any) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


@router.post("/chat/stream", summary="Same as /chat, as Server-Sent Events")
async def chat_stream(request: Request, body: ChatRequest) -> StreamingResponse:
    _rag(request)  # fail fast with a JSON error when chat is not configured

    async def events() -> AsyncIterator[str]:
        yield _sse("status", {"stage": "retrieving"})
        try:
            response = await _answer(request, body)
        except AppError as exc:
            yield _sse("error", {"code": exc.code, "message": exc.message})
            return
        except Exception:
            logger.exception("chat_stream_failed")
            yield _sse("error", {"code": "internal_error", "message": "Unexpected error"})
            return
        yield _sse("status", {"stage": "answering", "mode": response["mode"]})
        text = response["answer"]
        for start in range(0, len(text), 60):
            yield _sse("delta", {"text": text[start : start + 60]})
        yield _sse("citations", response["citations"])
        yield _sse("done", {k: v for k, v in response.items() if k not in ("answer", "citations")})

    return StreamingResponse(
        events(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
