# agent

Question answering: hybrid retrieval, reranking, conversation memory, grounded answers with
citations, and the Gemini router. Owner: TMK. Used by the backend (`/chat`, `/chat/stream`) and
the terminal (`make ask`).

## Status (2026-11-04)
| Part | Status |
|---|---|
| Hybrid retrieval: PostgreSQL full-text (accent-insensitive) + Qdrant vectors (BGE-M3), RRF fusion | Running |
| Reranking with bge-reranker-v2-m3, weak candidates dropped | Running |
| Grounded answers: only retrieved articles, `[n]` citations checked, excerpts as fallback | Running |
| Free Gemini models in rotation with per-model cooldown | Running |
| Conversation memory: follow-ups rewritten into standalone queries | Merged and unit-tested; live check pending |
| Time understanding ("hôm nay", "mới nhất") | Planned |
| BM25 terminal prototype over exported files (`main.py`, `make chat`) | Legacy, kept for reference |

Layout: `rag.py` (wiring), `hybrid.py` (retrieval), `answer.py` (prompt and checks), `memory.py`
(follow-ups), `clients/` (embeddings, reranker, Qdrant, LLM router), `config.py` (settings).
Settings and operation: [docs/runbooks/chat-prototype.md](../docs/runbooks/chat-prototype.md).
Decisions: [ADR 0006](../docs/decisions/0006-llm-gemini-model-rotation.md),
[ADR 0007](../docs/decisions/0007-bge-m3-and-reranker-gguf.md).
