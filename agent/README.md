# agent

Query understanding, retrieval, evidence checks, prompts and the LLM client. Owner: TMK.

## Status
| Part | Status |
|---|---|
| Terminal chat prototype: answers only from stored articles, with links; Ollama via OpenAI-compatible API | Implemented (prototype) |
| Keyword retrieval (BM25, accent-insensitive) | Implemented (baseline) |
| Hybrid retrieval with multilingual vectors, query understanding, time parsing | Planned (Phase 4) |

How to run, configure and change the model: [docs/runbooks/chat-prototype.md](../docs/runbooks/chat-prototype.md). Decision: [ADR 0004](../docs/decisions/0004-local-llm-ollama.md).
