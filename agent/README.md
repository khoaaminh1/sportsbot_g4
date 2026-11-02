# agent

Query understanding, retrieval, evidence checks, prompts and the LLM client. Owner: TMK.

## Status
| Part | Status |
|---|---|
| Terminal chat prototype (BM25 over exported Bronze files): answers only from stored articles, with links | Implemented (prototype) |
| Hybrid RAG (PostgreSQL + Qdrant + reranker), free Gemini models in rotation (`make ask`) | Skeleton, tested |
| Keyword retrieval (BM25, accent-insensitive) | Implemented (baseline) |
| Query understanding, time parsing | Planned |

How to run, configure and change the model: [docs/runbooks/chat-prototype.md](../docs/runbooks/chat-prototype.md). Decisions: [ADR 0006](../docs/decisions/0006-llm-gemini-model-rotation.md), [ADR 0007](../docs/decisions/0007-bge-m3-and-reranker-gguf.md).
