# Runbook: the chatbot (RAG), its settings, and the legacy prototype

The chatbot answers sports questions **only from articles SportsBot has collected** and lists the
articles it used with their links and publication times. If no stored article matches, it says so
and does not call the model. Code: `agent/src/sportsbot_agent/`. Decisions:
[ADR 0006](../decisions/0006-llm-gemini-model-rotation.md) (Gemini models),
[ADR 0007](../decisions/0007-bge-m3-and-reranker-gguf.md) (embedding and reranker).
The grounding rules first written in [ADR 0004](../decisions/0004-local-llm-ollama.md) still apply.

## How an answer is made
1. **Follow-up?** If the conversation has earlier turns, the question is rewritten into a
   standalone search query (see "Conversation memory").
2. **Keyword search** in PostgreSQL over unaccented title and text ("doi tuyen" finds "đội tuyển").
3. **Vector search** in Qdrant over 1,200-character chunks embedded with BGE-M3.
4. **Fusion** with Reciprocal Rank Fusion, then **bge-reranker-v2-m3** keeps the best
   `RERANK_TOP` candidates above `RERANK_MIN_SCORE`. Nothing left → "no evidence", no model call.
5. The top `RETRIEVAL_TOP_K` articles go to Gemini with strict rules: use only these articles,
   cite every fact as `[n]`, no URLs, no invented scores or names, answer `INSUFFICIENT_EVIDENCE`
   if they do not answer.
6. The answer is checked (at least one valid citation, no URLs); otherwise the article excerpts
   are shown instead. Sources are returned with title, outlet, publication time and link.

## Run
- Web app: `make up-demo` (crawler PC) or `make team-data` / `make up-team` (teammates), then
  http://localhost:8000/app/.
- Terminal: `make ask Q="Tin tennis mới nhất?"`; without the model: `make ask-no-llm Q="..."`.
- Which Gemini models answer with your key: `make llm-check`.

## Settings (`.env`, defaults from `.env.example`)
| Variable | Default | Meaning |
|---|---|---|
| `GEMINI_API_KEY` | empty | Free key from https://aistudio.google.com/apikey; without it the bot shows excerpts |
| `GEMINI_MODELS` | `gemini-3.8-flash,gemini-3.6-flash,gemini-3.5-flash-lite,gemini-3.1-flash-lite` | Tried in order; a model that hits its quota rests |
| `LLM_REASONING_EFFORT` | `low` | Thinking budget sent to Gemini (retried without it if refused) |
| `LLM_COOLDOWN_SECONDS` | `65` | Rest after a per-minute quota error (daily quota: until Pacific midnight) |
| `LLM_TIMEOUT_SECONDS` | `30` | Wait limit per model |
| `LLM_MAX_TOKENS` | `1500` | Answer length limit |
| `LLM_TEMPERATURE` | `0.1` | Low: factual answers |
| `RETRIEVAL_CANDIDATES` | `30` | Candidates from each search before fusion |
| `RERANK_TOP` | `12` | Candidates kept after reranking |
| `RERANK_MIN_SCORE` | `-2.0` | Reranker score below which a candidate is dropped |
| `RETRIEVAL_TOP_K` | `5` | Articles given to the model |

## Conversation memory (follow-up questions)
Send the same `conversation_id` with every message of a conversation (the web app does this).
The backend loads the last 4 question/answer pairs from `messages` and the agent
(`agent/src/sportsbot_agent/memory.py`) rewrites a follow-up such as "Anh ấy bao nhiêu tuổi?"
into a standalone search query ("Wayne Rooney bao nhiêu tuổi") before retrieval. The rewrite
costs one extra Gemini call, only when there is history. The answer prompt shows the earlier
turns as context, never as a source: facts and citations still come only from retrieved
articles. If the rewrite fails, the previous question is prepended to the new one instead.
`meta.search_query` in the response shows the rewritten query (null when unchanged).

## Known limitations (2026-11-04)
- **Latency:** about 20-50 s per answer on the crawler PC (CPU embedding and reranking plus
  Gemini); being measured (TMK, `docs/TASKS.md`).
- **No time understanding yet:** "hôm nay", "tuần này", "mới nhất" are not turned into date filters.
- **Opinion and aggregate questions** ("4 trận hay nhất năm 2026") usually find no evidence: the
  corpus holds news reports, not rankings.
- **Citations are checked for form, not truth:** a cited sentence can still misstate its article;
  faithfulness is part of the evaluation (plan task 8).
- **BBC articles** contain only the feed summary (pages blocked from the crawler's network).

## Legacy: the terminal prototype (BM25 over exported files)
The first prototype (PR #1, Phase 3a) is kept for reference: `make chat` / `make chat-no-llm`
search Bronze records exported to `data/articles/` with accent-insensitive BM25
(`retrieval.py`, `store.py`, `main.py`) and use the same grounding rules and Gemini router.
It needs the legacy collector (`make up-collect`, `make collect-once`, `make articles-export`).
Measured 2026-11-02 with the earlier local model: asked directly without articles, `llama3` 8B
gave fluent but **wrong** facts about the Vietnam national team, which is why every model only
sees stored articles and must cite them.
