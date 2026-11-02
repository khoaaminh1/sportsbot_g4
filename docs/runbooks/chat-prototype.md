# Runbook: terminal chat prototype (BM25, answers only from stored articles)

The prototype answers sports questions **only from articles SportsBot has collected**, and lists the articles it used with their links and publication times. If no stored article matches, it says so and does not call the model. Code: `agent/src/sportsbot_agent/`. Decisions: [ADR 0004](../decisions/0004-local-llm-ollama.md) (grounding rules), [ADR 0006](../decisions/0006-llm-gemini-model-rotation.md) (Gemini models). The full RAG is `make ask` ([streaming-stack runbook](streaming-stack.md)).

## How it works
1. Load stored articles (Bronze records exported from HDFS) from `data/articles/`.
2. Keyword search (BM25, accent-insensitive: `doi tuyen` matches `đội tuyển`; aliases such as `VN` → `viet nam`).
3. No match → "No matching article…" (the model is **not** called).
4. Match → the top 5 articles go to the model with strict rules: use only these articles, cite every fact as `[n]`, no URLs, no invented scores or names, answer `INSUFFICIENT_EVIDENCE` if the articles do not answer.
5. The answer is checked: at least one citation, all citation numbers valid, no URLs. Otherwise the program shows the article excerpts instead.
6. Print the answer and the cited sources: title, source, publication time (UTC), link.

| Module | Role |
|---|---|
| `config.py` | Settings from environment variables (below) |
| `clients/llm_router.py` | Free Gemini models tried in turn (OpenAI-compatible API) |
| `store.py` | Loads stored articles (JSON Lines, plain or gzip) |
| `retrieval.py` | Accent-insensitive BM25 keyword search |
| `answer.py` | Grounded answer, citation check, fallbacks |
| `main.py` | Terminal program (interactive or one question) |

## Run the prototype
Put `GEMINI_API_KEY` in `.env` (without it, the prototype shows excerpts only). From the
repository root, with the virtual environment active:

```bash
make setup                 # once
make up-collect            # PostgreSQL + HDFS + collector (or skip if already collecting)
make collect-once          # collect now instead of waiting for the scheduler
make articles-export       # copy stored articles from HDFS to data/articles/
make chat                  # interactive chat; type /exit to quit
```

One question: `PYTHONPATH=agent/src python -m sportsbot_agent.main "Tin mới nhất về đội tuyển Việt Nam?"`.
Without the model (matching excerpts only): `make chat-no-llm`.

## Settings
| Variable | Default | Meaning |
|---|---|---|
| `GEMINI_API_KEY` | empty | Free key from https://aistudio.google.com/apikey |
| `GEMINI_MODELS` | see `.env.example` | Models tried in order |
| `LLM_TIMEOUT_SECONDS` | `60` | Wait limit per answer |
| `LLM_MAX_TOKENS` | `600` | Answer length limit |
| `ARTICLES_DIR` | `data/articles` | Folder with stored articles |
| `RETRIEVAL_TOP_K` | `5` | Articles given to the model |

## Why grounding (measured 2026-11-02 with the earlier local model)
Asked directly, without articles, the local `llama3` 8B answered fluent but **wrong** facts about the Vietnam national team (a World Cup goal that never happened; a midfielder called a goalkeeper). This is why every model only sees stored articles and must cite them.

## Known limitations
- **Stopgap data path:** articles are exported from HDFS Bronze to a local file; Phase 3b/5 replace this with Gold tables in PostgreSQL and real indexes.
- **Keyword search only:** no semantic or cross-language search yet (a Vietnamese question finds Vietnamese articles, not English ones). Phase 4 adds multilingual vectors (hybrid retrieval).
- **Short evidence:** RSS summaries are short; some questions get `INSUFFICIENT_EVIDENCE` even when the full article would answer.
- **Citations are checked for form, not truth:** a cited sentence can still misstate its article. Faithfulness evaluation is a Phase 4 task.
- **No conversation memory** and no time understanding ("today", "last week") yet.
