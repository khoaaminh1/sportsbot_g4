# Runbook: terminal chat prototype (Ollama, answers only from stored articles)

The prototype answers sports questions **only from articles SportsBot has collected**, and lists the articles it used with their links and publication times. If no stored article matches, it says so and does not call the model. Code: `agent/src/sportsbot_agent/`. Decision: [ADR 0004](../decisions/0004-local-llm-ollama.md).

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
| `llm_client.py` | OpenAI-compatible client (Ollama), clear errors: server down, model missing, timeout |
| `store.py` | Loads stored articles (JSON Lines, plain or gzip) |
| `retrieval.py` | Accent-insensitive BM25 keyword search |
| `answer.py` | Grounded answer, citation check, fallbacks |
| `main.py` | Terminal program (interactive or one question) |

## Requirements (measured on TMK's Mac, 2026-11-02)
| Item | Value |
|---|---|
| Machine | MacBook with Apple M2 Pro (10 CPU cores, 16 GPU cores), 16 GB RAM, macOS 26.5.2 |
| Python | 3.11.4 |
| Ollama | 0.40.13, running as the Ollama app (`ollama serve`) |
| Model | `llama3:latest` (Llama 3, 8B, Q4_0, 4.7 GB on disk) |
| Speed | 37 tokens/s; first request 10.2 s including 5.6 s model load, next one 2.3 s |

Any machine that runs Ollama works: macOS (Apple Silicon or Intel), Windows, Linux. An 8B model needs about 5-6 GB of free memory (estimate); a 3B model about 3 GB.

## Install Ollama
- **macOS / Windows:** download the installer from https://ollama.com/download and start the app. It serves on `http://localhost:11434`.
- **Check:** `ollama --version` and `curl http://localhost:11434/api/version`.
- **Download a model** (needs approval in this project because of size): `ollama pull llama3` (4.7 GB).
- **List models:** `ollama list`.

## Run the prototype
From the repository root, with the virtual environment active (`source .venv/bin/activate`; on Windows `.venv\Scripts\activate`):

```bash
make setup                 # once: installs agent/requirements.txt (httpx) too
make up-collect            # PostgreSQL + HDFS + collector (or skip if already collecting)
make collect-once          # collect now instead of waiting for the scheduler
make articles-export       # copy stored articles from HDFS to data/articles/
make chat                  # interactive chat; type /exit to quit
```

One question: `PYTHONPATH=agent/src python -m sportsbot_agent.main "Tin mới nhất về đội tuyển Việt Nam?"`.
Without the model (shows matching excerpts only): `make chat-no-llm`.
Windows PowerShell (no make): `$env:PYTHONPATH="agent/src"; python -m sportsbot_agent.main`.

## Change the model or server
All settings are environment variables; nothing is hard-coded for one machine.

| Variable | Default | Meaning |
|---|---|---|
| `LLM_BASE_URL` | `http://localhost:11434/v1` | OpenAI-compatible endpoint (Ollama on this machine) |
| `LLM_MODEL` | `llama3:latest` | Any model shown by `ollama list` |
| `LLM_TIMEOUT_SECONDS` | `120` | Wait limit per answer |
| `LLM_MAX_TOKENS` | `400` | Answer length limit |
| `LLM_TEMPERATURE` | `0.1` | Low = more literal answers |
| `ARTICLES_DIR` | `data/articles` | Folder with stored articles |
| `RETRIEVAL_TOP_K` | `5` | Articles given to the model |

Example: `LLM_MODEL=qwen2.5:7b make chat`. To use the Windows PC's Ollama from the Mac: `LLM_BASE_URL=http://<pc-ip>:11434/v1` (Ollama must be set to listen on the network on the PC).

## Measured model behaviour (2026-11-02, before grounding)
Asked directly, without articles, `llama3` answered fluent but **wrong** facts about the Vietnam national team: it claimed a Vietnamese player scored at a World Cup (Vietnam has never played one) and listed a midfielder as a goalkeeper. English answers were broadly correct. This is why the prototype answers only from stored articles and checks citations.

## Known limitations
- **Stopgap data path:** articles are exported from HDFS Bronze to a local file; Phase 3b/5 replace this with Gold tables in PostgreSQL and real indexes.
- **Keyword search only:** no semantic or cross-language search yet (a Vietnamese question finds Vietnamese articles, not English ones). Phase 4 adds multilingual vectors (hybrid retrieval).
- **Short evidence:** RSS summaries are short; some questions get `INSUFFICIENT_EVIDENCE` even when the full article would answer.
- **Citations are checked for form, not truth:** a cited sentence can still misstate its article. Faithfulness evaluation is a Phase 4 task.
- **Model quality:** `llama3` writes weak Vietnamese; compare models (for example `qwen2.5:7b`) with a decision rule fixed before the run.
- **No conversation memory** and no time understanding ("today", "last week") yet.
