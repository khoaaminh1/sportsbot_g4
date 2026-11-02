# Team onboarding: run SportsBot on your machine (VNP, LTN)

Status: 2026-11-03, 01:45 ICT. Presentation: **Tuesday 10 November 2026**.
Plan with daily gates and tasks: [docs/plans/2026-11-02-plan-to-presentation.md](plans/2026-11-02-plan-to-presentation.md).

## 1. Where the project is

Running 24/7 on TMK's home PC (Windows 11 + WSL2 Ubuntu 24.04 + Docker Desktop, i5-14400F, 32 GB):

| Measured on 2026-11-03 01:41 ICT | Value |
|---|---|
| Articles stored (full text, PostgreSQL) | 963 |
| Text chunks with BGE-M3 vectors (Qdrant) | 2,966 |
| Sources enabled | 20 of 23 (vi, en, es, fr); 2 disabled (HTTP 404), BBC = feed summaries only |
| First chat answers | Correct, cited (VnExpress, Dân trí, VOV, Thanh Niên); about 20 s per answer |
| Monitor | Running; caught the two 404 feeds on its first check |

Pipeline (team diagram, ADR 0005): RSS → Kafka `articles.discovered` → extractor (fetch + extract +
dedup) → `articles.extracted` → PostgreSQL + Qdrant (BGE-M3) → hybrid RAG with bge-reranker →
free Gemini models → FastAPI → demo page. A third consumer archives full text for Spark.

Not done yet (the plan's tasks): Spark Silver/Gold + near-duplicates (VNP), classifier (VNP),
golden questions + labels + source checks (LTN), retrieval evaluation (TMK), slides (all).

## 2. Who does what this week

| Member | Tasks (plan) | Needs the full stack? |
|---|---|---|
| TMK | 1, 3, 8, 10 | Yes (the shared crawler) |
| VNP | 6 Spark Silver/Gold, 7 classifier, 9 scaling | Yes, plus a data snapshot from TMK (section 5) |
| LTN | 2 sources, 4 golden questions, 5 labels, 10 slides | Light: Docker for `make sources-check`; CSV files from TMK |

**Only TMK's PC crawls 24/7.** If you start the stack on your machine for development, it
crawls too; stop it when you are done (`make down`) so sites are not hit twice.

## 3. Install (once)

### Windows (WSL2)
1. PowerShell **as administrator**: `wsl --install -d Ubuntu-24.04`, restart, create the Ubuntu
   user and password (write the password down; reset it with `wsl -d Ubuntu-24.04 -u root` then
   `passwd <user>` if forgotten).
2. Install Docker Desktop (`winget install -e --id Docker.DockerDesktop`), restart, open it:
   Settings → General → "Use the WSL 2 based engine"; Resources → WSL integration → enable
   Ubuntu-24.04 → Apply & restart.
3. Optional, 16+ GB RAM: `"[wsl2]`nmemory=12GB`nprocessors=6" | Out-File -Encoding ascii "$env:USERPROFILE\.wslconfig"` then `wsl --shutdown`.
4. VS Code: install the **WSL** extension. From now on work in Ubuntu.

### macOS
Docker Desktop or OrbStack, Python 3.11+, Git, make (Xcode command line tools).

### Know where you are
| Prompt | Where | Use for |
|---|---|---|
| `PS C:\...>` | PowerShell (Windows) | only `wsl -d Ubuntu-24.04` to enter Ubuntu |
| `user@PC:~/sportsbot_g4$` | Ubuntu | **every project command** (`make`, `git`, `docker`) |

## 4. Get the code and run

In Ubuntu (clone into your Linux home, **not** under `/mnt/c`):
```bash
cd ~
git clone https://github.com/khoaaminh1/sportsbot_g4.git
cd sportsbot_g4
python3 -m venv .venv && source .venv/bin/activate
cp .env.example .env
code .env        # set GEMINI_API_KEY (your own free key: https://aistudio.google.com/apikey)
make setup && make test          # expect "passed", 0 failed
```
Never share `.env` or screenshots of it. Each person uses **their own** Gemini key.

Full stack (about 4-5 GB RAM, first start 10-20 min):
```bash
make models          # ~1.2 GB GGUF files, once
make up-demo         # PostgreSQL, Qdrant, Kafka, workers, monitor, embedder, reranker, backend
make ps              # all running / healthy
make llm-check       # which Gemini models answer with your key
make crawl-stats     # articles per source, vectors
make ask Q="Arsenal thắng Liverpool như thế nào?"
```
Demo page: http://localhost:8000/app/ · API docs: http://localhost:8000/docs · Stop: `make down`.

## 5. Use TMK's data instead of crawling yourself

TMK exports (on the crawler PC):
```bash
make db-export       # data/snapshots/sportsbot-<date>.sql.gz  (articles, sources, runs)
make bronze-export   # data/snapshots/bronze-<date>.tgz        (archived full text, for Spark)
```
and shares the files (Google Drive or a private Kaggle Dataset).

You import (stack started with `make up-demo`, then):
```bash
docker compose --profile core --profile stream stop discover extractor consumer-operational consumer-semantic consumer-archive
make db-import FILE=data/snapshots/sportsbot-<date>.sql.gz    # replaces your local articles
make reindex          # builds the Qdrant vectors from PostgreSQL (CPU: a few minutes per 1,000)
```
For Spark (VNP): unpack the Bronze archive into the `bronze_data` volume or read it directly:
`mkdir -p data/bronze && tar xzf data/snapshots/bronze-<date>.tgz -C data/bronze`; files are
gzip JSON Lines under `source=extracted/ingest_date=*/ingest_hour=*/`, one article per line
(fields: see `pipelines/src/sportsbot_pipelines/extraction/worker.py::build_extracted`).

LTN's labeling file: TMK runs `make export-labeling N=400` and sends
`data/labeling/sample.csv` (open in Google Sheets; instructions in the plan, task 5).

## 6. Everyday commands

| Command | What it does |
|---|---|
| `make ps` / `make logs-stream` | service status / follow the workers |
| `make crawl-stats` | articles per source + vectors |
| `make monitor-once` | health check now (failing sources, stalls) |
| `make dead-letters` | why articles were rejected |
| `make replay-dead-letters` | retry network/429/5xx failures |
| `make reindex` | build missing vectors |
| `make sources-check` | fetch every feed once, print a table (LTN task 2) |
| `make sources-sitemaps` | sitemap URLs from robots.txt (for backfill) |
| `make backfill SINCE=2026-10-01 MAX=3000` | older articles from sitemaps (TMK's PC only) |
| `make ask Q="..."` | chatbot in the terminal |

## 7. Problems we already hit (and the fix)

| Symptom | Fix |
|---|---|
| `make: command not found` / `source: not recognized` | You are in PowerShell. `wsl -d Ubuntu-24.04`, `cd ~/sportsbot_g4` |
| `python: Permission denied` | `source .venv/bin/activate` (fixed in the Makefile since PR #11) |
| `.venv/bin/activate: No such file` | `rm -rf .venv && python3 -m venv .venv` |
| `sudo`: wrong password 3 times | Reset from PowerShell: `wsl -d Ubuntu-24.04 -u root`, `passwd <user>` |
| `docker` not found in PowerShell | Normal: use Docker from Ubuntu (`wsl -d Ubuntu-24.04 -- docker version`) |
| Embedder HTTP 500 `unordered_map::at` | Old model file: `git pull`, `make models`, `make up-ai`, `make reindex` |
| `llm-check`: 503 / timeout on a model | Google overloaded; the router uses the next model |
| BBC pages: connection reset | Blocked by the network in Vietnam; BBC is summary-only by design |
| `qdrant_chunks` not growing | `docker compose --profile core --profile stream logs --tail 20 consumer-semantic` |

## 8. Rules

- Work on a branch, open a pull request, wait for both CI checks; **never tick "bypass rules"**.
- No secrets in git, chat or screenshots (`.env`, API keys, webhook URLs).
- Never disable robots.txt or the 3-second per-site delay; never work around a block.
- Numbers in the report come from real runs, with the command and date. No invented results.
- Code, commits and docs in English.
