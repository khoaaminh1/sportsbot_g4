# Handoff: VNP (Big Data layer and classifier)

For VNP and VNP's AI assistant. Read this file, then `AGENTS.md`, `docs/DATA.md`,
`docs/TEAM_ONBOARDING.md` and tasks 6, 7, 9 of
`docs/plans/2026-11-02-plan-to-presentation.md`.

## Your outcomes before Saturday 7 November (presentation Tuesday 10 November)
1. **Task 6 – Spark Silver/Gold** on HDFS from the Bronze snapshot: clean, exact + near-duplicate
   detection (MinHashLSH), story clusters, Gold statistics, a JSON run report per run.
2. **Task 7 – Sport classifier**: baseline (Spark MLlib TF-IDF + logistic regression) vs improved
   (BGE-M3 vectors + logistic regression), evaluated on LTN's hand labels only.
3. **Task 9 – Scaling runs**: the Silver job at increasing input sizes, `local[*]` vs the
   2-worker standalone cluster, with the measured table.

## Set up (once)
Follow `docs/TEAM_ONBOARDING.md` sections 3-4 (WSL2 + Docker on Windows, or macOS), then:
```bash
make kaggle-pull               # needs KAGGLE_API_TOKEN + KAGGLE_DATASET in .env (docs/DATA.md)
make up-bigdata                # HDFS NameNode + 2 DataNodes + Spark master + worker
make spark-smoke               # must print "ok": true
make bronze-load FILE=data/kaggle/bronze.tgz
make hdfs-ls                   # /sportsbot/bronze/source=extracted/... present
```
You do not need the chatbot stack for tasks 6 and 9. For task 7's improved model you need
BGE-M3 vectors: `make models && make up-ai`, then call `http://localhost:8091/v1/embeddings`
(see `agent/src/sportsbot_agent/clients/embeddings.py`; send single-line text).

## Task 6 specification

**Files to create**
- `pipelines/spark/jobs/silver_articles.py` — Bronze `source=extracted` → Silver Parquet
- `pipelines/spark/jobs/gold_stats.py` — Silver → Gold statistics Parquet
- `pipelines/spark/jobs/textnorm.py` — pure-Python helpers (normalise text, word 3-shingles),
  importable without Spark so they can be unit-tested
- `pipelines/tests/unit/test_textnorm.py`
- run reports: `docs/reports/runs/silver-<run_id>.json`, `gold-<run_id>.json`

**Rules fixed before any run (do not change them after seeing results)**
- Input: `hdfs://namenode:8020/sportsbot/bronze/source=extracted/*/*/*.jsonl.gz`
- Clean: Unicode NFC, collapse whitespace, drop rows without `text` or `title`; keep the
  original columns; add `ingest_date` from `extracted_at`.
- Exact duplicates: same `content_hash` → keep the earliest `extracted_at`.
- Near duplicates: `MinHashLSH` (numHashTables = 5) on word 3-shingles of lower-cased,
  accent-preserving text; a pair with **Jaccard similarity ≥ 0.8** is the same story.
  `story_cluster_id` = smallest `article_id` in the connected group (or the article itself).
- Output: `hdfs://namenode:8020/sportsbot/silver/articles/` Parquet, partitioned by
  `language` and `ingest_date`, plus rejected rows with a `reason` under `silver/_rejected/`.
- Gold: counts per source × day × language, duplicate rate, cluster sizes, freshness lag
  (`extracted_at - published_at`) percentiles → `hdfs://namenode:8020/sportsbot/gold/stats/`.
- The Spark image runs **Python 3.10**: no `datetime.UTC` (use `timezone.utc`), no 3.11 syntax.
- Run report fields: `run_id`, `snapshot_file`, `input_rows`, `output_rows`, `rejected_rows`,
  `exact_duplicates`, `near_duplicate_pairs`, `clusters`, `duration_seconds`, `master`
  (`local[*]` or `spark://spark-master:7077`), `started_at`.

**Done when** `spark-submit` on the cluster writes Silver and Gold, the run report is
committed, unit tests pass (`make lint test`), and the report numbers can be reproduced by
re-running the same command on the same snapshot.

## Task 7 specification (short)
- `ml/train_sport_classifier.py`, `ml/README.md` (model card), `docs/reports/runs/classifier-*.json`.
- Training labels: weak labels from URL paths (`/bong-da/`, `/football/` → football; write the
  full mapping in the model card). Test set: LTN's hand labels **only**, never used for training.
- Report macro-F1, per-class and per-language F1, confusion matrix; baseline vs improved.

## Task 9 specification (short)
- Same Silver job at 3 input sizes. Real corpus first; a larger public Vietnamese news corpus
  only if labelled "public historical corpus, not collected by SportsBot".
- Both masters (`local[*]`, cluster). Table: rows, duration, rows/s, storage per layer.
- Say "single machine, pseudo-distributed (Docker)"; never "production cluster".

## Working rules
- Branch per task (`feature/vnp-silver-job`), pull request to `main`, both CI checks green; never
  tick "bypass rules". TMK reviews and merges.
- Do not edit `agent/`, `backend/`, `frontend/`, `docker-compose.yml`, `.github/` without asking TMK.
- No data files in git (see `docs/DATA.md`); only code, tests and run reports.
- No invented numbers: every figure comes from a run report produced by your code.

## Prompt for VNP's AI assistant (Claude Code / Cursor / Copilot)
```
You are helping VNP on the SportsBot project (university Big Data course, presentation
10 November 2026). Explain to me in Vietnamese; write code, comments, commits in English.
Read first: docs/handoff/VNP.md, AGENTS.md, docs/DATA.md, docs/TEAM_ONBOARDING.md.
Task now: Task 6 in docs/handoff/VNP.md (Spark Silver/Gold). Follow its fixed rules exactly.
Work test-first for the pure-Python helpers. Keep Spark code compatible with Python 3.10.
Do not touch files outside pipelines/spark/, pipelines/tests/, docs/reports/runs/ without
asking me. Never commit data files. Before claiming anything works, run the command and show
me the output (make lint test; spark-submit on spark://spark-master:7077). When done, prepare a
branch feature/vnp-silver-job and a pull request description; do not merge.
```
