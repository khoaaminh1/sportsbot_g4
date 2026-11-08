# Handoff: VNP (Big Data layer and classifier)

For VNP and VNP's AI assistant. Deadlines and checklists: `docs/TASKS.md` (VNP section). Read this file, then `AGENTS.md`, `docs/DATA.md`,
`docs/TEAM_ONBOARDING.md` and tasks 6, 7, 9 of
`docs/plans/2026-11-02-plan-to-presentation.md`.

## Your outcomes (presentation moved to Thursday 12 November; deadlines in docs/TASKS.md)
1. **Task 6 – Spark Silver/Gold** on HDFS from the Bronze snapshot: clean, exact + near-duplicate
   detection (MinHashLSH), story clusters, Gold statistics, a JSON run report per run.
2. **Task 7 – Sport classifier**: baseline (Spark MLlib TF-IDF + logistic regression) vs improved
   (BGE-M3 vectors + logistic regression), evaluated on LTN's hand labels only.
3. **Task 9 – Scaling runs**: the Silver job at increasing input sizes, `local[*]` vs the
   2-worker standalone cluster, with the measured table.

## State on 2026-11-04 (read this first)

- **Bronze is done; Silver and Gold do not exist yet.** That is expected: they are your task 6.
  Bronze is written continuously by the streaming pipeline (`consumer-archive`); Silver and Gold
  are batch Spark jobs that read it. No file under `pipelines/spark/jobs/` other than
  `hdfs_smoke.py` exists, and `ml/` has only a README.
- Where Bronze lives: on TMK's crawler PC in the Docker volume `bronze_data` (local backend, not
  HDFS, because the crawler PC does not run the HDFS profile). You get it as `bronze.tgz` from the
  private Kaggle dataset and load it into **your** HDFS with `make bronze-load`. Ask TMK for a fresh
  `make kaggle-push` before a run you will report, and write the Kaggle version and
  `export-summary.json` time in the run report (`snapshot_file`).
- Size: about 12,000 articles on 2026-11-04 (vi, en, es, fr), most of them from a backfill of
  September-November 2026, then about 30 new articles per hour.
- Archive layout: `source=extracted/ingest_date=YYYY-MM-DD/ingest_hour=HH/*.jsonl.gz` holds the
  articles (read only this). Folders `source=<feed id>/` hold raw RSS records (titles and summaries)
  from discovery; ignore them for Silver.
- Known properties to handle in Silver (decide and write the rule **before** the first reported
  run):
  - Some extracted texts start with the title repeated (VnExpress: "Al Nassr đại thắng khi vắng
    Ronaldo Uzbekistan Không có ..."); harmless for exact duplicates, may raise near-duplicate
    similarity slightly.
  - BBC rows (`extractor = rss_summary`) contain only the feed summary (short texts): exclude
    them from near-duplicate detection or report them separately.
  - `published_at` can be missing; never use it as the event time.
- Weak labels for task 7: many Vietnamese sites use flat URLs without a section path
  (VnExpress, Tuổi Trẻ, VietNamNet), so URL paths alone label few articles. Since 2026-11-03 most
  international feeds are **per sport**, so `source_id` gives reliable weak labels:

  | `source_id` contains | Weak label |
  |---|---|
  | `football`, `soccer`, `futbol` | football (NFL feeds are American football: other_sport) |
  | `tennis` | tennis |
  | `nba`, `basketball` | basketball |
  | `formula1`, `formulaone`, `rpm`, `cricket`, `rugby`, `golf`, `cycling`, `athletics`, `boxing`, `snooker`, `swimming`, `nfl`, `mlb`, `nhl` | other_sport |
  | general feeds (`*_the_thao`, `bbc_sport_all`, `guardian_sport`, `france24_sport_fr`) | no weak label (use URL path rules where they exist, e.g. Dân Trí `/the-thao/bong-da/`) |

  The API's categories are `football, volleyball, basketball, tennis, badminton, other_sport`
  (`docs/contracts/api.openapi.yaml`); volleyball and badminton have no dedicated feed, so they come
  only from Vietnamese URL paths or keywords. Write the final mapping in the model card, and keep
  LTN's hand labels as the only test set.

## Results (VNP, 2026-11-05)

Snapshot: Kaggle `minhkhoa132/sportsbot-corpus`, `export-summary.json` exported_at
2026-11-03T08:53:39Z, 11,620 articles; 11,573 rows in Bronze `source=extracted` (Bronze is
written in batches of 100, so the newest articles of an export may be missing). Cluster:
`spark://spark-master:7077`, 1 worker with 2 cores, 2 HDFS DataNodes (single machine,
pseudo-distributed).

**V1 Silver** (PR #28, run report `docs/reports/runs/silver-20261105T150259Z.json`)
- 11,573 rows in, 11,573 out, 0 rejected, 0 exact duplicates; 673.8 s in the job.
- MinHash LSH written in Spark SQL (the Spark image has no numpy for `pyspark.ml`): 1,215,007
  candidate pairs (1.98 % of all pairs), 1,368 pairs with Jaccard >= 0.8, 28 story clusters with
  118 articles.
- Hand check: 5 clusters (10 articles) are the same story from two outlets; one VietNamNet
  cluster of 52 articles has the site's navigation menu as text (extractor issue for TMK, as are
  titles with `&amp;apos;`). The Silver rules were not changed after seeing this.
- Two trial runs on the older snapshot (1,873 rows) gave identical counts: the job is
  deterministic.

**V2 Gold** (run report `docs/reports/runs/gold-20261105T154732Z.json`)
- Four tables under `gold/stats/`; consistency checks (table sums = 11,573 Silver rows) pass;
  20.57 s in the job.
- Near-duplicate rate 1.02 %; cross-source clusters 5 (10 articles), same-source 23 (108).
- Median lag `extracted_at - published_at`: RSS 50.51 h, sitemap 1,024.67 h, section pages
  1,279.89 h (backfill age, not pipeline speed). No article without `published_at`.

- **Since 2026-11-09 the Spark image has numpy** (`infra/spark/Dockerfile`, image
  `sportsbot-spark:3.5.3-numpy`), so `pyspark.ml` works for task 7 (TF-IDF, LogisticRegression,
  and MinHashLSH if you want to compare it with the SQL version). After `git pull`:
  `make up-bigdata` (rebuilds the image once), then `make spark-ml-check` prints `"ok": true` with the numpy version
  on the driver and on an executor.

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

> **2026-11-09: task 7 moved to TMK** (VNP finished Silver and Gold). Code:
> `pipelines/spark/jobs/sport_classifier.py`, model card `ml/README.md`. VNP: no action needed;
> the scaling runs (task 9) remain yours if there is time.

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
12 November 2026). Explain to me in Vietnamese; write code, comments, commits in English.
Read first: docs/TASKS.md, docs/handoff/VNP.md, AGENTS.md, docs/DATA.md, docs/TEAM_ONBOARDING.md.
Task now: V1 in docs/TASKS.md = Task 6 in docs/handoff/VNP.md (Spark Silver/Gold). Follow its fixed rules exactly.
Work test-first for the pure-Python helpers. Keep Spark code compatible with Python 3.10.
Do not touch files outside pipelines/spark/, pipelines/tests/, docs/reports/runs/ without
asking me. Never commit data files. Before claiming anything works, run the command and show
me the output (make lint test; spark-submit on spark://spark-master:7077). When done, prepare a
branch feature/vnp-silver-job and a pull request description; do not merge.
```
