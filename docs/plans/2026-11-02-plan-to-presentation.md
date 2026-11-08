# Plan to the Presentation (Tuesday 10 November 2026)

> **For agentic workers (TMK and VNP use Claude):** implement one task at a time, test first
> (red, then green), and do not mark a step done without the command output. Steps use
> checkboxes (`- [ ]`). LTN works without an AI assistant: LTN's tasks are written so that each
> step is a command to run or a file to fill in.

**Goal:** On 10 November, demonstrate a working system on real collected news: streaming
collection, a Big Data batch layer, one trained and evaluated model, and a chatbot that answers
only from stored articles with links and publication times.

**Architecture:** Team diagram (ADR 0005): RSS/sitemaps → Kafka → extractor → PostgreSQL +
Qdrant (BGE-M3) → hybrid RAG with reranker (ADR 0007) → free Gemini models in rotation
(ADR 0006) → FastAPI (SSE chat, WebSocket news) → demo page. A third consumer archives
extracted articles to HDFS; Spark builds Silver/Gold and the experiments from it.

**Tech stack:** Python 3.11, Kafka 3.9 (KRaft), PostgreSQL 16, Qdrant 1.19, llama.cpp (GGUF),
HDFS 3.4 + Spark 3.5, FastAPI, Docker Compose.

**Spec:** `docs/MASTER_PROJECT_INSTRUCTION.md` (Claude Project), ADR 0005-0007, the team's
pipeline diagram (2026-11-02).

## Global Constraints

- Presentation: Tuesday 10 November 2026. Code freeze: Monday 9 November, 12:00.
- No fabricated data, numbers or results. Synthetic and public datasets are labeled by origin.
- robots.txt and the per-host delay (default 3 s) are never disabled to crawl faster.
- No secrets in git: `GEMINI_API_KEY` lives only in `.env`.
- One Gemini key per project; no extra accounts to multiply quota.
- Code, docs, commits in English. `main` changes only through pull requests with CI green.
- Only one crawler instance writes to the shared database (the always-on home PC).

## Review Focus

1. A site that blocks us (403) or changes layout → articles go to the dead letters with a reason;
   the source is disabled, nothing crashes. Covered by Task 2 step 4.
2. Embedder/Qdrant down while crawling → nothing lost, the semantic consumer resumes. Covered by
   the `TransientError` unit test; Task 1 step 6 checks it live.
3. Questions typed without diacritics ("ket qua viet nam") → still retrieve. Golden set must
   include them (Task 4).
4. A question the corpus cannot answer → "no evidence", no LLM call, no invented facts. Golden
   set has 15 such questions (Task 4); Task 8 measures refusals.
5. All Gemini models out of quota during the demo → article excerpts with links are shown.
   Task 10 rehearses with `GEMINI_API_KEY` removed.

---

## Who does what

| Member | Assistant | Focus |
|---|---|---|
| TMK (lead) | Claude | Stack running 24/7, crawling, RAG quality, API/demo page, merges, demo script |
| VNP | Claude | Spark Silver/Gold + near-duplicates, classifier training (MLlib), scaling experiment |
| LTN | none | Sources and sitemaps, hand labels, golden questions, slides, testing the demo page |

Daily at 21:00: each member posts in the group chat what is done (with the command output or a
screenshot), what is next, what blocks them. TMK merges at most once a day after CI is green.

## Status on 2026-11-09

The presentation moved to **Thursday 12 November 2026**; code freeze Wednesday 11 November, 12:00.
The day-by-day gates below are the original schedule, kept as history; the current schedule is
`docs/TASKS.md`. Done since 2026-11-04: task 6 (Silver + Gold, PRs #28, #30), tasks 4 and 8
(golden set and first evaluation, PRs #32-#35). Open: task 5 (labels, TMK), task 7 and 9 (VNP),
task 10 (all), LTN's test rounds.

## Status on 2026-11-04

Done: task 1 (stack running 24/7, restarts after reboot), task 2 (45 of 48 feeds checked from
Vietnam, sitemaps and section pages configured; done by TMK), task 3 (backfill from 2026-09-01:
about 12,000 articles). Not started: tasks 4-10. Current progress board: the Status section of
`README.md`. Work split with deadlines per person: `docs/TASKS.md` (LTN is the tester).

## Day-by-day gates

| Day | Gate (evidence) |
|---|---|
| Mon 2 Nov (night) | Branch pushed, PR open, CI result known |
| Tue 3 | Crawler running 24/7 on the home PC; `make crawl-stats` shows > 0 articles from ≥ 10 sources |
| Wed 4 | Sitemap backfill for ≥ 5 Vietnamese sources; ≥ 3,000 articles; first cited answers |
| Thu 5 | Spark Silver job with duplicate counts; 200 labels; 30 golden questions |
| Fri 6 | Classifier baseline trained and evaluated; 400 labels; 50 golden questions |
| Sat 7 | Retrieval evaluation numbers; scaling experiment (2 sizes); corpus snapshot frozen |
| Sun 8 | Slides v1, demo script, rehearsal 1 |
| Mon 9 | 12:00 code freeze; bug fixes only; rehearsal 2; backup demo video recorded |
| Tue 10 | Presentation |

---

### Task 1 (TMK): Run the stack and start crawling — Tue 3

**Files:** `.env` (local only), `pipelines/sources.json`

- [ ] Step 1: (done 2026-11-03 on `main`) `cp .env.example .env`; set `GEMINI_API_KEY`, `COLLECTOR_CONTACT`.
- [ ] Step 2: `make setup && make test` → expect `passed`, `0 failed`.
- [ ] Step 3: `make models && make up-demo && make ps` → every service running or healthy.
- [ ] Step 4: `make llm-check` → at least 2 Gemini targets `"ok": true`. Remove failing names from `GEMINI_MODELS`.
- [ ] Step 5: `make discover-once`, wait 5 minutes, `make crawl-stats` → articles > 0, `qdrant_chunks` > 0.
- [ ] Step 6: `docker compose --profile ai stop embedder`, wait 1 minute, `make up-ai`; `make crawl-stats` → chunk count keeps growing (no loss during the outage).
- [ ] Step 7: Move the stack to the home PC (Windows + WSL2 or Docker Desktop), same steps; leave it running. Only this machine crawls.
- [ ] Step 8: Post `make crawl-stats` output in the group chat.

### Task 2 (LTN): Verify sources and add sitemaps — Tue 3 to Wed 4

**Files:** `pipelines/sources.json`

- [ ] Step 1: `make sources-check` → copy the output into the group chat.
- [ ] Step 2: For each source with `"status": "ok"`: set `"verified": true` and add to `notes`: `Live check YYYY-MM-DD (LTN): N entries.`
- [ ] Step 3: For each source with an error: set `"enabled": false` and write the error in `notes`.
- [ ] Step 4: `make sources-sitemaps` → for each Vietnamese source, copy the sitemap URLs into `"sitemaps"`, and set `"sitemap_include"` to the URL part of sports articles (for example `"/the-thao/"` or `"/bong-da|/the-thao"`). Open one article URL in the browser to check the pattern.
- [ ] Step 5: Find 5 more Vietnamese sports RSS feeds (Bóng Đá+, Webthethao, Thể Thao 247, Thể thao & Văn hóa, VTC News...). Add each like the existing entries, `"verified": false`. Then repeat Step 1.
- [ ] Step 6: `make test` → `test_repository_registry_is_valid` passes. Open a pull request "chore(sources): verify feeds and add sitemaps".

### Task 3 (TMK): Backfill from sitemaps — Wed 4

- [ ] Step 1: After Task 2 is merged: `make backfill SINCE=2026-10-01 MAX=3000`.
- [ ] Step 2: `make dead-letters` → if one source has mostly `too_short`/`no_main_text`, lower `EXTRACT_MIN_CHARS` only if the pages are real articles; otherwise disable its sitemap.
- [ ] Step 3: `make crawl-stats` → ≥ 3,000 articles in total. Post the output.

### Task 4 (LTN): Golden questions — Wed 4 to Fri 6

**Files:** `evaluation/golden_questions.csv` (rules in `evaluation/README.md`)

- [ ] Step 1: Write 50 questions (35 answerable, 15 not; ≥ 15 in English; ≥ 10 typed without diacritics).
- [ ] Step 2: For each answerable question, find the answering articles on http://localhost:8000/app/ or with `make ask-no-llm Q="..."`; copy their links into `expected_urls`.
- [ ] Step 3: Commit on a branch, open a pull request.

### Task 5 (LTN): Hand labels for the classifier test set — Wed 4 to Fri 6

**Files:** `data/labeling/sample.csv` (exported), labeled copy committed as `ml/labels/sport_test_v1.csv`

- [ ] Step 1: TMK runs `make export-labeling N=400` and sends the CSV.
- [ ] Step 2: Open in Google Sheets. Fill `sport_label` with exactly one of: `football, volleyball, basketball, tennis, badminton, other_sport`; `labeler` = `LTN`. Unsure → `notes`.
- [ ] Step 3: Gate Thu 5: 200 rows; gate Fri 6: 400 rows. Download as CSV (UTF-8); commit only `article_id,sport_label,labeler,notes` (no titles or excerpts: publishers' text stays out of the public repo).

### Task 6 (VNP): Spark Silver/Gold on the archive — Tue 3 to Thu 5

**Files:** Create `pipelines/spark/jobs/silver_articles.py`, `pipelines/spark/jobs/gold_stats.py`; Test: `pipelines/tests/unit/test_spark_logic.py` (pure-Python helpers)

**Interfaces:**
- Consumes: Bronze `source=extracted/ingest_date=*/ingest_hour=*/part-*.jsonl.gz` (fields of `articles.extracted`, see `pipelines/src/sportsbot_pipelines/extraction/worker.py::build_extracted`).
- Produces: `hdfs:///sportsbot/silver/articles/` Parquet (article_id, source_id, language, title, text, published_at, content_hash, story_cluster_id), `hdfs:///sportsbot/gold/stats/` (counts per source/day/language, duplicate rate) and a JSON run report under `docs/reports/runs/`.

- [ ] Step 1: Failing test for the shingling/normalising helper (pure Python, no Spark).
- [ ] Step 2: Silver job: read JSONL, drop exact duplicates by `content_hash`, `MinHashLSH` on word 3-shingles, Jaccard ≥ 0.8 → same `story_cluster_id` (rule fixed before running).
- [ ] Step 3: Run on the cluster: `spark-submit --master spark://spark-master:7077 ...` → report with input, output, exact duplicates, near-duplicate pairs, duration.
- [ ] Step 4: Gold stats job → Parquet + run report.
- [ ] Step 5: Pull request with the run reports.

### Task 7 (VNP): Sport classifier, baseline vs improved — Thu 5 to Fri 6

**Files:** Create `ml/train_sport_classifier.py`, `ml/README.md` (model card); run report `docs/reports/runs/classifier-*.json`

- [ ] Step 1: Training labels from URL paths (weak labels: `/bong-da/`, `/football/` → football, `/tennis/` → tennis, ...); write the mapping in the model card. Test set: LTN's hand labels only (never used for training).
- [ ] Step 2: Baseline: Spark MLlib TF-IDF (character n-grams) + logistic regression. Improved: BGE-M3 vectors (embedder API) + logistic regression.
- [ ] Step 3: Report macro-F1, per-class F1, per-language F1, confusion matrix on the test set.
- [ ] Step 4: The better model fills `articles.sport_category` (script, idempotent).

### Task 8 (TMK): Retrieval and answer evaluation — Fri 6 to Sat 7

**Files:** Create `evaluation/run_eval.py`; Test: `evaluation/tests/test_metrics.py`

- [ ] Step 1: Failing tests for `recall_at_k(expected: set[str], ranked: list[str], k: int) -> float` and `mrr(...) -> float`.
- [ ] Step 2: Script: for each golden question, keyword-only, vector-only, hybrid, hybrid + rerank → recall@5, MRR; for unanswerable questions, share of `no_evidence`/`insufficient`.
- [ ] Step 3: Tune `RERANK_MIN_SCORE` with the rule fixed first: highest threshold that keeps recall@5 within 2 points of no threshold.
- [ ] Step 4: Commit the run file and a short report.

### Task 9 (VNP): Scaling experiment — Sat 7

- [ ] Step 1: Freeze the corpus snapshot (date, counts).
- [ ] Step 2: Run the Silver job at two sizes (the real corpus, and a larger public Vietnamese news corpus labeled "public historical corpus, not collected by SportsBot") in `local[*]` and on the 2-worker cluster.
- [ ] Step 3: Table: records, duration, throughput, storage per layer. Pseudo-distributed, single machine: say so.

### Task 10 (all): Presentation — Sun 8 to Tue 10

- [ ] Slides (LTN drafts, TMK and VNP fill numbers): problem, diagram, live crawl numbers, Kafka UI, Spark results, classifier, RAG evaluation, demo, limitations, honesty notes.
- [ ] Demo script (TMK): 3 questions that work, 1 the corpus cannot answer, 1 typed without diacritics; the live news feed updating.
- [ ] Rehearsal with `GEMINI_API_KEY` removed (excerpts with links must still be shown).
- [ ] Backup video of the full demo (Mon 9).

## Cut list (in this order, if a gate is missed)

1. Second ML model (question intent): already cut.
2. Scaling experiment with the public corpus → real corpus only, two sizes.
3. Improved classifier → baseline only, reported honestly.
4. HDFS archive in the demo → show Spark run reports instead of live HDFS.
