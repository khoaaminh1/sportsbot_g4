# Data: where it lives, how to get it, what is in it

## Why the data is not in this repository
- The repository is **public**. Collected articles contain the publishers' full text: sharing
  it publicly is not allowed. Only the team receives it, for this course project.
- Data changes every hour; git is for code. `data/` is git-ignored on purpose.
- Never commit data files, never upload them to a public place, never paste article text into
  issues or pull requests.

## Who produces it
Only TMK's home PC crawls (24/7, Vietnam IP). Everyone else works from snapshots that TMK
exports. Do not run the crawler on another machine for long: it would hit the same sites twice
and build a different, smaller corpus.

## How the team receives snapshots

**Private Kaggle dataset (default).** TMK runs `make kaggle-export && make kaggle-push` (a new
version each time). You run `make kaggle-pull`: files land in `data/kaggle/`. One-time setup per
person: Kaggle → Settings → API → Create New Token → put it in your `.env` as
`KAGGLE_API_TOKEN=...`, and set `KAGGLE_DATASET=<TMK's Kaggle username>/sportsbot-corpus`
(TMK adds you as a collaborator; the dataset must stay private).
An AI assistant can run `make kaggle-pull` itself once the token is in `.env`; it must never
ask you to paste the token into a chat.

**Fallback: the team's Google Drive folder** (shared with team members only), same files.

## The snapshot files

| File | Made by | Use it for |
|---|---|---|
| `sportsbot-<YYYYMMDD-HHMM>.sql.gz` | `make db-export` | Load the whole PostgreSQL database into your stack (`make db-import`) |
| `bronze-<YYYYMMDD-HHMM>.tgz` | `make bronze-export` | Spark input on HDFS (`make bronze-load`) |
| `articles.jsonl.gz` + `export-summary.json` | `make kaggle-export` | Quick analysis with pandas or Spark local mode |
| `sample.csv` | `make export-labeling N=400` | Hand labels (LTN) |

Files from `make kaggle-pull` are in `data/kaggle/`; put files received over Drive in `data/snapshots/`. The time in the file
name is the snapshot time (local time of TMK's PC); cite it in every report.

## How to load each file

**Into PostgreSQL + Qdrant (to run the chatbot on the team corpus):**
```bash
make up-demo
docker compose --profile core --profile stream stop discover extractor consumer-operational consumer-semantic consumer-archive
make db-import FILE=data/snapshots/sportsbot-<date>.sql.gz
make reindex
```
Stopping the workers keeps your machine from crawling and changing the imported corpus.

**Into HDFS (for the Spark jobs):**
```bash
make up-bigdata
make bronze-load FILE=data/snapshots/bronze-<date>.tgz
make hdfs-ls
```
Spark reads `hdfs://namenode:8020/sportsbot/bronze/source=extracted/ingest_date=*/ingest_hour=*/*.jsonl.gz`.
Jobs live in `pipelines/spark/jobs/` (mounted in the Spark containers at
`/opt/sportsbot/pipelines/spark/jobs/`); run them like `make spark-smoke` does.

**With pandas:**
```python
import pandas as pd

df = pd.read_json("data/snapshots/articles.jsonl.gz", lines=True)
```

## Record formats

### `articles.jsonl.gz` (one article per line; from PostgreSQL `articles`)
`article_id` (sha256 of the canonical URL), `source_id`, `source_name`, `language` (ISO 639-1,
from the page or the source), `title`, `text` (full extracted text, or the feed summary for
RSS-only sources such as BBC), `summary`, `url`, `published_at`, `collected_at`,
`ingested_at`, `extracted_at` (RFC 3339, UTC), `word_count`, `discovered_via` (`rss` | `listing` |
`sitemap`), `content_hash` (sha256 of the normalised text), `sport_category` (null until the
classifier exists).

### Bronze `source=extracted/...` (one `articles.extracted` Kafka message per line)
Same article plus pipeline metadata: `schema` (`extracted.v1`), `canonical_url`, `language_hint`,
`summary_html`, `published_raw`, `tags`, `discovered_at`, `run_id`, `extractor`
(`trafilatura` | `rss_summary`). Built by
`pipelines/src/sportsbot_pipelines/extraction/worker.py::build_extracted`.
The archive can also contain `source=<feed id>/...` folders: the raw RSS records (titles and
summaries only) written by discovery.

### Known properties of the data (read before analysing)
- Duplicates: exact re-posts (same normalised text) are dropped before storage; near-duplicates
  (the same story from two outlets) are **kept** on purpose; finding them is Spark's job.
- `published_at` is the publisher's time; it can be missing; never treat it as the event time.
- BBC rows hold feed summaries only (the article pages are blocked from the crawler's network).
- Languages are skewed towards Vietnamese; report per-language numbers, not only totals.
- Every report states: snapshot file name, row counts, and that the data was collected by
  SportsBot (or, for scaling runs, that a public corpus was used and which one).
