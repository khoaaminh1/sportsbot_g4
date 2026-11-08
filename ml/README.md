# ml

Datasets, hand-labelled sets and the model card of Model A, the sport-category classifier (plan
task 7). Specification: `docs/handoff/VNP.md`, task 7. Model B (query intent) is out of scope for
the presentation.

Status (2026-11-09): **code ready, not yet run on the real corpus.** The Spark job ran end to end
on a small synthetic data set (local mode) to check the code; those numbers are not results and
are not reported. Real numbers come from `make spark-classifier` and land in
`docs/reports/runs/classifier-<run_id>.json`.

## Model card: sport classifier

| | |
|---|---|
| Task | One sport category per article: `football`, `volleyball`, `basketball`, `tennis`, `badminton`, `other_sport` (the API's categories) |
| Input | Silver articles (`hdfs:///sportsbot/silver/articles`), Vietnamese, English, Spanish and more |
| Code | `pipelines/spark/jobs/sport_classifier.py` (Spark job), `pipelines/spark/jobs/sportlabels.py` (weak labels and metrics, unit-tested) |
| Baseline | Spark MLlib: RegexTokenizer (title + first 2,000 characters) → HashingTF (2^18) → IDF → multinomial LogisticRegression (maxIter 50, regParam 0.01) |
| Improved | The article's BGE-M3 vector already in Qdrant (chunk 0, 1024-d) → the same LogisticRegression |
| Training labels | Weak labels from metadata, never from the text (rules below) |
| Test sets | 1. Hand labels `ml/labels/sport_test_v1.csv` (excluded from training). 2. A 20 % weak-label holdout (seed 42), reported separately |
| Metrics | Accuracy, macro-F1, per-class precision/recall/F1, confusion matrix, macro-F1 per language |
| Environment | Single machine, pseudo-distributed Spark standalone in Docker (1 master, 2 workers); not a production cluster |

### Weak-label rules (fixed before the first run)

The first matching rule wins; articles with no match are not used for training.

1. **Feed**: the `source_id` of a per-sport feed. `tennis` → tennis; `nba`, `basketball` →
   basketball; `nfl` → other_sport (checked before `football`); `football`, `soccer`, `futbol` →
   football; `formula1`, `formulaone`, `rpm`, `cricket`, `rugby`, `golf`, `cycling`, `athletics`,
   `boxing`, `snooker`, `swimming`, `mlb`, `nhl` → other_sport.
2. **URL path segment**: `bong-chuyen`/`volleyball` → volleyball; `cau-long`/`badminton` →
   badminton; `tennis`; `bong-ro`/`basketball`/`nba` → basketball; `nfl`, `f1`, `formula1`,
   `cricket`, `rugby`, `golf`, `cycling`, `boxing`, `athletics` → other_sport;
   `bong-da`/`football`/`soccer`/`futbol` → football.
3. **Title keyword, volleyball and badminton only** (`bóng chuyền`, `volleyball`, `voleibol`,
   `cầu lông`, `badminton`, `bádminton`): no feed covers these sports, so without this rule they
   would have no training rows.

### Known limitations

- The weak-label holdout uses the same rules as the training labels, so it overstates quality;
  the hand-labelled set is the number to quote.
- Volleyball and badminton have few training rows (title rule only); expect low recall.
- Feeds and URL paths are dominated by football; the report gives the class counts.
- Articles without a Qdrant vector are left out of the improved model's numbers (counts in the
  report).

## Hand labels

`ml/labels/sport_test_v1.csv` holds only `article_id,sport_label,labeler,notes`. Titles and
excerpts are publishers' text and never go into git (`docs/DATA.md`).

1. `make export-labeling N=200` → `data/labeling/sample.csv` (not committed).
2. Label each row with one of the six classes (`notes` for doubtful cases).
3. Keep only the four columns above, save as `ml/labels/sport_test_v1.csv`, commit.

## How to run

```bash
make up-core && make up-bigdata          # Qdrant (core) and Spark/HDFS (bigdata)
make bronze-load FILE=data/kaggle/bronze.tgz
make spark-silver SNAPSHOT="<kaggle version, time>"
make labels-load                         # copies the hand labels into HDFS
make spark-classifier SNAPSHOT="<kaggle version, time>"
```

`spark-classifier` writes `data/classifier.log` and `docs/reports/runs/classifier-<run_id>.json`.
Without hand labels the job still runs and reports the weak-label holdout only.
