# ml

Datasets, hand-labelled sets and the model card of Model A, the sport-category classifier (plan
task 7). Specification: `docs/handoff/VNP.md`, task 7. Model B (query intent) is out of scope for
the presentation.

Status (2026-11-09): **done.** Run `20261108T213620Z` on Silver run `20261108T205819Z`:
[report](../docs/reports/runs/classifier-20261108T213620Z.json), results table and findings in the
README Status section. On the 193 test articles: baseline accuracy 0.570 / macro-F1 0.544, BGE-M3
accuracy 0.927 / macro-F1 0.750.

## Model card: sport classifier

| | |
|---|---|
| Task | One sport category per article: `football`, `volleyball`, `basketball`, `tennis`, `badminton`, `other_sport` (the API's categories) |
| Input | Silver articles (`hdfs:///sportsbot/silver/articles`), Vietnamese, English, Spanish and more |
| Code | `pipelines/spark/jobs/sport_classifier.py` (Spark job), `pipelines/spark/jobs/sportlabels.py` (weak labels and metrics, unit-tested) |
| Baseline | Spark MLlib: RegexTokenizer (title + first 2,000 characters) → HashingTF (2^18) → IDF → multinomial LogisticRegression (maxIter 50, regParam 0.01) |
| Improved | The article's BGE-M3 vector already in Qdrant (chunk 0, 1024-d) → the same LogisticRegression |
| Training labels | Weak labels from metadata, never from the text (rules below) |
| Test sets | 1. Test labels `ml/labels/sport_test_v1.csv` (excluded from training). 2. A 20 % weak-label holdout (seed 42), reported separately |
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
- Volleyball and badminton training rows come from the title rule only; for Vietnamese articles
  this rule dominates the weak labels, and the TF-IDF baseline learns language instead of sport
  (it labels most Vietnamese football test articles as volleyball).
- The test set is small for minority classes (volleyball 2, tennis 4) and AI-assisted.
- Feeds and URL paths are dominated by football; the report gives the class counts.
- Articles without a Qdrant vector are left out of the improved model's numbers (counts in the
  report).

## Test labels

`ml/labels/sport_test_v1.csv` holds only `article_id,sport_label,labeler,notes`. Titles and
excerpts are publishers' text and never go into git (`docs/DATA.md`).

**Origin of v1 (197 articles, random sample from `make export-labeling N=200`):** labels were
proposed by Claude (AI assistant) from each article's title and excerpt and reviewed by TMK;
`labeler` is `TMK+Claude`, and rows TMK corrects by hand become `TMK`. They are AI-assisted, not
independent human annotation. Class counts: football 150, other_sport 29, basketball 6,
badminton 6, tennis 4, volleyball 2. 193 of the 197 articles are in Silver run
`20261108T205819Z`.

To relabel by hand:

1. `make export-labeling N=200` → `data/labeling/sample.csv` (not committed).
2. Label each row with one of the six classes (`notes` for doubtful cases).
3. Keep only the four columns above, save as `ml/labels/sport_test_v1.csv`, commit.

## How to run

```bash
make up-core && make up-bigdata          # Qdrant (core) and Spark/HDFS (bigdata)
make bronze-load FILE=data/kaggle/bronze.tgz
make spark-silver SNAPSHOT="<kaggle version, time>"
make spark-gold SNAPSHOT="<same>"            # optional: Gold tables on the same Silver
make labels-load                         # copies the hand labels into HDFS
make spark-classifier SNAPSHOT="<kaggle version, time>"
```

`spark-classifier` writes `data/classifier.log` and `docs/reports/runs/classifier-<run_id>.json`.
Without hand labels the job still runs and reports the weak-label holdout only.
