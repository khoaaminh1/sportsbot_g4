# Evaluation

Owners: TMK (questions, judgements, metrics); LTN tests the app (`docs/TASKS.md`). Status 2026-11-06:
50 questions drafted, relevance judgements pending, scripts ready (`sportsbot_eval.py`).

`golden_questions.csv`: questions written by hand against the **collected** corpus.

| Column | Meaning |
|---|---|
| `question_id` | `q001`, `q002`, ... |
| `language` | `vi` or `en` |
| `question` | as a real user would type it (some without diacritics: "doi tuyen viet nam") |
| `expected_urls` | links of the articles that answer it (copy them from the sources under an answer, or from the left panel of the web app), separated by `;`; the evaluation script converts them to article ids |
| `answerable` | `yes`, or `no` for questions the corpus cannot answer (the bot must say so) |
| `question_type` | `latest`, `result`, `schedule`, `player`, `team`, `other` |
| `author` | `LTN`, `VNP`, `TMK` |

Target before the presentation: 50 questions (35 answerable, 15 not answerable), at least 15 in
English. Write the question first, then search the corpus for the articles; do not pick an
article and paraphrase its title (that makes retrieval look better than it is).

The 50 questions were drafted with AI assistance from general sports topics **without looking at
the corpus**, then reviewed by TMK; `expected_urls` come from human judgements (below).

## Workflow (on the machine with the stack)
```bash
make eval-pool     # top 10 articles per answerable question -> data/evaluation/pool.csv (git-ignored)
# open pool.csv (Google Sheets / Excel), put 1 in "relevant" for each article that answers it
make eval-apply    # copies the marked links into evaluation/golden_questions.csv
make eval          # metrics + docs/reports/runs/eval-<run_id>.json (about 1 min per question)
make eval NO_ANSWERS=1   # retrieval metrics only, no Gemini calls
```
An answerable question with no relevant article in its pool: search the web app for one, or set
`answerable` to `no` if the corpus really has nothing (write why in the PR).

## Metrics
- Retrieval, on answerable questions with judgements: hit@1, hit@5, recall@5, MRR@10.
- Answers: share of answerable questions answered (`generated` or `excerpts`), share that cite an
  expected article, share of unanswerable questions correctly refused (`no_evidence` or
  `insufficient`), seconds per question (p50, p90).
- Relevance comes from pooling the system's own top 10, so recall is relative to that pool, not
  to every relevant article in the corpus. The report says so.
