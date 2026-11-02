# Evaluation

Owners: LTN (questions), TMK (scripts and metrics). Status: question template ready; scripts are plan task 8.

`golden_questions.csv`: questions written by hand against the **collected** corpus.

| Column | Meaning |
|---|---|
| `question_id` | `q001`, `q002`, ... |
| `language` | `vi` or `en` |
| `question` | as a real user would type it (some without diacritics: "doi tuyen viet nam") |
| `expected_article_ids` | article ids (from the demo page link or `make export-labeling`) that answer it, separated by `;` |
| `answerable` | `yes`, or `no` for questions the corpus cannot answer (the bot must say so) |
| `question_type` | `latest`, `result`, `schedule`, `player`, `team`, `other` |
| `author` | `LTN`, `VNP`, `TMK` |

Target before the presentation: 50 questions (35 answerable, 15 not answerable), at least 15 in
English. Write the question first, then search the corpus for the articles; do not pick an
article and paraphrase its title (that makes retrieval look better than it is).

Metrics (computed by a script added in the plan, task 9): recall@5, MRR, share of correct
"no evidence" answers, citation validity.
