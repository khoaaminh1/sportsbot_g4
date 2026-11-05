# Work split until the presentation (Tuesday 10 November 2026)

One page per person: what to deliver, where, by when, and how to know it is done.
Updated 2026-11-04 13:45 ICT. The live progress board is the Status section of `README.md`;
when you finish a step, tick it here **and** update that board in the same pull request.

| Person | Role | Delivers |
|---|---|---|
| **TMK** (Trần Minh Khoa) | Lead, pipeline, RAG, demo | Crawler PC, data snapshots, retrieval evaluation, latency, merges, demo |
| **VNP** (Võ Ngọc Phú) | Big Data engineer | Spark Silver + Gold, sport classifier, scaling experiment |
| **LTN** (Lê Tấn Nguyên) | Tester and evaluation data | Test rounds with reports and bug issues, golden questions, hand labels |

## How work is credited (GitHub contributors)

GitHub lists a person as a contributor when commits **authored by their account** reach `main`.
1. Accept the invitation to the repository (TMK sends it), or work from a fork.
2. Set your identity once, with the e-mail of your GitHub account:
   `git config --global user.name "Your Name"` and `git config --global user.email "you@example.com"`
   (or the `...@users.noreply.github.com` address shown in GitHub → Settings → Emails).
3. Branch → commit → pull request → both CI checks green → TMK merges. Never push to `main`,
   never tick "bypass rules".
4. Without the command line (LTN): on GitHub open the file → pencil icon (edit) or
   "Add file → Upload files" → "Create a new branch for this commit and start a pull request".

Only real work is committed: code, tests, run reports, test reports, golden questions, labels.

## Timeline

| Day | VNP | LTN | TMK |
|---|---|---|---|
| Wed 4 | Set up HDFS/Spark, load Bronze, task 6 tests | Test round 1 (setup on own machine) | Fresh Kaggle snapshot, new labeling sample |
| Thu 5 | **Silver job run on the cluster** | Golden questions 25, labels 200 | Answer latency |
| Fri 6 | Gold job; classifier baseline | Golden questions 50, labels 400 | Evaluation script ready |
| Sat 7 | Classifier improved + report; scaling runs | Test round 2 (full app) | Task 8 numbers; corpus snapshot frozen |
| Sun 8 | Slides: Spark + classifier part | Slides draft, demo checklist | Slides: pipeline + RAG part; rehearsal 1 |
| Mon 9 | Fixes only (code freeze 12:00) | Test round 3 (regression on the frozen build) | Backup demo video; rehearsal 2 |
| Tue 10 | Presentation | Presentation | Presentation |

If a day slips, the cut list at the end of the plan applies (scaling with a public corpus is cut first).

---

## VNP: Big Data layer and model

Read first: [handoff/VNP.md](handoff/VNP.md) (fixed rules, data state, weak labels),
[DATA.md](DATA.md), `AGENTS.md`. Python 3.10 in the Spark image.

### V1. Spark Silver (plan task 6) — due **Thu 5, 23:59**
Branch `feature/vnp-silver-job`.
- [x] `make kaggle-pull`, `make up-bigdata`, `make spark-smoke` prints `"ok": true`.
- [x] `make bronze-load FILE=data/kaggle/bronze.tgz`, `make hdfs-ls` shows `source=extracted`.
- [x] Write the cleaning rules you add (title repeated at the start of text, BBC summary rows) in
      the job's docstring **before** the first reported run.
- [x] `pipelines/spark/jobs/textnorm.py` + `pipelines/tests/unit/test_textnorm.py` (tests first).
- [x] `pipelines/spark/jobs/silver_articles.py`: clean, exact duplicates by `content_hash`,
      MinHashLSH (5 tables) on word 3-shingles, Jaccard ≥ 0.8, `story_cluster_id`, rejected rows.
- [x] Run with `--master spark://spark-master:7077`; commit `docs/reports/runs/silver-<run_id>.json`.
- [x] Pull request: command used, run report, `make lint test` output.

**Done when** Silver Parquet is on HDFS, the run report is in the PR, tests pass, and rerunning the
same command on the same snapshot gives the same counts.

### V2. Spark Gold (plan task 6) — due **Fri 6, 12:00**
Branch `feature/vnp-gold-job`.
- [ ] `pipelines/spark/jobs/gold_stats.py`: articles per source × day × language, exact and near
      duplicate rates, cluster size distribution, freshness lag percentiles.
- [ ] Run on the cluster; commit `docs/reports/runs/gold-<run_id>.json`.
- [ ] Add one table of Gold numbers to the README Status section (with the run id).

### V3. Sport classifier (plan task 7) — due **Sat 7, 18:00**
Branch `feature/vnp-classifier`.
- [ ] Weak labels from `source_id` and URL paths (mapping in handoff/VNP.md); write the final
      mapping in `ml/README.md` (model card).
- [ ] Test set: LTN's labels (`ml/labels/sport_test_v1.csv`) only, never in training.
- [ ] Baseline: Spark MLlib TF-IDF + logistic regression. Improved: BGE-M3 vectors (embedder API)
      + logistic regression.
- [ ] Report macro-F1, per-class and per-language F1, confusion matrix:
      `docs/reports/runs/classifier-<run_id>.json`.
- [ ] Optional, if time: script that fills `articles.sport_category` with the better model.

### V4. Scaling experiment (plan task 9) — due **Sun 8, 12:00** (cut first if late)
- [ ] Silver job at 3 sizes of the real corpus (e.g. 25 %, 50 %, 100 %), `local[*]` and the cluster.
- [ ] Table: rows, duration, rows/s, storage per layer; say "single machine, pseudo-distributed (Docker)".

### V5. Slides — Sun 8
Two or three slides: Bronze → Silver → Gold with real counts, duplicate findings, classifier
results, scaling table. Numbers only from your run reports.

---

## LTN: tester and evaluation data

LTN has no AI assistant: every step below is something to open, type, check or fill in.
Setup: [TEAM_ONBOARDING.md](TEAM_ONBOARDING.md) sections 1-4 (`make team-data`).

### L1. Test round 1: setup on a clean machine — due **Wed 4 / Thu 5**
Testing the onboarding guide is itself a test: a teammate must be able to run the project from it.
- [ ] Follow TEAM_ONBOARDING sections 1-4 exactly as written. Note the time each section takes.
- [ ] Every step that fails or is unclear: open a GitHub issue with the **Bug** template (the exact
      command, the output, Windows/macOS). No screenshots of `.env` or tokens.
- [ ] Result: `make team-data` finishes and http://localhost:8000/app/ answers a question.

### L2. Golden questions (plan task 4) — 25 by **Thu 5**, 50 by **Fri 6**
File: `evaluation/golden_questions.csv` (rules in `evaluation/README.md`).
- [ ] 50 questions: 35 answerable, 15 not answerable; at least 15 in English; at least 10 typed
      without diacritics; several sports, not only football.
- [ ] Write the question first, then find the answering articles in the web app; copy each
      article's link into `expected_urls` (several links separated by `;`).
- [ ] Commit through a pull request (GitHub web upload is fine).

### L3. Hand labels (plan task 5) — 200 by **Thu 5**, 400 by **Fri 6**
- [ ] TMK sends a fresh `sample.csv` (400 articles from the multi-sport corpus).
- [ ] In Google Sheets fill `sport_label` with exactly one of
      `football, volleyball, basketball, tennis, badminton, other_sport`; `labeler` = `LTN`;
      unsure → write why in `notes`.
- [ ] Commit **only** the columns `article_id,sport_label,labeler,notes` as
      `ml/labels/sport_test_v1.csv` (titles and excerpts are publishers' text and must not go into
      the public repository).

### L4. Test round 2: the whole app — due **Sat 7**
On http://localhost:8000/app/ after `make team-data` with the newest snapshot. For each case write
pass/fail, the answer time shown under the answer, and a note.

| # | Do | Expected |
|---|---|---|
| 1 | Open the page | Left panel: "Trực tiếp", article count, sources, languages; recent articles with times |
| 2 | Ask in Vietnamese with diacritics about a recent match | Answer with [1], [2] markers; numbered sources; links open the right article; publication time shown |
| 3 | Same question without diacritics ("doi tuyen viet nam") | Relevant answer, as in 2 |
| 4 | Ask in English about a Spanish or Vietnamese story | English answer citing the foreign-language source |
| 5 | Ask about a person, then a follow-up with "anh ấy" / "ông ấy" / "he" | The follow-up answer is about the same person |
| 6 | Ask something the news cannot know ("Ai vô địch World Cup 2034?") | "Không tìm thấy..." or "không đủ thông tin"; no invented facts |
| 7 | Click each sport shortcut (Bóng đá ... Bơi lội) and send | Question prefilled; answers for tennis, F1, basketball, golf, rugby are found |
| 8 | Click each suggested question | Each returns an answer or a clear "not found" |
| 9 | Empty message, only symbols ("???"), a 2,000-character message | Send disabled or a clear error; the page does not break |
| 10 | "Cuộc trò chuyện mới" after a conversation | Empty start page; the next question is not treated as a follow-up |
| 11 | Narrow the browser to phone width | Left panel becomes a drawer; everything readable |
| 12 | Switch the system between light and dark mode | Both themes readable |
| 13 | Click a source link in an answer | Opens the original article on the publisher's site |

- [ ] Write the results in `docs/reports/test-report-2026-11-07.md` (template below) and open a PR.
- [ ] One GitHub issue per failure (Bug template), linked in the report.

### L5. Test round 3: regression on the frozen build — **Mon 9**, after 12:00
- [ ] Repeat the L4 table on the code-freeze version; check that every issue marked fixed is fixed.
- [ ] `docs/reports/test-report-2026-11-09.md` via PR. This is the go/no-go input for the demo.

### L6. Slides draft and demo checklist — Sun 8
Slide skeleton (problem, architecture, data, Big Data layer, model, chatbot, evaluation, demo,
limitations); TMK and VNP fill in the numbers.

### Test report template
```markdown
# Test report YYYY-MM-DD
Tester: LTN · Build: commit <hash from `git log -1 --oneline`> · Snapshot: Kaggle version <n>
Machine: Windows 11 + WSL2 / macOS · Browser: <name and version>

| # | Case | Result (pass/fail) | Answer time (s) | Notes / issue |
|---|---|---|---|---|

Summary: <n> pass, <n> fail. Blocking for the demo: <list or "none">.
```

---

## TMK: lead

- [ ] Wed 4: fresh `make kaggle-export && make kaggle-push`; `make export-labeling N=400` and send
      `sample.csv` to LTN; invite VNP and LTN to the repository and the Kaggle dataset.
- [ ] Thu 5: answer latency (20-50 s now): measure where the time goes, fix the largest part.
- [ ] Fri 6: `evaluation/run_eval.py` with tests (recall@5, MRR, refusals), reading `expected_urls`.
- [ ] Sat 7: task 8 numbers on LTN's 50 questions; freeze the corpus snapshot for the report.
- [ ] Review and merge VNP's and LTN's PRs within a day; reply to every bug issue.
- [ ] Sun 8 / Mon 9: demo script, rehearsals, backup video.

## Progress

| Item | Owner | Due | State |
|---|---|---|---|
| Tasks 1-3: crawler running, sources checked, backfill | TMK | Wed 4 | Done |
| V1 Silver | VNP | Thu 5 | Run done 2026-11-05 (`silver-20261105T150259Z`), PR in review |
| V2 Gold | VNP | Fri 6 | Not started |
| V3 Classifier | VNP | Sat 7 | Not started |
| V4 Scaling | VNP | Sun 8 | Not started |
| L1 Test round 1 | LTN | Thu 5 | Not started |
| L2 Golden questions | LTN | Fri 6 | Not started |
| L3 Hand labels | LTN | Fri 6 | Not started (fresh sample pending) |
| L4 Test round 2 | LTN | Sat 7 | Not started |
| L5 Test round 3 | LTN | Mon 9 | Not started |
| Task 8 evaluation | TMK | Sat 7 | Waits for L2 |
| Latency | TMK | Thu 5 | Open |
| Slides, rehearsals, video | All | Mon 9 | Not started |
