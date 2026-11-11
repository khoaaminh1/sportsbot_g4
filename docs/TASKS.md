# Work split until the presentation (Thursday 12 November 2026)

One page per person: what to deliver, where, by when, and how to know it is done.
Updated 2026-11-11 (presentation Thursday 12 November; code freeze Wednesday 11, 12:00, done:
only docs and fixes after it). Original deadlines are kept below in the done items. The live progress board is the Status section of `README.md`;
when you finish a step, tick it here **and** update that board in the same pull request.

| Person | Role | Delivers |
|---|---|---|
| **TMK** (Trần Minh Khoa) | Lead, pipeline, RAG, demo | Crawler PC, data snapshots, retrieval evaluation, latency, merges, demo |
| **VNP** (Võ Ngọc Phú) | Big Data engineer | Spark Silver + Gold (classifier moved to TMK, scaling cut) |
| **LTN** (Lê Tấn Nguyên) | Tester | Test rounds with reports (Markdown via GitHub web) and bug issues |

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

| Day | VNP | LTN (tester) | TMK |
|---|---|---|---|
| Mon 9 | Pull numpy image; classifier baseline on weak labels | Accounts, invitations, setup on own machine (test round 1) | 200 hand labels; numpy PR merged; fresh Kaggle snapshot; invite LTN |
| Tue 10 | Classifier evaluated on the hand labels, report; scaling runs (2 sizes) | Round 1 report by 12:00; test round 2 (13 cases), report by 21:00 | Fix test-round bugs; slides (pipeline, RAG, evaluation) |
| Wed 11 | Slides (Spark, classifier); fixes only after 12:00 | Test round 3 on the frozen build, report by 20:00 | **12:00 code freeze**; backup demo video; rehearsal |
| Thu 12 | Presentation | Presentation | Presentation |

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
- [x] `pipelines/spark/jobs/gold_stats.py`: articles per source × day × language, exact and near
      duplicate rates, cluster size distribution, freshness lag percentiles.
- [x] Run on the cluster; commit `docs/reports/runs/gold-<run_id>.json`.
- [x] Add one table of Gold numbers to the README Status section (with the run id).

### V3. Sport classifier (plan task 7) — due **Tue 10, 18:00** (taken over by TMK on 2026-11-09)
Branch `feature/sport-classifier`. VNP finished Silver and Gold; TMK runs this task.
- [x] Weak labels from `source_id`, URL paths and (volleyball/badminton only) title keywords;
      final mapping in `ml/README.md` (model card).
- [x] Test set: hand labels (`ml/labels/sport_test_v1.csv`), excluded from training; plus a
      weak-label holdout reported separately.
- [x] Baseline: Spark MLlib TF-IDF + logistic regression. Improved: BGE-M3 vectors (from Qdrant)
      + logistic regression. Code: `pipelines/spark/jobs/sport_classifier.py`.
- [x] Test labels committed (197, labelled by Claude, reviewed by TMK) and loaded: `make labels-load`.
- [x] Run on the real Silver: run `20261108T213620Z`, report in `docs/reports/runs/`; numbers in
      the README Status section.
- [ ] Optional, if time: script that fills `articles.sport_category` with the better model.

### V4. Scaling experiment (plan task 9) — **cut on 2026-11-09** (TMK decision; not needed for the presentation)
- [ ] Silver job at 3 sizes of the real corpus (e.g. 25 %, 50 %, 100 %), `local[*]` and the cluster.
- [ ] Table: rows, duration, rows/s, storage per layer; say "single machine, pseudo-distributed (Docker)".

### V5. Slides — Wed 11
Two or three slides: Bronze → Silver → Gold with real counts, duplicate findings, classifier
results, scaling table. Numbers only from your run reports.

---

## LTN: tester and evaluation data

LTN has no AI assistant: every step below is something to open, type, check or fill in.
A Vietnamese step-by-step guide for LTN is shared by TMK (Claude doc "Hướng dẫn việc của LTN").
Setup: [TEAM_ONBOARDING.md](TEAM_ONBOARDING.md) sections 1-4 (`make team-data`).

### L1. Test round 1: setup on a clean machine — **Mon 9**, report by **Tue 10, 12:00**
Testing the onboarding guide is itself a test: a teammate must be able to run the project from it.
- [ ] Follow TEAM_ONBOARDING sections 1-4 exactly as written. Note the time each section takes.
- [ ] Every step that fails or is unclear: open a GitHub issue with the **Bug** template (the exact
      command, the output, Windows/macOS). No screenshots of `.env` or tokens.
- [ ] Result: `make team-data` finishes and http://localhost:8000/app/ answers a question.

### L2. Golden questions (plan task 4) — **moved to TMK** on 2026-11-06
50 questions drafted in `evaluation/golden_questions.csv`; TMK judges relevance with
`make eval-pool` / `make eval-apply` (`evaluation/README.md`). The steps below are kept for reference.
File: `evaluation/golden_questions.csv` (rules in `evaluation/README.md`).
- [ ] 50 questions: 35 answerable, 15 not answerable; at least 15 in English; at least 10 typed
      without diacritics; several sports, not only football.
- [ ] Write the question first, then find the answering articles in the web app; copy each
      article's link into `expected_urls` (several links separated by `;`).
- [ ] Commit through a pull request (GitHub web upload is fine).

### L3. Test labels (plan task 5) — **done by TMK** on 2026-11-09
197 labels in `ml/labels/sport_test_v1.csv`, proposed by Claude and reviewed by TMK (origin in
`ml/README.md`). The steps below are kept for reference.
- [ ] TMK sends a fresh `sample.csv` (400 articles from the multi-sport corpus).
- [ ] In Google Sheets fill `sport_label` with exactly one of
      `football, volleyball, basketball, tennis, badminton, other_sport`; `labeler` = `LTN`;
      unsure → write why in `notes`.
- [ ] Commit **only** the columns `article_id,sport_label,labeler,notes` as
      `ml/labels/sport_test_v1.csv` (titles and excerpts are publishers' text and must not go into
      the public repository).

### L4. Test round 2: the whole app — report by **Tue 10, 21:00**
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

- [ ] Write the results in `docs/reports/test-report-2026-11-10.md` (template below) and open a PR.
- [ ] One GitHub issue per failure (Bug template), linked in the report.

### L5. Test round 3: regression on the frozen build — **Wed 11**, after 12:00, report by 20:00
- [ ] Repeat the L4 table on the code-freeze version; check that every issue marked fixed is fixed.
- [ ] `docs/reports/test-report-2026-11-11.md` via PR. This is the go/no-go input for the demo.

### L6. Demo checklist — Wed 11 (optional)
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

- [x] Golden questions, judgements, first evaluation (PRs #32-#35, 2026-11-06).
- [x] Mon 9: test labels (197, AI-assisted, reviewed; `ml/labels/sport_test_v1.csv`).
- [x] Mon 9: sport classifier, Silver and Gold reruns on one snapshot (PRs #38-#40).
- [x] Mon 9: merge the numpy Spark image PR (#37).
- [ ] Mon 9: `make kaggle-export && make kaggle-push`; invite LTN to
      the repository and the Kaggle dataset.
- [ ] Tue 10: answer test-round bugs. Optional items (reranker threshold, VietNamNet menu-text
      fix) cut on 2026-11-09.
- [ ] Tue 10 / Wed 11: slides, demo script, 12:00 Wednesday code freeze, backup video, rehearsal.

## Progress

| Item | Owner | Due | State |
|---|---|---|---|
| Tasks 1-3: crawler running, sources checked, backfill | TMK | Wed 4 | Done (about 15,000 articles on 2026-11-09, per TMK) |
| V1 Silver | VNP | Thu 5 | Done 2026-11-05 (PR #28) |
| V2 Gold | VNP | Fri 6 | Done 2026-11-05 (PR #30) |
| L2 Golden questions | TMK (from LTN) | Sat 7 | Done 2026-11-06 (PR #34) |
| Task 8 evaluation | TMK | Sat 7 | First run done 2026-11-06 (README Status) |
| L3 Test labels | TMK (from LTN) | Mon 9 | Done 2026-11-09 (197, AI-assisted, reviewed) |
| V3 Classifier | TMK | Tue 10 | Done 2026-11-09 (run `20261108T213620Z`) |
| V4 Scaling | VNP | - | Cut 2026-11-09 (not needed) |
| L1 Test round 1 | LTN | Tue 10 12:00 | Pending: no report in the repository on 2026-11-11 |
| L4 Test round 2 | LTN | Tue 10 21:00 | Pending: no report in the repository on 2026-11-11 |
| L5 Test round 3 | LTN | Wed 11 20:00 | Pending: no report in the repository on 2026-11-11 |
| Slides, rehearsals, video | All | Wed 11 | In progress 2026-11-11: slides (about 8) tonight |
