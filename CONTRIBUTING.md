# Contributing

Who does what and by when: [docs/TASKS.md](docs/TASKS.md). Rules for people and AI assistants:
[AGENTS.md](AGENTS.md).

## Workflow
1. Pick your item in `docs/TASKS.md` (or an issue). `git checkout main && git pull`.
2. Branch from `main`: `feature/<slug>`, `fix/<slug>` or `docs/<slug>` (e.g. `feature/vnp-silver-job`).
3. Work inside your module (module map in `AGENTS.md`). Run `make lint test`
   (and `make test-integration` if you touched the database or the stack).
4. Open a pull request with the template: what changed, the exact commands you ran and what they
   printed. Link the TASKS item or issue.
5. Both CI checks (`lint-test`, `stack`) must be green. TMK reviews and merges. Nobody pushes to
   `main` or ticks "bypass rules".
6. If the PR changes the state of a component or task, update the Status section of `README.md`
   and tick the item in `docs/TASKS.md` in the same PR.

Credit: commits count for the person whose GitHub account e-mail is in `git config user.email`.
Without the command line, edit or upload files on GitHub and choose "Create a new branch ... and
start a pull request".

## Commit messages
[Conventional Commits](https://www.conventionalcommits.org/), English, imperative:
```
feat: add Spark Silver job with MinHashLSH near-duplicate clusters
fix: count only enabled sources as active
docs: add test report for round 2
```

## Rules
- Never commit secrets, `.env`, data files (articles, Parquet, CSV with article text), model
  weights, indexes or notebook outputs. See `docs/DATA.md`.
- Never fabricate data, benchmarks or results. Label synthetic and public historical data by origin.
- New dependency: add it pinned to the module's `requirements.txt` and mention it in the PR.
- Code written with AI tools is still yours: read the whole diff, run the tests, be able to explain it.

## Reviewing
Read the diff, not only the description. Check: scope, tests for new behaviour, run reports for
numbers, docs and contracts updated, no secrets or data files, files only in the author's module.
