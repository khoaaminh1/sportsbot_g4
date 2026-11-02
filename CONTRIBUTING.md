# Contributing

## Workflow
1. Take an issue assigned to you (or open one). Every change starts from an issue with acceptance criteria.
2. `git pull`, then create a branch from the integration branch (`develop` once it exists, `main` until then):
   `feature/<issue>-<slug>` (e.g. `feature/12-rss-collector`), `fix/<issue>-<slug>` or `docs/<issue>-<slug>`.
3. Implement within the issue's scope. Run `make lint test` (and `make test-integration` if you touched the stack).
4. Open a Pull Request with the template. Link the issue with `Refs #<n>`, or `Closes #<n>` in the PR that finishes it.
5. Wait for CI to pass. Merge only on a green check (squash merge).
6. Ask another member to review. Review is **required** for: contracts (`docs/contracts/`), database migrations, Spark jobs, `docker-compose.yml`, and evaluation runs or results. For other PRs it is recommended.

## Branches
- `main`: protected; receives a tagged merge at each phase gate (`v0.2-infra`, `v0.3-data`, ...).
- `develop`: integration branch for feature PRs (to be created when Phase 2 is merged).
- Feature branches are short-lived; keep PRs small (one issue, ideally under ~400 changed lines).

## Commit messages
[Conventional Commits](https://www.conventionalcommits.org/), English, imperative:
```
feat: add VnExpress RSS collector
fix: keep original published_at string when parsing fails
docs: record ADR 0003 embedding model
```

## Rules
- Never commit secrets, `.env`, data files, model weights, indexes or notebook outputs.
- Never fabricate data, benchmarks or results. Label synthetic and public historical data by origin.
- New dependency: add it to the module's `requirements.txt` (pinned) and mention it in the PR.
- If AI tools produced code, you are still the author: read the whole diff, run the tests, and be able to explain every line.

## Reviewing
Read the diff, not only the description. Check: scope matches the issue, acceptance criteria met, tests cover new behaviour, contracts/docs updated, no secrets or data files, logs and errors follow the shared conventions.

## Local setup
See the Quick start in `README.md` and `docs/runbooks/stack.md`.
