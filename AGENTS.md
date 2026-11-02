# AGENTS.md

Instructions for humans and AI coding agents working in this repository. Keep this file short; details live in `docs/`.

## Project
Vietnam SportsBot: a multilingual (Vietnamese, English, more) sports news chatbot. News is collected from RSS feeds, permitted pages and sports APIs, processed with HDFS + Spark (Bronze/Silver/Gold), indexed for hybrid retrieval, and answered with RAG plus citations. Read first: `README.md`, `docs/decisions/`, `docs/contracts/`.

## Before you start any task
1. `git pull`, read the issue and its acceptance criteria. If something is missing or contradictory, ask in the issue; do not guess.
2. Work on a branch: `feature/<issue>-<slug>`, `fix/<issue>-<slug>` or `docs/<issue>-<slug>`. Never push to `main`.
3. Only edit files inside the module(s) your issue names. Shared files (`docs/contracts/`, `docker-compose.yml`, `.github/`, `pyproject.toml`, `infra/`) change only through a dedicated issue.

## Module map
| Path | Scope | Owner |
|---|---|---|
| `infra/` | HDFS, Spark, PostgreSQL config and migrations | TMK, VNP |
| `pipelines/` | Collectors, Bronze writer, Spark jobs, dedup, enrichment, exports | VNP, LTN |
| `ml/` | Datasets, labels, classifier training and evaluation | LTN |
| `agent/` | Query understanding, retrieval, evidence checks, prompts, LLM client | TMK |
| `backend/` | FastAPI service; must match `docs/contracts/api.openapi.yaml` | TMK |
| `evaluation/` | Golden set, metrics, run files, reports | LTN, TMK |
| `frontend/` | Web UI (Phase 6); talks to the backend only through the API contract | LTN, TMK |

## Commands
- `make setup` install dev tools and dependencies, git hooks
- `make lint` / `make format` ruff checks / fixes
- `make test` unit + contract tests (no Docker)
- `make up-core` / `make up-bigdata` / `make down` Docker stack
- `make test-integration` tests against the running core stack
- `make spark-smoke` Spark job writing and reading Parquet on HDFS
Run `make lint test` before every commit; CI runs the same.

## Conventions
- Python 3.11, type hints on public functions, line length 100, English everywhere (code, logs, docs, issues, PRs). Collected news keeps its original language.
- Conventional Commits: `feat:`, `fix:`, `docs:`, `test:`, `refactor:`, `chore:`.
- Configuration comes from environment variables listed in `.env.example`; add new ones there with a safe placeholder.
- Logs are structured (JSON by default) and carry the request id; errors use the shared error schema.
- The LLM is always called through an OpenAI-compatible client configured by `LLM_*` variables; never hard-code a vendor, model or URL.
- Answers come only from retrieved evidence, cite sources with links and publication times, and say so when evidence is insufficient.
- Update docs and contracts in the same PR as the code that changes them.

## Do not
- Read, print or commit `.env`, API keys, tokens or personal data.
- Commit data files, model weights, indexes or notebook outputs (`data/` is git-ignored).
- Fabricate collected data, benchmarks or training results; label synthetic and public datasets by origin.
- Present single-machine or pseudo-distributed runs as a production cluster.
- Change a contract, schema or public interface without an approved issue.
- Claim something works without running the relevant command and reading its output.

## Definition of done
Acceptance criteria met, `make lint test` passes, CI green, docs/contracts updated, the PR links its issue and says how it was tested, and the author has read the full diff.
