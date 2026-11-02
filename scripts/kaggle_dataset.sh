#!/usr/bin/env bash
# Share the crawled corpus as a PRIVATE Kaggle dataset (versioned), or download it.
#
#   bash scripts/kaggle_dataset.sh push   # TMK, on the crawler PC (after make kaggle-export)
#   bash scripts/kaggle_dataset.sh pull   # teammates
#
# Needs in .env: KAGGLE_DATASET=<owner>/<slug> and either KAGGLE_API_TOKEN (new tokens,
# Kaggle → Settings → API → Create New Token) or a legacy ~/.kaggle/kaggle.json.
# The dataset stays private: full news text is for the team's research only, never public.
set -euo pipefail
cd "$(dirname "$0")/.."

PY="${PYTHON:-.venv/bin/python}"
[[ -x "$PY" ]] || PY=python3
DIR=data/kaggle
: "${KAGGLE_DATASET:?set KAGGLE_DATASET=<kaggle-username>/sportsbot-corpus in .env}"

if ! "$PY" -m kaggle --version >/dev/null 2>&1 && ! command -v kaggle >/dev/null 2>&1; then
  echo "installing the kaggle CLI into the virtualenv"
  "$PY" -m pip install -q kaggle
fi
KAGGLE="$(dirname "$PY")/kaggle"
[[ -x "$KAGGLE" ]] || KAGGLE=kaggle

case "${1:-}" in
  push)
    [[ -f "$DIR/articles.jsonl.gz" ]] || { echo "run make kaggle-export first" >&2; exit 2; }
    if [[ ! -f "$DIR/dataset-metadata.json" ]]; then
      cat > "$DIR/dataset-metadata.json" <<META
{
  "title": "SportsBot G4 corpus (private)",
  "id": "${KAGGLE_DATASET}",
  "licenses": [{"name": "other"}],
  "description": "PRIVATE team dataset for a university Big Data project (Group 4). Sports news collected by SportsBot from public RSS feeds and article pages, respecting robots.txt. Full text is copyrighted by the publishers and is shared only inside the team for research; do not make this dataset public. Files: articles.jsonl.gz (one article per line), sportsbot.sql.gz (PostgreSQL dump for make db-import), bronze.tgz (Bronze archive for make bronze-load), export-summary.json."
}
META
    fi
    if "$KAGGLE" datasets status "$KAGGLE_DATASET" >/dev/null 2>&1; then
      "$KAGGLE" datasets version -p "$DIR" -m "snapshot $(date -u +%Y-%m-%dT%H:%MZ)" -d
    else
      "$KAGGLE" datasets create -p "$DIR"   # private by default
      echo "Created. Add VNP and LTN: dataset page → Settings → Sharing → collaborators."
    fi
    ;;
  pull)
    mkdir -p "$DIR"
    "$KAGGLE" datasets download -d "$KAGGLE_DATASET" -p "$DIR" --unzip --force
    ls -lh "$DIR"
    echo "Load into your stack: make db-import FILE=$DIR/sportsbot.sql.gz && make reindex"
    ;;
  *)
    echo "usage: $0 push|pull" >&2
    exit 2
    ;;
esac
