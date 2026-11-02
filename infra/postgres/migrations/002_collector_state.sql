-- 002_collector_state.sql: state for incremental collection (Phase 3).
-- Applied by `python -m sportsbot_pipelines migrate` on existing databases, and by the postgres
-- image on new volumes (files run in name order).

BEGIN;

-- Conditional-GET state per source, so unchanged feeds cost a 304.
CREATE TABLE collector_state (
    source_id      TEXT PRIMARY KEY REFERENCES sources (source_id),
    etag           TEXT,
    last_modified  TEXT,
    last_run_at    TIMESTAMPTZ,
    last_success_at TIMESTAMPTZ
);

-- Every article id already written to Bronze, per source: re-running never duplicates Bronze rows.
CREATE TABLE seen_items (
    source_id   TEXT NOT NULL REFERENCES sources (source_id),
    record_id   TEXT NOT NULL,           -- sha256 of the canonical URL (article contract)
    first_seen  TIMESTAMPTZ NOT NULL DEFAULT now(),
    run_id      TEXT NOT NULL,
    PRIMARY KEY (source_id, record_id)
);

ALTER TABLE collection_runs
    ADD COLUMN run_key        TEXT UNIQUE,   -- the run id used in Bronze file names
    ADD COLUMN items_dropped  INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN duration_ms    INTEGER;

INSERT INTO schema_migrations (version, description) VALUES ('002', 'collector state');

COMMIT;
