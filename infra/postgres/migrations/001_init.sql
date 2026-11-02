-- 001_init.sql: initial application schema.
-- Applied automatically by the postgres image on a NEW volume (files in /docker-entrypoint-initdb.d
-- run in name order). Later migrations are new files (002_..., 003_...) that also insert their
-- version into schema_migrations. Column meanings follow docs/contracts/article.schema.json.

BEGIN;

CREATE TABLE schema_migrations (
    version     TEXT PRIMARY KEY,
    description TEXT NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Source registry (mirrors pipelines/sources.yaml; the YAML is the source of truth).
CREATE TABLE sources (
    source_id        TEXT PRIMARY KEY,                 -- e.g. 'vnexpress_the_thao'
    name             TEXT NOT NULL,
    kind             TEXT NOT NULL CHECK (kind IN ('rss', 'api', 'page')),
    url              TEXT NOT NULL,
    language         TEXT,                             -- ISO 639-1 expected language, e.g. 'vi'
    interval_minutes INTEGER NOT NULL DEFAULT 60 CHECK (interval_minutes >= 5),
    enabled          BOOLEAN NOT NULL DEFAULT TRUE,
    robots_allowed   BOOLEAN,                          -- NULL = not checked yet
    notes            TEXT,                             -- terms of use, licence remarks
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per collector run per source: the failure and freshness statistics live here.
CREATE TABLE collection_runs (
    run_id        BIGSERIAL PRIMARY KEY,
    source_id     TEXT NOT NULL REFERENCES sources (source_id),
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ,
    status        TEXT NOT NULL DEFAULT 'running'
                  CHECK (status IN ('running', 'success', 'partial', 'failed', 'skipped')),
    http_status   INTEGER,
    items_fetched INTEGER NOT NULL DEFAULT 0,
    items_new     INTEGER NOT NULL DEFAULT 0,
    error         TEXT,
    bronze_path   TEXT                                  -- HDFS path written by this run
);
CREATE INDEX collection_runs_source_started_idx ON collection_runs (source_id, started_at DESC);

-- Serving copy of Gold articles (written by the Gold export, read by the API).
-- Full text is kept for retrieval only; the API shows title, excerpt and link.
CREATE TABLE articles (
    article_id       TEXT PRIMARY KEY,                 -- stable id, see the article contract
    title            TEXT NOT NULL,
    content          TEXT,
    summary          TEXT,
    source_id        TEXT REFERENCES sources (source_id),
    source_name      TEXT NOT NULL,
    source_url       TEXT NOT NULL UNIQUE,
    published_at     TIMESTAMPTZ,                      -- NULL when the source gives no date
    collected_at     TIMESTAMPTZ NOT NULL,
    language         TEXT,
    sport_category   TEXT CHECK (sport_category IN
                     ('football', 'volleyball', 'basketball', 'tennis', 'badminton', 'other_sport')),
    teams            TEXT[] NOT NULL DEFAULT '{}',
    athletes         TEXT[] NOT NULL DEFAULT '{}',
    competition      TEXT,
    content_hash     TEXT NOT NULL,
    story_cluster_id TEXT,                             -- near-duplicate group from the Silver job
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX articles_published_idx ON articles (published_at DESC NULLS LAST);
CREATE INDEX articles_sport_published_idx ON articles (sport_category, published_at DESC NULLS LAST);
CREATE INDEX articles_language_idx ON articles (language);
CREATE INDEX articles_content_hash_idx ON articles (content_hash);
CREATE INDEX articles_cluster_idx ON articles (story_cluster_id);

-- Chat history.
CREATE TABLE conversations (
    conversation_id UUID PRIMARY KEY,
    language        TEXT,                              -- preferred answer language
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE messages (
    message_id      BIGSERIAL PRIMARY KEY,
    conversation_id UUID NOT NULL REFERENCES conversations (conversation_id) ON DELETE CASCADE,
    role            TEXT NOT NULL CHECK (role IN ('user', 'assistant')),
    content         TEXT NOT NULL,
    citations       JSONB NOT NULL DEFAULT '[]',        -- [{article_id, source_url, published_at}]
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX messages_conversation_idx ON messages (conversation_id, created_at);

INSERT INTO schema_migrations (version, description) VALUES ('001', 'initial schema');

COMMIT;
