-- 003: columns written by the streaming pipeline + accent-insensitive keyword search.
-- Keyword search uses the 'simple' text-search configuration on unaccented text, so
-- "doi tuyen viet nam" matches "Đội tuyển Việt Nam" and the same index serves English,
-- Spanish and French articles. Vector search lives in Qdrant (collection sportsbot_chunks).
BEGIN;

CREATE EXTENSION IF NOT EXISTS unaccent;

-- unaccent() is STABLE; a generated column needs an IMMUTABLE function with a fixed dictionary.
CREATE OR REPLACE FUNCTION sb_unaccent(value text) RETURNS text
    LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT
    AS $$ SELECT public.unaccent('public.unaccent'::regdictionary, value) $$;

ALTER TABLE articles
    ADD COLUMN author         TEXT,
    ADD COLUMN word_count     INTEGER,
    ADD COLUMN discovered_via TEXT,          -- rss | sitemap
    ADD COLUMN extracted_at   TIMESTAMPTZ;

ALTER TABLE articles
    ADD COLUMN search_tsv tsvector GENERATED ALWAYS AS (
        setweight(to_tsvector('simple', sb_unaccent(coalesce(title, ''))), 'A')
        || setweight(to_tsvector('simple', sb_unaccent(left(coalesce(content, ''), 20000))), 'B')
    ) STORED;

CREATE INDEX articles_search_idx ON articles USING GIN (search_tsv);
CREATE INDEX articles_ingested_idx ON articles (ingested_at DESC);
CREATE INDEX articles_source_published_idx ON articles (source_id, published_at DESC NULLS LAST);

INSERT INTO schema_migrations (version, description)
VALUES ('003', 'streaming pipeline columns and accent-insensitive keyword search');

COMMIT;
