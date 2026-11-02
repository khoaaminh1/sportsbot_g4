"""Kafka topics of the ingestion pipeline (docs/decisions/0005-streaming-pipeline-kafka.md).

    sources.json -> RSS / sitemap discovery -> articles.discovered
    extractor (fetch, extract, de-duplicate) -> articles.extracted | articles.dead-letter
    articles.extracted -> operational consumer (PostgreSQL)
                       -> semantic consumer (chunks -> BGE-M3 -> Qdrant)

Messages are JSON objects. The key is the source id, so every article of one site lands in the
same partition and one extractor instance handles it: per-host politeness holds even when the
extractor is scaled out.
"""

from __future__ import annotations

DISCOVERED = "articles.discovered"
EXTRACTED = "articles.extracted"
DEAD_LETTER = "articles.dead-letter"

ALL_TOPICS = (DISCOVERED, EXTRACTED, DEAD_LETTER)

# Consumer groups: each group gets its own copy of the stream.
GROUP_EXTRACTOR = "sportsbot-extractor"
GROUP_OPERATIONAL = "sportsbot-operational"
GROUP_SEMANTIC = "sportsbot-semantic"

DISCOVERED_SCHEMA = "discovered.v1"
EXTRACTED_SCHEMA = "extracted.v1"
DEAD_LETTER_SCHEMA = "dead_letter.v1"
