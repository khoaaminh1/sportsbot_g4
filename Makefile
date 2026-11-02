.PHONY: help setup lint format test test-integration run up-core up-bigdata up up-collect collect-once sources-check bronze-ls articles-export chat chat-no-llm down ps logs hdfs-init hdfs-ls spark-smoke clean-volumes up-stream up-ai up-tools up-demo models discover-once backfill sources-sitemaps crawl-stats dead-letters logs-stream ask ask-no-llm

PYTHON ?= python

# Host commands (make ask, make test-integration, ...) see the same settings as the stack.
ifneq (,$(wildcard .env))
include .env
export
endif
COMPOSE ?= docker compose
ALL_PROFILES = --profile core --profile bigdata --profile collect --profile stream --profile ai --profile tools
STREAM = --profile core --profile stream
SINCE ?= 2026-10-01
MAX ?= 5000

help:  ## List targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'

setup:  ## Install dev tools and backend dependencies, install git hooks
	$(PYTHON) -m pip install -r requirements-dev.txt -r backend/requirements.txt -r pipelines/requirements.txt -r agent/requirements.txt
	pre-commit install

lint:  ## Static checks (ruff)
	ruff check .
	ruff format --check .

format:  ## Auto-format and fix lint issues
	ruff format .
	ruff check --fix .

test:  ## Unit and contract tests (no Docker needed)
	$(PYTHON) -m pytest

test-integration:  ## Tests against the running core stack (make up-core first)
	SPORTSBOT_INTEGRATION=1 $(PYTHON) -m pytest -m integration

run:  ## Backend on the host with auto-reload (needs Postgres and Qdrant, e.g. make up-core)
	$(PYTHON) -m uvicorn app.main:app --app-dir backend/src --reload --port 8000

up-core:  ## Start PostgreSQL, Qdrant and the backend (waits for health checks)
	$(COMPOSE) --profile core up -d --build --wait

up-bigdata:  ## Start HDFS (NameNode + 2 DataNodes) and Spark (master + worker), then create HDFS dirs
	$(COMPOSE) --profile bigdata up -d --wait namenode datanode1 datanode2 spark-master spark-worker
	$(COMPOSE) --profile bigdata run --rm hdfs-init

up:  ## Start everything (needs about 8-10 GB RAM, estimate)
	$(COMPOSE) --profile core --profile bigdata up -d --build --wait namenode datanode1 datanode2 spark-master spark-worker postgres qdrant backend
	$(COMPOSE) --profile bigdata run --rm hdfs-init

up-collect:  ## Start the collector scheduler (also starts core and bigdata services it needs)
	$(COMPOSE) $(ALL_PROFILES) up -d --build --wait postgres namenode datanode1 datanode2
	$(COMPOSE) --profile bigdata run --rm hdfs-init
	$(COMPOSE) $(ALL_PROFILES) up -d --build collector

collect-once:  ## One collection pass over all enabled sources into HDFS Bronze (stack must be up)
	$(COMPOSE) $(ALL_PROFILES) run --rm --build collector collect

sources-check:  ## Fetch robots.txt and every feed once, print a table (no writes)
	$(COMPOSE) $(ALL_PROFILES) run --rm --no-deps --build collector sources check

bronze-ls:  ## List Bronze files in HDFS
	$(COMPOSE) --profile bigdata exec -T namenode hdfs dfs -ls -R /sportsbot/bronze

articles-export:  ## Copy collected Bronze articles from HDFS to data/articles/ for the chat prototype
	mkdir -p data/articles
	$(COMPOSE) --profile bigdata exec -T namenode hdfs dfs -text "/sportsbot/bronze/*/*/*/*.jsonl.gz" > data/articles/bronze-export.jsonl
	@echo "Exported $$(wc -l < data/articles/bronze-export.jsonl | tr -d ' ') records to data/articles/bronze-export.jsonl"

chat:  ## Terminal chat: answers only from stored articles, with links (needs Ollama running)
	PYTHONPATH=agent/src $(PYTHON) -m sportsbot_agent.main

chat-no-llm:  ## Same, but only shows matching article excerpts (no model needed)
	PYTHONPATH=agent/src $(PYTHON) -m sportsbot_agent.main --no-llm

# ------------------------------------------------------------ streaming (ADR 0005) ----
up-stream:  ## Start PostgreSQL, Qdrant, backend, Kafka, topics, discovery, extractor, consumers
	$(COMPOSE) $(STREAM) up -d --build --wait postgres qdrant kafka
	$(COMPOSE) $(STREAM) run --rm kafka-init
	$(COMPOSE) $(STREAM) up -d --build backend discover extractor consumer-operational consumer-semantic consumer-archive

up-ai:  ## Start the BGE-M3 embedder and bge-reranker (needs `make models` first)
	$(COMPOSE) --profile ai up -d --wait embedder reranker

up-tools:  ## Kafka UI on http://localhost:8085
	$(COMPOSE) $(STREAM) --profile tools up -d kafka-ui

up-demo: up-stream up-ai  ## Everything for the demo: stream + AI services (no HDFS/Spark)

models:  ## Download the GGUF models (BGE-M3 ~620 MB, bge-reranker-v2-m3 ~640 MB) into ./models
	bash scripts/download_models.sh

discover-once:  ## One RSS discovery pass into Kafka now (the discover service also runs on its own)
	$(COMPOSE) $(STREAM) run --rm --build discover discover --once

backfill:  ## Queue older articles from sitemaps: make backfill SINCE=2026-10-01 MAX=5000 [SOURCE=id]
	$(COMPOSE) $(STREAM) run --rm --build discover backfill --since $(SINCE) --max $(MAX) $(if $(SOURCE),--source $(SOURCE),)

sources-sitemaps:  ## Print the Sitemap: lines of each source's robots.txt (to fill sources.json)
	$(COMPOSE) $(STREAM) run --rm --no-deps --build discover sources sitemaps

crawl-stats:  ## Articles per source in PostgreSQL, chunks in Qdrant
	$(COMPOSE) $(STREAM) run --rm --no-deps discover stats

dead-letters:  ## Show the last 20 dead-letter messages (why articles were rejected)
	$(COMPOSE) $(STREAM) exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server kafka:9092 --topic articles.dead-letter --from-beginning \
		--max-messages 20 --timeout-ms 5000 || true

logs-stream:  ## Follow the streaming workers' logs
	$(COMPOSE) $(STREAM) logs -f discover extractor consumer-operational consumer-semantic consumer-archive

ask:  ## Ask the hybrid RAG from the terminal: make ask Q="Kết quả Việt Nam - Thái Lan?"
	PYTHONPATH=agent/src $(PYTHON) -m sportsbot_agent.ask "$(Q)"

ask-no-llm:  ## Retrieval only (no LLM): see which articles the RAG finds
	PYTHONPATH=agent/src $(PYTHON) -m sportsbot_agent.ask --no-llm "$(Q)"

down:  ## Stop all services (named volumes keep the data)
	$(COMPOSE) $(ALL_PROFILES) down

ps:  ## Show service status
	$(COMPOSE) $(ALL_PROFILES) ps

logs:  ## Follow logs of all services
	$(COMPOSE) $(ALL_PROFILES) logs -f

hdfs-init:  ## Create /sportsbot/{bronze,silver,gold} in HDFS (idempotent)
	$(COMPOSE) --profile bigdata run --rm hdfs-init

hdfs-ls:  ## List /sportsbot in HDFS
	$(COMPOSE) --profile bigdata exec -T namenode hdfs dfs -ls -R /sportsbot

spark-smoke:  ## Run a Spark job on the cluster that writes and reads Parquet on HDFS
	$(COMPOSE) --profile bigdata exec -T spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 /opt/sportsbot/pipelines/spark/jobs/hdfs_smoke.py

clean-volumes:  ## DANGER: stop everything and delete all data volumes
	$(COMPOSE) $(ALL_PROFILES) down -v
