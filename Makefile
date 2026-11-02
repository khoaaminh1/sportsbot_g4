.PHONY: help setup lint format test test-integration run up-core up-bigdata up up-collect collect-once sources-check bronze-ls down ps logs hdfs-init hdfs-ls spark-smoke clean-volumes

PYTHON ?= python
COMPOSE ?= docker compose
ALL_PROFILES = --profile core --profile bigdata --profile collect

help:  ## List targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'

setup:  ## Install dev tools and backend dependencies, install git hooks
	$(PYTHON) -m pip install -r requirements-dev.txt -r backend/requirements.txt -r pipelines/requirements.txt
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
