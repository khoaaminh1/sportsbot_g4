# Runbook: streaming pipeline, AI services and the demo

Covers ADR 0005-0007. Works on macOS, Intel or Apple Silicon (Docker Desktop or OrbStack), and
Windows (Docker Desktop with WSL2: clone and run the commands inside Ubuntu, not in PowerShell).
Commands run from the repository root. CI runs the stack on x86_64 (Intel/AMD) Linux.

Intel CPUs: the embedder and reranker run on the CPU. Give Docker at least 8 GB of memory and
4 CPUs (Docker Desktop → Settings → Resources), and expect slower embedding than on Apple
Silicon; `make crawl-stats` shows whether vectors keep up with new articles.

## 0. One-time setup

```bash
git fetch origin
git checkout feature/stream-rag-skeleton
cp .env.example .env            # then edit .env: GEMINI_API_KEY=..., COLLECTOR_CONTACT=team email
make setup                      # Python tools for tests and `make ask`
make test                       # expect: all passed, some skipped
make models                     # ~1.3 GB of GGUF files into ./models (only once)
```

Get a free Gemini key at https://aistudio.google.com/apikey (personal Google account, no card).
Put it only in `.env`; never in code, commits or chat messages.

## 1. Start everything for the demo

```bash
make up-demo        # PostgreSQL, Qdrant, backend, Kafka + topics, workers, embedder, reranker
make ps             # all "running"/"healthy"; kafka-init exits after creating the topics
make llm-check      # which Gemini models answer with your key
```

Open:
- Demo page (news feed + chat): http://localhost:8000/app/
- API docs: http://localhost:8000/docs
- Kafka UI (optional, `make up-tools`): http://localhost:8085

Memory (estimate): about 3.5-4 GB for `up-demo`. HDFS + Spark (`make up-bigdata`) add about
1.5 GB idle and up to 2 GB while a job runs.

## 2. Fill the corpus

The `discover` service polls every RSS source on its interval by itself. To go faster:

```bash
make discover-once                       # one RSS pass now
make sources-sitemaps                    # print Sitemap: URLs from each site's robots.txt
# copy useful sitemap URLs into pipelines/sources.json ("sitemaps", "sitemap_include"), then:
make backfill SINCE=2026-10-01 MAX=3000  # queue older sports articles (sitemaps + listings)
make backfill SINCE=2026-10-01 MAX=3000 SOURCE=dantri_the_thao  # one source only
make crawl-stats                         # articles per source, chunks in Qdrant
make dead-letters                        # why some articles were rejected
make logs-stream                         # follow the workers
```

Sites whose article URLs have no section path (VnExpress, Tuoi Tre, VietNamNet) cannot filter
their sitemaps to sports, so their `listing` entry walks the paginated sports section instead
(`/the-thao`, `/the-thao-p2`, ...) up to `max_pages`, stopping after two pages without new links.
Listing pages carry no dates, so `SINCE` does not limit them. Each source prints a JSON line
with `found` and `queued`; running backfill again queues only URLs not seen before.

Politeness is not optional: robots.txt is respected and each site gets at most one request
every `MIN_SECONDS_BETWEEN_REQUESTS_PER_HOST` seconds (default 3). With 20 sites that is up to
about 20 articles per 3 seconds in total. To use more sites in parallel, scale the extractor:

```bash
docker compose --profile core --profile stream up -d --scale extractor=3 extractor
```

VnExpress answers only from Vietnamese IP addresses: run the crawler from Vietnam.

## 2b. Monitoring and alerts

The `monitor` service (started by `make up-stream`) checks every 15 minutes and sends a
message when something goes wrong, and again when it is fixed:

| Alert | Meaning |
|---|---|
| `source_failing:<id>` | 3+ failed runs in a row; the scheduler backs off (doubling wait, max 6 h) |
| `source_stale:<id>` | no successful run for 3 h |
| `no_new_articles` | nothing stored for 2 h |
| `embedding_stalled` | articles grow but Qdrant does not, for 30 min |
| `qdrant_down` | Qdrant unreachable |
| `dead_letters_spike` | 50+ rejected articles since the last check (top reasons listed) |

Set one or both channels in `.env`, then `make up-stream` and `make alert-test`:
- Discord: channel → Edit Channel → Integrations → Webhooks → New Webhook → Copy URL →
  `ALERT_DISCORD_WEBHOOK_URL=...`
- Telegram: @BotFather → /newbot → token → `ALERT_TELEGRAM_BOT_TOKEN=...`; add the bot to the
  group, send a message, open `https://api.telegram.org/bot<token>/getUpdates` and copy
  `chat.id` → `ALERT_TELEGRAM_CHAT_ID=...`

`make monitor-once` runs one check now and prints the snapshot.

Keep it running unattended (Windows): Docker Desktop → Settings → General → "Start Docker
Desktop when you sign in"; Windows → Settings → System → Power → Screen and sleep → "Never"
for sleep when plugged in. Containers restart by themselves (`restart: unless-stopped`).

## 3. Ask questions

```bash
make ask Q="Kết quả trận Việt Nam gặp Thái Lan?"
make ask-no-llm Q="Hà Nội FC"       # which articles the retrieval finds, without the LLM
```

The answer lists sources with titles, publication times and links. "no_evidence" means no
stored article matched: the LLM was not called.

## 4. Faster AI services on an Apple Silicon Mac (optional)

Only for M1-M5 Macs: Docker has no GPU access, native llama.cpp uses Metal. On Intel Macs and
Windows PCs keep the Docker services (CPU); they are the tested path (`linux/amd64`).

```bash
brew install llama.cpp
llama-server -m models/bge-m3-Q8_0.gguf --embedding --pooling cls -c 8192 -b 8192 -ub 8192 --port 8091 &
llama-server -m models/bge-reranker-v2-m3-Q8_0.gguf --reranking --embedding --pooling rank -c 8192 -b 8192 -ub 8192 --port 8092 &
# in .env:
#   STACK_EMBEDDING_BASE_URL=http://host.docker.internal:8091
#   STACK_RERANK_BASE_URL=http://host.docker.internal:8092
make up-stream      # without the ai profile
```

## 5. Big Data layer (Spark on the archive)

```bash
STREAM_BRONZE_BACKEND=hdfs   # in .env, then:
make up-bigdata && make up-stream
make bronze-ls               # source=extracted/... files written by consumer-archive
```

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `consumer-semantic` logs `dependency_unavailable_retrying` | Embedder or Qdrant down. It waits and resumes; start `make up-ai`. |
| Chat says "no matching article" for everything | Corpus empty or not embedded yet: `make crawl-stats`. |
| `llm-check`: all Gemini targets fail with 401/403 | Wrong or missing `GEMINI_API_KEY` in `.env`. |
| `llm-check`: a Gemini model "model not found" | The name changed; edit `GEMINI_MODELS` in `.env`. |
| Many `http_403` dead letters for one site | The site blocks crawlers: disable the source in `sources.json`. |
| `embedder` container exits immediately | Model file missing: `make models`; check `ls models/`. |
| Embedder logs HTTP 500 `unordered_map::at` | Old BGE-M3 GGUF (tokenizer entries missing). `git pull`, `make models` (downloads `bge-m3-Q8_0.gguf`), `make up-ai`, `make reindex`. The client also skips single bad chunks now. |
| `consumer-semantic` logs `dependency_unavailable_gave_up` | The embedder or Qdrant was down for 10+ minutes. Start them, then `make reindex` (builds only missing vectors). |
| Dead letters with `network_error` / `"retryable": true` | Temporary network problem or 429/5xx. `make replay-dead-letters` sends them through again. |
| A site's article pages are blocked by the network (TLS reset, also with `curl`) | Set `"extract": false` for that source in `sources.json`: title + feed summary are stored instead (done for BBC on 2026-11-03). |
| Kafka does not start after changing `CLUSTER_ID` | Remove its volume: `docker volume rm sportsbot_kafka_data`. |
