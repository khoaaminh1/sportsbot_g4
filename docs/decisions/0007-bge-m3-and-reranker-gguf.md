# 0007. BGE-M3 embeddings and bge-reranker-v2-m3 as GGUF models on llama.cpp

- Status: accepted (2026-11-02, TMK)
- Date: 2026-11-02
- Deciders: TMK, VNP, LTN
- Replaces: `intfloat/multilingual-e5-small` in the Phase 1 plan

## Context
- The team chose BGE-M3 and a GGUF reranker for both stores (PostgreSQL keyword search and
  Qdrant vectors). BGE-M3: multilingual (Vietnamese included), 1024-dim dense vectors, 8192
  tokens. bge-reranker-v2-m3: multilingual cross-encoder (568M parameters).
- llama.cpp `llama-server` serves embeddings (`--embedding --pooling cls`, OpenAI
  `/v1/embeddings`) and reranking (`--reranking --embedding --pooling rank`, `/v1/rerank`).
- Q8_0 files: BGE-M3 about 620 MB, reranker about 640 MB (Hugging Face listings). Q8_0 keeps
  embeddings very close to f16 (published cosine ≥ 0.999 for one BGE-M3 GGUF build).

## Decision
- `embedder` and `reranker` services (profile `ai`) run `ghcr.io/ggml-org/llama.cpp:server`
  with the GGUF files from `./models` (`make models`). The image is configurable
  (`LLAMA_CPP_IMAGE`); on a Mac a native `llama-server` (Homebrew, Metal) can replace them via
  `STACK_EMBEDDING_BASE_URL` / `STACK_RERANK_BASE_URL`.
- Retrieval (`agent/hybrid.py`): PostgreSQL full-text search (unaccented, `simple` config) and
  Qdrant search on BGE-M3 vectors, fused by Reciprocal Rank Fusion (k = 60), then reranked;
  candidates below `RERANK_MIN_SCORE` are dropped. Each stage degrades gracefully.
- llama.cpp serves BGE-M3's dense vectors only (no sparse or ColBERT outputs); the keyword side
  comes from PostgreSQL instead.

## Consequences
- No Python ML runtime in the serving path; the same two containers run on macOS and Windows.
- CPU-only inside Docker on a Mac: embedding a large backlog is slow. Measure articles per
  minute on the first run and switch to native `llama-server` if needed.
- `RERANK_MIN_SCORE` (-2.0) is a starting guess; tune it on the golden question set and
  record the rule before looking at the results.
- Not yet run with the real models in this repository's CI (model downloads are too large);
  the clients are tested against recorded API shapes, and Qdrant against a real server.
