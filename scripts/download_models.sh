#!/usr/bin/env bash
# Download the GGUF models used by the `ai` profile into ./models (about 1.3 GB in total).
# Works on macOS, Linux and Windows (Git Bash / WSL). Re-running skips files already present.
# Override the source with EMBEDDING_GGUF_URL / RERANK_GGUF_URL if a mirror is needed.
set -euo pipefail

cd "$(dirname "$0")/.."
mkdir -p models

EMBEDDING_GGUF_URL="${EMBEDDING_GGUF_URL:-https://huggingface.co/vonjack/bge-m3-gguf/resolve/main/bge-m3-q8_0.gguf}"
RERANK_GGUF_URL="${RERANK_GGUF_URL:-https://huggingface.co/gpustack/bge-reranker-v2-m3-GGUF/resolve/main/bge-reranker-v2-m3-Q8_0.gguf}"

fetch() {
  local url="$1" out="models/$2" min_mb="$3"
  if [[ -f "$out" ]]; then
    echo "exists: $out ($(du -h "$out" | cut -f1))"
    return
  fi
  echo "downloading $out"
  echo "  from $url"
  curl -fL --retry 3 -C - -o "$out.part" "$url"
  local size_mb=$(( $(wc -c < "$out.part") / 1024 / 1024 ))
  if (( size_mb < min_mb )); then
    echo "ERROR: $out is only ${size_mb} MB (expected >= ${min_mb} MB); check the URL" >&2
    rm -f "$out.part"
    exit 1
  fi
  mv "$out.part" "$out"
  echo "done: $out (${size_mb} MB)"
}

fetch "$EMBEDDING_GGUF_URL" bge-m3-q8_0.gguf 500
fetch "$RERANK_GGUF_URL" bge-reranker-v2-m3-Q8_0.gguf 500
echo "Models ready. Start them with: make up-ai"
