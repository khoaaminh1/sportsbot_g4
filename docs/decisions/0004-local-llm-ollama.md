# 0004. Local LLM with Ollama; answers only from stored articles

- Status: accepted (2026-11-02, team decision communicated by TMK)
- Date: 2026-11-02
- Deciders: TMK, VNP, LTN

## Context
- The Phase 1 assessment left the LLM open: a paid hosted API or a free local model.
- The team decided: Ollama for local inference, no paid API for now; test the chatbot in the terminal before any website; Mac for light work, Windows PC for heavier inference; code must stay portable between macOS and Windows.
- Measured on TMK's Mac (Apple M2 Pro, 16 GB, Ollama 0.40.13, `llama3:latest` 8B Q4_0): 37 tokens/s; first request 10.2 s including 5.6 s model load. Asked directly, the model gave fluent but false facts about Vietnamese football (a World Cup goal that never happened, a midfielder called a goalkeeper).
- Requirement restated by TMK: answer only from stored data, always with a reference link.

## Decision
1. **Ollama** serves the LLM locally, reached through its **OpenAI-compatible endpoint** (`LLM_BASE_URL=http://localhost:11434/v1`). The model is a setting (`LLM_MODEL`), initially `llama3:latest`, which is already installed.
2. **Grounding is mandatory.** The model only receives retrieved stored articles; no match means no model call. Answers must cite `[n]`, may not contain URLs, and fall back to article excerpts when the check fails. Sources are printed with title, publication time and link.
3. No paid API and no model download without the team's approval.

## Consequences
- Portable: the same code and URL work with Ollama on macOS and Windows; pointing `LLM_BASE_URL` at the Windows PC moves inference there.
- Answer quality in Vietnamese is limited by `llama3`; a model comparison (for example `qwen2.5:7b`, about 4.7 GB) is planned for Phase 4 with a decision rule written before the run.
- An 8B model needs about 5-6 GB of memory (estimate), so on the 16 GB Mac avoid running it alongside heavy Spark jobs.
- Revisit if no local model reaches acceptable faithfulness on the golden set.
