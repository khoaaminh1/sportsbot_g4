# 0006. Chat LLM: free Gemini models in rotation, Ollama as the last fallback

- Status: accepted (2026-11-02, TMK)
- Date: 2026-11-02
- Deciders: TMK, VNP, LTN
- Amends: ADR 0004 (Ollama stays, as the fallback; grounding rules unchanged)

## Context
- ADR 0004 chose local Ollama. Measured on TMK's Mac, `llama3` 8B wrote fluent but wrong
  Vietnamese when asked without context; quality in Vietnamese is the main risk for the demo.
- The team wants several **free** Gemini models used in turn when one runs out of quota.
- Gemini API free tier (third-party summaries of Google's pricing pages, read 2026-11-02):
  Flash and Flash-Lite models are free; limits are per project and per model, RPD resets at
  midnight Pacific time; Google no longer prints exact limits (see AI Studio); free-tier
  prompts may be used to improve Google's products. Pro models left the free tier in April 2026.
- Gemini serves an OpenAI-compatible endpoint:
  `https://generativelanguage.googleapis.com/v1beta/openai`.

## Decision
- `agent/clients/llm_router.py` tries `GEMINI_MODELS` in order, then Ollama
  (`LLM_BASE_URL`, `LLM_MODEL`) when `LLM_USE_OLLAMA_FALLBACK=true`.
- HTTP 429: per-minute limits rest the model for `LLM_COOLDOWN_SECONDS`; daily limits rest it
  until midnight Pacific. 404 (unknown model) and 401/403 disable the target for the session.
- Default order: `gemini-3.8-flash, gemini-3.6-flash, gemini-3.5-flash-lite,
  gemini-3.1-flash-lite, gemini-2.5-flash-lite`. These names come from third-party pages;
  `make llm-check` verifies which ones answer with our key before the demo.
- One API key per project. We do not create extra accounts or projects to multiply the free
  quota (against the provider's terms).
- Grounding rules from ADR 0004 are unchanged: the model sees only retrieved stored articles,
  must cite `[n]`, may not add URLs; no evidence means no model call.

## Consequences
- Prompts (questions + public news excerpts) go to Google on the free tier: acceptable for
  public news; users are told not to type personal data.
- The answering model can change between questions; the API returns `meta.llm` so evaluation
  and the demo show which model answered.
- Revisit if the free tier changes again; the router needs only configuration changes.
