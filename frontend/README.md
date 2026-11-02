# frontend

The SportsBot web page: one static file (`index.html`) plus `config.js`. No build step.
Owners: TMK (code), LTN (testing). It calls only the documented backend API.

- **Left panel, live:** what the crawler is doing now, over the `/ws/news` WebSocket: corpus
  counters (articles, last hour, active sources, languages), articles published in the last
  72 hours (seeded from `GET /news/latest`, so backfilled old articles do not flood it), and
  collection runs that found new items or failed.
- **Centre:** a multi-sport question box, sport shortcuts that prefill a question, and suggested
  questions labelled by sport; answers stream over SSE (`POST /chat/stream`) with numbered
  citations and a numbered source list (title, outlet, publication time).
- Look: "stadium after dark", navy with a trophy-gold accent (light theme: porcelain and navy);
  the brand mark is an athletics track, not a single ball. Themes follow the system; on phones
  the live panel becomes a drawer.
- Font: Archivo (Google Fonts, Vietnamese subset); its width axis gives condensed headlines and
  numbers and normal-width reading text. System fallbacks when offline.

## Run
Served by the backend: `make up-core` (or `make up-demo`), then http://localhost:8000/app/.

## Host the page elsewhere (e.g. Vercel)
1. Make the API reachable from the internet (planned: Cloudflare Tunnel + rate limits). Do not
   expose the API publicly before the rate limits exist: strangers could use up the Gemini quota.
2. Set the API address in `config.js`: `window.SPORTSBOT_API = "https://<your-api-host>";`
3. Allow the page's origin on the backend: `CORS_ORIGINS=https://<project>.vercel.app` in `.env`.
4. Vercel: New Project → import the GitHub repo → Root Directory `frontend` → Framework preset
   "Other" → Deploy (no build command).

Article text shown comes from the API: titles, excerpts (max 400 characters) and links to the
originals. Untrusted text is inserted with `textContent` or escaped before markup is added.
