# Runbook: a public link to the web app (free)

Gives people outside the home network a link such as `https://<random>.trycloudflare.com/app/`
to the SportsBot web app running on the crawler PC. Free: a Cloudflare **Quick Tunnel**, no
account, no domain, no open router port, no Vercel (the backend already serves the page).

```
visitor → https://<random>.trycloudflare.com → Cloudflare → tunnel container → backend:8000 → /app/
```

## Before you open it

Everyone who asks a question spends **your** free Gemini quota and 20-50 s of the PC's CPU.
`make public-up` therefore refuses to start without an access code. Set in `.env`:

| Variable | Suggested | Meaning |
|---|---|---|
| `CHAT_ACCESS_CODE` | a short word you give to your audience | Needed to ask questions; the live news panel stays open to everyone |
| `CHAT_RATE_LIMIT` / `CHAT_RATE_WINDOW_SECONDS` | `20` / `600` | Questions per visitor per window (visitor = Cloudflare's visitor address) |
| `CHAT_MAX_CONCURRENT` | `3` | Questions answered at the same time; more get "busy, try again" |
| `WS_MAX_CONNECTIONS` | `100` | Open live-feed connections |

Never put the access code in git, slides or a public post; send it with the link only to the
people you invite. To revoke it, change `CHAT_ACCESS_CODE` and run `make public-up` again.

## Open, check, close

```bash
make public-up       # rebuilds the backend, checks it asks for the code, starts the tunnel, prints the link
make public-url      # print the link again (it changes every time the tunnel restarts)
make public-down     # close the link; the local app at http://localhost:8000/app/ keeps running
```

Check from a phone on mobile data (not the home Wi-Fi): open the link, see the live panel, ask a
question, type the access code when asked.

## What to expect

- **The link is random and changes** whenever the tunnel container restarts (including after a
  reboot, because it restarts by itself). Run `make public-url` and share the new one.
- **No Server-Sent Events through Quick Tunnels** (Cloudflare limitation), so on a
  `trycloudflare.com` address the page asks `POST /chat` and shows the whole answer at once
  instead of streaming it. The live news panel uses a WebSocket, which works.
- Quick Tunnels allow at most 200 requests in flight and have **no uptime guarantee**: for the
  presentation, demo from `http://localhost:8000/app/` and keep the public link for the audience
  to try, with the recorded video as the backup.
- The PC must be on with Docker running; when it is off, the link shows a Cloudflare error.
- Visitors see titles, excerpts (at most 400 characters) and links to the original articles,
  never the full text.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `make public-up`: "Set CHAT_ACCESS_CODE" | Add `CHAT_ACCESS_CODE=<code>` to `.env` |
| `make public-up`: "Backend did not ask for the access code" | The running backend is old or the code is not loaded: check `.env`, then `make public-up` again; the link is not opened until the backend answers 401 |
| `make public-url`: no link after a minute | `docker compose --profile public logs tunnel`; the tunnel needs outbound UDP 7844 or HTTPS 443 |
| The page asks for the code again and again | The code typed differs from `.env`; after editing `.env` run `make public-up` (it recreates the backend) |
| "bạn đã hỏi nhiều câu liên tiếp" | The rate limit for that visitor; wait, or raise `CHAT_RATE_LIMIT` |
| "chatbot đang trả lời câu hỏi khác" | All `CHAT_MAX_CONCURRENT` slots busy; ask again in a few seconds |

A stable address (your own domain on Cloudflare, a named tunnel, SSE support) needs a Cloudflare
account and a domain; not needed for the course.
