# Team onboarding: get the code, the data, and run SportsBot

For VNP and LTN (and anyone setting up a new machine). Updated 2026-11-03.
Presentation: **Tuesday 10 November 2026**. Plan: [plans/2026-11-02-plan-to-presentation.md](plans/2026-11-02-plan-to-presentation.md).

**The short version.** Only TMK's PC crawls the news. You do not crawl: you download TMK's corpus
from the team's **private Kaggle dataset** and run the chatbot on it with one command,
`make team-data`. Sections 1-4 get you there; section 5 is what to do after; section 7 lists the
problems we already hit.

| You are | Do sections | Then |
|---|---|---|
| **LTN** (tester, no AI assistant) | 1, 2, 3, 4 | [TASKS.md](TASKS.md), LTN section: test rounds, golden questions, labels |
| **VNP** (Claude) | 1, 2, 3, 4 | [TASKS.md](TASKS.md) VNP section and [handoff/VNP.md](handoff/VNP.md): Spark tasks 6, 7, 9 |
| **TMK** (crawler PC) | section 6 | Publish a new Kaggle version every evening |

## 1. Install the tools (once, about 30-45 minutes)

### Windows 11 (WSL2)
1. Open **PowerShell as administrator** and run `wsl --install -d Ubuntu-24.04`. Restart the PC.
   Ubuntu opens and asks for a user name and password: **write the password down** (`sudo` asks
   for it). Forgotten? In PowerShell: `wsl -d Ubuntu-24.04 -u root`, then `passwd <your user>`.
2. Install Docker Desktop: `winget install -e --id Docker.DockerDesktop`, restart, open Docker
   Desktop. Settings → General → tick "Use the WSL 2 based engine". Settings → Resources →
   WSL integration → turn on **Ubuntu-24.04** → Apply & restart.
3. Install VS Code and its **WSL** extension. Open Ubuntu, then run `code .` in a folder to edit
   it from Windows.
4. In Ubuntu, install the basics:
   ```bash
   sudo apt update && sudo apt install -y git make python3-venv python3-pip
   ```

### macOS
Docker Desktop (or OrbStack), Git, `make` (`xcode-select --install`), Python 3.11+.

### Know which window you are in
| The line starts with | You are in | Use it for |
|---|---|---|
| `PS C:\...>` | Windows PowerShell | only `wsl -d Ubuntu-24.04` (enter Ubuntu) |
| `yourname@PC:~$` | Ubuntu | **every project command**: `git`, `make`, `docker` |

## 2. Get the code (once)

In **Ubuntu**, in your Linux home (not under `/mnt/c`, which is slow):
```bash
cd ~
git clone https://github.com/khoaaminh1/sportsbot_g4.git
cd sportsbot_g4
python3 -m venv .venv
source .venv/bin/activate          # the line now starts with (.venv)
cp .env.example .env
make setup && make test            # expect "passed" and 0 failed
```
Later, to get TMK's newest code: `cd ~/sportsbot_g4 && git pull`.

### Your `.env` (secrets, never shared)
Open it with `code .env` and fill in:

| Line | Value |
|---|---|
| `GEMINI_API_KEY=` | **Your own** free key from https://aistudio.google.com/apikey (each member uses their own) |
| `KAGGLE_DATASET=` | `minhkhoa132/sportsbot-corpus` |
| `KAGGLE_API_TOKEN=` | Your Kaggle token (section 3) |

Never commit `.env`, never paste it into a chat, never post a screenshot of it. If a key leaks,
delete it on the website and create a new one.

## 3. Kaggle access (once)

The corpus lives in a **private** Kaggle dataset owned by TMK. Private means only people TMK adds
can see it; this is required because the article text belongs to the publishers.

1. Create a free account at https://www.kaggle.com (Google sign-in is fine) and **send your
   Kaggle username to TMK** (shown on your profile page, e.g. `kaggle.com/<username>`).
2. Wait for TMK to add you. You can then open
   https://www.kaggle.com/datasets/minhkhoa132/sportsbot-corpus while signed in.
   If the page says 404, you are not added yet, or you are signed in with another account.
3. Create your API token: Kaggle → your avatar → **Settings** → section **API** → **Create New
   Token**. Copy the token and paste it into your `.env` as `KAGGLE_API_TOKEN=<token>`.
   (If Kaggle downloads a `kaggle.json` file instead, put it at `~/.kaggle/kaggle.json` and run
   `chmod 600 ~/.kaggle/kaggle.json`; leave `KAGGLE_API_TOKEN` empty.)
4. Test it:
   ```bash
   make kaggle-pull        # downloads into data/kaggle/ and lists the files
   ```
   Expected files: `articles.jsonl.gz`, `sportsbot.sql.gz`, `bronze.tgz`, `export-summary.json`.
   `export-summary.json` tells you when the snapshot was taken and how many articles it has.

Only want to read the data, without running anything? Download the files from the dataset page
(button **Download**), or in a Kaggle notebook use **Add data** → the team dataset.

## 4. Run the chatbot on the team corpus

One command (first time 15-30 minutes: it downloads about 1.3 GB of models and builds vectors):
```bash
cd ~/sportsbot_g4
make team-data
```
It downloads the newest Kaggle version, downloads the models if missing, starts PostgreSQL,
Qdrant, the backend and the AI services **without the crawler**, imports the articles and builds
the vectors. Then open **http://localhost:8000/app/** in your browser (Windows too).

| Later | Command |
|---|---|
| Start again after a restart | `make up-team` |
| Get TMK's newer data | `make team-data` (replaces your local articles) |
| Check Gemini keys | `make llm-check` |
| Ask in the terminal | `make ask Q="Tin tennis mới nhất?"` |
| Stop everything | `make down` |

Do not run `make up-demo` or `make up-stream` on your machine: those start the crawler, and only
TMK's PC should crawl (two crawlers hit the same sites twice and build two different corpora).

## 5. Work with the data

- What every file and field means, and how to load it into PostgreSQL, HDFS (Spark) or pandas:
  **[DATA.md](DATA.md)**.
- Spark (VNP): `make up-bigdata`, then `make bronze-load FILE=data/kaggle/bronze.tgz`.
- Hand labels (LTN): TMK sends `data/labeling/sample.csv`; open it in Google Sheets, fill the
  label column as described in the plan (task 5), send it back. Do not share it outside the team.
- Every number you put in a report or slide states the snapshot (from `export-summary.json`)
  and the command that produced it.

## 6. TMK: publish the corpus to Kaggle

On the crawler PC, in Ubuntu, with `KAGGLE_DATASET` and `KAGGLE_API_TOKEN` in `.env`:
```bash
cd ~/sportsbot_g4
make kaggle-export      # data/kaggle/: articles.jsonl.gz, sportsbot.sql.gz, bronze.tgz, export-summary.json
make kaggle-push        # first time creates the PRIVATE dataset; afterwards adds a new version
```
- Add teammates once: dataset page → **Settings** → **Sharing** → add each Kaggle username
  (Can view is enough; Can edit only if they should publish versions).
- Keep the dataset **private**. Never switch it to public.
- Optional, every evening at 23:00 automatically (Windows PowerShell, then test with `/Run`):
  ```powershell
  schtasks /Create /TN "SportsBot Kaggle" /SC DAILY /ST 23:00 /TR "wsl -d Ubuntu-24.04 --cd ~/sportsbot_g4 -- make kaggle-export kaggle-push"
  schtasks /Run /TN "SportsBot Kaggle"
  ```

## 7. Problems we already hit

| Symptom | Fix |
|---|---|
| `make: command not found`, `source: not recognized` | You are in PowerShell. Run `wsl -d Ubuntu-24.04`, then `cd ~/sportsbot_g4` |
| `python: Permission denied` or `No module named ...` | `source .venv/bin/activate` |
| `.venv/bin/activate: No such file` | `rm -rf .venv && python3 -m venv .venv && make setup` |
| `sudo`: wrong password 3 times | PowerShell: `wsl -d Ubuntu-24.04 -u root`, then `passwd <user>` |
| `make kaggle-pull`: 403 or 404 | Not added to the dataset yet, wrong `KAGGLE_DATASET`, or an expired token: create a new token |
| `make kaggle-pull`: `KAGGLE_DATASET` not set | Add `KAGGLE_DATASET=minhkhoa132/sportsbot-corpus` to `.env` |
| `docker: permission denied` / cannot connect | Open Docker Desktop and wait until it says running; WSL integration must be on (section 1) |
| Page http://localhost:8000/app/ does not open | `make ps`: the backend must be running; first start can take a few minutes |
| Embedder HTTP 500 `unordered_map::at` | Old model file: `git pull`, `make models`, `make up-team`, `make reindex` |
| `llm-check`: 503 or timeout on one model | Google is busy; the router uses the next model |
| Chat says "Không tìm thấy bài viết phù hợp" | The corpus has no article on it; name the team, athlete or competition |
| Reloading VS Code stopped a long command | Run long commands in a separate Ubuntu or PowerShell window, not the VS Code terminal |

## 8. Rules

- Work on a branch, open a pull request, wait for both CI checks; never tick "bypass rules".
- No secrets in git, chats or screenshots (`.env`, API keys, tokens, webhook URLs).
- Never disable robots.txt or the 3-second per-site delay; never work around a block.
- No invented numbers: every figure comes from a real run, with the command and the date.
- Code, commits and documentation in English. Questions in the team chat in any language.
