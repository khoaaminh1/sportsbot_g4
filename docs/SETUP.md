# Setup and Operation

Run these commands in Ubuntu WSL. Clone into a new directory so the existing local prototype remains available.

```bash
git clone --branch feature/live-sports-assistant https://github.com/khoaaminh1/sportsbot_g4.git sportsbot_g4_live
cd sportsbot_g4_live
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
cp .env.example .env
python -m sportsbot.cli ingest
python -m sportsbot.cli ask "VN Indo bóng đá"
python -m streamlit run app.py
```

Open http://localhost:8501 in the Windows browser. The gio browser launch warning in WSL does not mean that Streamlit failed.

For conversational terminal mode run `python -m sportsbot.cli chat`. Enter /exit to stop. The ask command accepts one quoted question. Add `--offline` to skip feed refresh. Run `python -m sportsbot.cli ingest --watch` in another terminal for collection every five minutes. Stop it with Ctrl+C. The computer and process must remain running.

## Language model configuration

Choose a provider offering a Chat Completions compatible HTTPS API. Put its API base URL, exact available model identifier and secret key into LLM_BASE_URL, LLM_MODEL and LLM_API_KEY in .env. The base URL usually includes the provider API version path, but must not include /chat/completions. Consult that provider's documentation. Requests can incur provider charges. Never paste a real key into GitHub or a chat message.

Without credentials the application provides sourced excerpts and explicitly identifies that mode. With credentials it rewrites queries into Vietnamese search terms, uses recent conversation context, retrieves RSS evidence and generates a short response in the user's language. The initial multilingual approach uses model based query rewriting and a small bilingual alias dictionary, not multilingual embeddings.

## Cloud demo

Connect this repository and branch to Streamlit Community Cloud, select app.py, install requirements.txt and set the three LLM values in application Secrets. SQLite on an ephemeral host is disposable. Use an external persistent database before promising continuous historical storage. Do not run Hadoop inside the web application. Hosting the UI is not a guarantee of continuous background ingestion.

## Big Data handoff

Export collected records using `python -m sportsbot.cli export > articles.jsonl`. Import this file into the course HDFS environment and process it with Spark according to the lab instructions. HDFS ingestion and Spark jobs are not implemented in this release.

## Validation

Install pytest separately and run `python -m pytest -q`. These offline tests check alias normalization, unsafe link rejection, missing evidence and the fallback path. Live collection needs network access to the publishers. Complete a provider smoke test after configuring credentials, including ambiguous match questions, follow up questions and English queries.
