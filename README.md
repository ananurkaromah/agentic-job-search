# agentic-job-search

An agentic AI application for intelligent job discovery, job matching, and application management using Databricks Apps, Lakebase, Vector Search/RAG, and AI Agents.

Built 100% inside the Databricks Workspace UI on Databricks Free Edition.

## Overview

The app ingests job postings from multiple sources, stores structured data in Lakebase (Postgres), indexes unstructured text (job descriptions, resumes) for semantic search, and exposes an AI agent that can search, explain matches, and manage a user's application pipeline — all through a Streamlit UI deployed as a Databricks App.

## Architecture

```
Volume (raw JSON) → PySpark ETL → Lakebase (Postgres) — 8 relational tables
                                        ↓
                          Delta mirror → Vector Search (1 endpoint, 2 indexes:
                          job_postings, profiles)
                                        ↓
                     Agent (Foundation Model + function calling,
                     read/write tools via a 2X-Small SQL Warehouse)
                                        ↓
                     Streamlit App (Databricks Apps) — 4-tab UI
```

## Project Structure

Two things run independently in this project: **notebooks/jobs** (run manually or on a schedule inside the workspace) and the **deployed app** (one self-contained unit Databricks Apps ships as-is). The folders below reflect that split — everything under `app/` has to be able to import from within `app/` only, since that's the one folder that actually gets deployed.

```
agentic-job-search/
├── README.md
├── requirements.txt
├── setup_secrets.py              # run once, from your terminal 
├── sql/
│   └── lakebase_schema.sql       # run once against Lakebase, via SQL editor
├── etl/                          # ── run as notebooks / a scheduled Workflow ──
│   ├── fetch_live_jobs.py        # pulls live listings into the Volume
│   └── etl_pipeline.py           # raw JSON -> normalized -> Lakebase upsert
├── vector_search/
│   └── vector_search_setup.py    # run once (or on schema change) to build the indexes
├── app/                          # ── deployed as one unit via `databricks apps deploy` ──
│   ├── app.py                    # Streamlit UI (4 tabs)
│   ├── app.yaml                  # deployment config (entrypoint, env vars)
│   ├── agent.py                  # Foundation Model orchestration + function calling
│   ├── agent_tools.py            # read/write tool functions, used by agent.py
│   └── lakebase.py               # Lakebase connection helper, imported by app.py & agent_tools.py
└── samples/
    ├── adzuna_sample.json        # example raw payload per source
    ├── remoteok_sample.json
    └── usajobs_sample.json
```

`lakebase.py` lives inside `app/` — not at the repo root — because `app.py` and `agent_tools.py` import it with a plain same-directory import (`from lakebase import get_connection`). Since Databricks Apps only uploads the `app/` folder to the app's compute, anything that folder's code imports has to live inside it too. `etl_pipeline.py` and `fetch_live_jobs.py` don't need it — they run as notebooks and read the secret directly via `dbutils.secrets.get(...)`.

## Core Components

**`etl/fetch_live_jobs.py`**
Optional replacement for the manual sample upload. Pulls current listings from Adzuna and USAJobs (both need API credentials from `setup_secrets.py`) and RemoteOK (public, no key), writing timestamped JSON files into the same Volume folders `etl_pipeline.py` already reads. Run it before `etl_pipeline.py` in the same Workflow job to keep data fresh.

**`sql/lakebase_schema.sql`**
DDL for the 8 relational tables: `users`, `profiles`, `skills`, `job_postings`, `applications`, `saved_jobs`, `interview_notes`, `contacts`. `job_postings` is deduplicated on `(source_api, external_id)` so re-running the ETL is idempotent.

**`etl/etl_pipeline.py`**
PySpark notebook that reads raw JSON per source from the Volume, normalizes Adzuna/RemoteOK/USAJobs into one common schema, strips HTML/URLs from descriptions, and upserts into `job_postings` via a staging table + `MERGE`.

**`vector_search/vector_search_setup.py`**
Creates one Vector Search endpoint and two Databricks-managed-embedding indexes: `job_postings_index` (over `clean_description`) and `profiles_index` (over `resume_text`), using `databricks-bge-large-en`. Includes an example hybrid semantic query.

**`app/agent_tools.py`**
Plain Python functions the agent calls as tools — three read tools (`search_and_rank_jobs`, `explain_job_match`, `surface_stale_applications`) and three write tools (`update_application_stage`, `draft_tailored_materials`, `add_interview_note`), all executing against Lakebase through `lakebase.py`.

**`app/agent.py`**
Wires `agent_tools.py` functions as OpenAI-style function-calling tools for a Databricks Foundation Model serving endpoint, looping tool calls (capped at 5 per turn) until the model returns a final answer.

**`app/app.py`**
Streamlit frontend with four tabs: Profile Setup, Semantic Job Search & Match Explanation, Application Pipeline (Kanban/Table), and Chat with the Agent.

**`app/lakebase.py` + `setup_secrets.py`**
Credential handling: `setup_secrets.py` is a one-time script (run from your terminal or a notebook) that stores the Lakebase connection URL as a Databricks secret. `lakebase.py` reads that secret at runtime inside the deployed app — the URL is never committed to the repo or written into `app.yaml`.

**`samples/*.json`**
One example raw payload per source API, used to test the ETL pipeline before wiring up live API calls (see the note on live fetching in the setup steps below).

## Setup & Deployment Instructions

1. **Populate the Volume.** Either upload the static payloads from `samples/` into `/Volumes/main/default/job_data/{adzuna,remoteok,usajobs}/` by hand, **or** run `etl/fetch_live_jobs.py` as a notebook to pull current listings automatically (requires the API secrets from step 3).
2. **Create the Lakebase schema.** Provision a Lakebase project, connect with its SQL editor (or psql), and run `sql/lakebase_schema.sql`.
3. **Store secrets.** Create a native-password role on the Lakebase instance, then run `python setup_secrets.py` once from a terminal with Databricks auth configured. This stores the Lakebase connection URL under scope `job_copilot`, key `lakebase-url`. The same run also prompts (optionally, leave blank to skip) for Adzuna `app_id`/`app_key` and USAJobs `Authorization-Key`/email, needed only if you're using `fetch_live_jobs.py`. Read access is granted to workspace users.
4. **Run the ETL.** Open `etl/etl_pipeline.py` as a notebook and run it (or schedule it, after `fetch_live_jobs.py`, as a Databricks Workflow for recurring ingestion). It reads `LAKEBASE_URL` from the same `job_copilot/lakebase-url` secret.
5. **Set up Vector Search.** Sync `job_postings` / `profiles` into Delta tables with Change Data Feed enabled, then run `vector_search/vector_search_setup.py` to create the endpoint and both indexes.
6. **Configure app secrets/env.** Set `DATABRICKS_HOST` and `DATABRICKS_TOKEN` as app environment variables (for the Foundation Model call in `agent.py`). Lakebase access needs no separate env var — `app/lakebase.py` reads it from the secret automatically.
7. **Deploy the app.** From the `app/` folder, run `databricks apps deploy`, using `app.py` + `app.yaml` as the entrypoint.
8. **Verify.** Open the deployed app URL, fill in a profile on the Profile Setup tab, then try a semantic search and a chat message to confirm the agent can read and write to Lakebase.

## Status

Portfolio / capstone project — in progress.