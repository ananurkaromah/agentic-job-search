# agentic-job-search

An agentic AI application for intelligent job discovery, job matching, and application management using Databricks Apps, Lakebase, pgvector, and AI Agents (via MCP).

## Overview

The app ingests job postings from multiple sources into Lakebase (Postgres), embeds job descriptions and resumes directly into `pgvector` for semantic search, and exposes agent tools — search, explain-match, and application pipeline read/write — over MCP so any MCP-compliant agent (Databricks Playground, Claude Desktop, etc.) can use them. A small Flask app provides an operational dashboard for triggering sync jobs and inspecting synced data.

There is no Databricks Vector Search, no Delta table mirroring, and no Change Data Feed anywhere in this project — semantic search is pgvector, directly against Lakebase, end to end.

## Architecture

```
Volume (raw JSON) → PySpark ETL → Lakebase (Postgres) — job_postings, profiles, ...
                                        ↓
                     sync_documents.py (plain SQL, same DB)
                                        ↓
                              job_documents (source text)
                                        ↓
                     ingest_job_embeddings.py (chunk + embed,
                     sentence-transformers, 384-dim)
                                        ↓
                    job_embeddings (pgvector) ←──────────────┐
                                        ↓                     │
                     agent_tools.py — search via pgvector      │  same Lakebase,
                     <=> operator, plus read/write tools       │  read directly
                                        ↓                     │
                    ┌───────────────────┴────────────────────┐│
                    ↓                                        ↓│
      mcp_server/ (Databricks App)              app/ (Databricks App)
      — MCP server exposing agent tools         — Flask dashboard: trigger
        to Playground / other agents              sync jobs, browse records
```

Two separate Databricks Apps, two separate jobs:
- **`app/`** — the required "Databricks App with a frontend." A Flask dashboard for operational visibility: trigger sync jobs (via the Databricks Jobs API) and browse synced tables. No agent logic lives here.
- **`mcp_server/`** — the required "AI agent with tools that search/retrieve and take real actions." Not itself the agent — it's the tool layer an agent (running in Databricks Playground, Claude Desktop, or elsewhere) connects to over MCP to actually search job postings, explain matches, and read/write the application pipeline.

## Project Structure

Every deployed app folder is self-contained: Databricks Apps only uploads the one folder you point a deployment at, so anything that folder's code imports has to live inside it — including its own `requirements.txt`.

```
agentic-job-search/
├── README.md
├── requirements.txt               # combined set, for local dev reference only
├── setup_secrets.py                # run once, from your terminal — not deployed anywhere

├── sql/
│   └── lakebase_schema.sql         # all 10 tables, incl. job_documents/job_embeddings + pgvector extension

├── etl/                             # ── run as notebooks / a scheduled Workflow ──
│   ├── fetch_live_jobs.py          # optional: pulls live listings into the Volume
│   └── etl_pipeline.py             # raw JSON -> normalized -> Lakebase upsert

├── embeddings/                      # ── pgvector pipeline, run as Databricks Jobs ──
│   ├── sync_documents.py           # job_postings/profiles -> job_documents (plain SQL, same DB)
│   ├── ingest_job_embeddings.py    # chunk + embed job_documents -> job_embeddings
│   └── search_job_embeddings.py    # query-side: cosine search over job_embeddings

├── app/                             # ── deployed as its own unit: Flask dashboard ──
│   ├── app.py                      # routes: trigger syncs, read synced records
│   ├── app.yaml                    # deployment config incl. sync job ID env vars
│   ├── requirements.txt            # this app's own dependency set
│   ├── lakebase.py                 # Lakebase connection helper
│   └── templates/
│       └── index.html              # dashboard UI (buttons + records table)

├── mcp_server/                      # ── deployed as its own unit: agent tool layer ──
│   ├── server.py                   # FastMCP server exposing agent_tools.py over MCP
│   ├── app.yaml                    # deployment config
│   ├── requirements.txt            # this app's own dependency set
│   ├── agent_tools.py              # read/write tool functions (pgvector search + Lakebase r/w)
│   ├── lakebase.py                 # copy of app/lakebase.py — see note below
│   └── search_job_embeddings.py    # copy of embeddings/search_job_embeddings.py

└── samples/
    ├── adzuna_sample.json          # example raw payload per source
    ├── remoteok_sample.json
    └── usajobs_sample.json
```

`app/` and `mcp_server/` don't share code by import — `mcp_server/lakebase.py` and `mcp_server/search_job_embeddings.py` are copies, not imports, of their `app/`/`embeddings/` counterparts, because each app folder is deployed independently and only ships with itself. `etl_pipeline.py`, `fetch_live_jobs.py`, `sync_documents.py`, and `ingest_job_embeddings.py` don't have this constraint — they run as notebooks/jobs with the full repo checked out, so `from app.lakebase import get_connection` works fine there (note: this is the repo-root `app/lakebase.py`, unrelated to Databricks Apps' `app/` deployment folder sharing the same name).

## Core Components

**`sql/lakebase_schema.sql`**
DDL for all 10 tables in one file: the original 8 (`users`, `profiles`, `skills`, `job_postings`, `applications`, `saved_jobs`, `interview_notes`, `contacts`) plus `job_documents` and `job_embeddings`, and `CREATE EXTENSION IF NOT EXISTS vector`. This is the single source of truth for the schema — run it first, on every fresh Lakebase instance.

**`etl/fetch_live_jobs.py`** and **`etl/etl_pipeline.py`**
Unchanged from before: pull raw listings into the Volume (optional, live path) and normalize/upsert them into `job_postings` via PySpark, using the native `postgresql` Spark connector.

**`embeddings/sync_documents.py`**
Replaces the old Delta/CDF sync entirely. `job_postings` and `job_documents` live in the same Lakebase database, so this is a plain `INSERT ... SELECT ... ON CONFLICT` — no Spark, no Delta, no external mirror table. Copies `job_postings.clean_description` and `profiles.resume_text` into `job_documents`, tagging each with `source_type` and a `content_hash` that `ingest_job_embeddings.py` uses to decide what needs (re-)embedding.

**`embeddings/ingest_job_embeddings.py`**
Chunks `job_documents.description_text` (800 chars, 100 overlap), embeds each chunk locally with `sentence-transformers/all-MiniLM-L6-v2` (384-dim), upserts into `job_embeddings` (pgvector) — idempotent via `content_hash` + `ON CONFLICT (document_id, chunk_index)`. Run as a Databricks Job, after `sync_documents.py`.

**`embeddings/search_job_embeddings.py`**
Query-side counterpart — embeds a query with the same model, runs pgvector's `<=>` cosine search over `job_embeddings`, joined back to `job_documents`. Used directly by `mcp_server`'s `semantic_search_embeddings` tool, and by `agent_tools.py`'s `search_and_rank_jobs` (which additionally joins to `job_postings` for full posting details).

**`mcp_server/agent_tools.py`**
Six tool functions: three read (`search_and_rank_jobs` — now pgvector-based, no Vector Search endpoint involved; `explain_job_match`; `surface_stale_applications`) and three write (`update_application_stage`, `draft_tailored_materials`, `add_interview_note`), all executing against Lakebase directly.

**`mcp_server/server.py`**
A `FastMCP` server wrapping every `agent_tools.py` function plus `semantic_search_embeddings` as MCP tools, deployed as its own Databricks App on the `streamable-http` transport. Connect an agent to it (Databricks Playground's "Add MCP server," Claude Desktop, etc.) to get real search + read + write capability — this is what satisfies the "AI agent with tools" requirement.

**`app/app.py`**
Flask routes: list configured sync jobs, trigger one via `jobs.run_now`, poll its run status, and read recent rows from a whitelisted set of tables (`job_postings`, `applications`, `job_documents`, `job_embeddings`). Purely operational — no agent logic. This satisfies the "Databricks App with a frontend" requirement.

**`app/templates/index.html`**
The dashboard itself: a button per configured sync (polls run status after triggering), and a table selector that fetches and renders recent rows from `/app/records/<table>`.

**`app/lakebase.py`, `mcp_server/lakebase.py`, `setup_secrets.py`**
Credential handling, unchanged from before: `setup_secrets.py` stores the Lakebase URL (and optional Adzuna/USAJobs keys) as Databricks secrets under scope `job_copilot`; each app's `lakebase.py` reads the Lakebase secret at runtime via the SDK. Neither app needs `DATABRICKS_HOST`/`DATABRICKS_TOKEN` secrets anymore — `app/api.py` uses `WorkspaceClient()`'s ambient Databricks Apps credentials to call the Jobs API, and `mcp_server/` never calls the Foundation Model API directly (that's the connecting agent's job, not the tool server's).

**`samples/*.json`**
Example raw payloads per source API, for testing `etl_pipeline.py` before wiring up `fetch_live_jobs.py`.

## Setup & Deployment Instructions

1. **Provision Lakebase.** Create a Lakebase project and a native-password role on it.
2. **Store secrets.** Run `python setup_secrets.py` once from a terminal with Databricks auth configured — stores the Lakebase URL under `job_copilot/lakebase-url` (required), plus optional Adzuna/USAJobs API keys if you'll use `fetch_live_jobs.py`.
3. **Create the schema.** Run `sql/lakebase_schema.sql` via the Lakebase SQL editor or `psql` — creates all 10 tables and enables `pgvector` in one pass.
4. **Populate the Volume.** Upload `samples/*.json` by hand, or run `etl/fetch_live_jobs.py` as a notebook.
5. **Run the ETL.** Run `etl/etl_pipeline.py` as a notebook (or schedule it as a Workflow) to populate `job_postings`.
6. **Sync documents.** Run `embeddings/sync_documents.py` to copy `job_postings`/`profiles` text into `job_documents`.
7. **Generate embeddings.** Run `embeddings/ingest_job_embeddings.py` (as a Databricks Job — needs `sentence-transformers` + `psycopg2-binary` on the job cluster) to populate `job_embeddings`.
8. **Create Databricks Jobs for each pipeline step**, if you want `app/app.py`'s dashboard to be able to trigger them: Workflows → Create Job, one task each for `fetch_live_jobs.py`, `etl_pipeline.py`, `sync_documents.py`, `ingest_job_embeddings.py`. Note each Job ID.
9. **Deploy the Flask dashboard.** Create app `job-copilot-app` in the Apps UI, upload the 5 files/folders under `app/` into its source folder, set the `*_JOB_ID` env vars in `app.yaml` to the IDs from step 8, add the `job_copilot`/`lakebase-url` secret as a Resource, and grant the app's service principal `CAN_MANAGE_RUN` on each of those Jobs so it can trigger them. Deploy.
10. **Deploy the MCP server.** Create app `job-copilot-mcp`, upload the 6 files under `mcp_server/`, add the same `lakebase-url` secret Resource, deploy.
11. **Verify.** Open `job-copilot-app`'s URL — confirm the dashboard loads, trigger a sync, confirm records show up under the relevant table. Connect an MCP client to `job-copilot-mcp`'s URL and confirm all 7 tools appear; run a search and a write action through it (e.g. via Databricks Playground) to confirm the agent requirement end-to-end.

## Status

Portfolio / capstone project — in progress.
