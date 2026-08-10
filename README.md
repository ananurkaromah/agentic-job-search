# agentic-job-search

An agentic AI application for intelligent job discovery, job matching, and application management using Databricks Apps, Lakebase, Vector Search/RAG, and AI Agents.

Built 100% inside the Databricks Workspace UI on Databricks Free Edition.

## Overview

The app ingests job postings from multiple sources, stores structured data in Lakebase (Postgres), indexes unstructured text (job descriptions, resumes) for semantic search, and exposes an AI agent that can search, explain matches, and manage a user's application pipeline — all through a Streamlit UI deployed as a Databricks App. Those same tools are also exposed over MCP, so other AI agents (Databricks Playground, Claude Desktop, third-party agents) can use them too, not just the built-in chat tab.

## Architecture

```
Volume (raw JSON) → PySpark ETL → Lakebase (Postgres) — 8 relational tables
                                        ↓
                    ┌───────────────────┴───────────────────┐
                    ↓                                       ↓
      Delta mirror → Vector Search           job_documents → chunk + embed
      (1 endpoint, 2 indexes:                 (sentence-transformers,
       job_postings, profiles)                 384-dim) → job_embeddings
                    ↓                          (pgvector, Direct Vector Access)
                    └───────────────────┬───────────────────┘
                                        ↓
                     Agent tools (search, explain match, pipeline
                     read/write) via a 2X-Small SQL Warehouse
                                        ↓
                    ┌───────────────────┴───────────────────┐
                    ↓                                       ↓
      Streamlit App (Databricks Apps)          MCP Server (Databricks Apps)
      — 4-tab UI, in-process agent.py          — same tools, exposed over
                                                  MCP for other agents/clients
```

Two vector search paths exist side by side: **Databricks Vector Search** (managed, over the Delta-synced `job_postings`/`profiles` tables) and **Direct Vector Access** (`job_documents`/`job_embeddings` in Lakebase via pgvector, embedded locally with `sentence-transformers`). They're independent — use either or both.

## Project Structure

Three things run independently in this project: **notebooks/jobs** (run manually or on a schedule inside the workspace), and two **deployed apps** (each a self-contained unit Databricks Apps ships as-is). The folders below reflect that split — everything under `app/` or `mcp_server/` has to be importable from within that same folder only, since that's the one folder that actually gets deployed.

```
agentic-job-search/
├── README.md
├── requirements.txt
├── setup_secrets.py              # run once, from your terminal — not deployed anywhere

├── sql/
│   └── lakebase_schema.sql       # run once against Lakebase, via SQL editor

├── etl/                          # ── run as notebooks / a scheduled Workflow ──
│   ├── fetch_live_jobs.py        # optional: pulls live listings into the Volume
│   └── etl_pipeline.py           # raw JSON -> normalized -> Lakebase upsert

├── vector_search/
│   ├── sync_lakebase_to_delta.py # reads Lakebase -> writes Delta mirrors w/ CDF
│   └── vector_search_setup.py    # run once (or on schema change) to build the indexes

├── embeddings/                   # ── Direct Vector Access (pgvector), run as a Job ──
│   ├── ingest_job_embeddings.py  # chunk + embed job_documents -> job_embeddings
│   └── search_job_embeddings.py  # query-side: cosine search over job_embeddings

├── app/                          # ── deployed as one unit via `databricks apps deploy` ──
│   ├── app.py                    # Streamlit UI (4 tabs)
│   ├── app.yaml                  # deployment config (entrypoint, env vars)
│   ├── agent.py                  # Foundation Model orchestration + function calling
│   ├── agent_tools.py            # read/write tool functions, used by agent.py
│   └── lakebase.py               # Lakebase connection helper, imported by app.py & agent_tools.py

├── mcp_server/                   # ── deployed as its own unit via `databricks apps deploy` ──
│   ├── server.py                 # FastMCP server exposing the same tools over MCP
│   ├── app.yaml                  # deployment config
│   ├── requirements.txt          # this app's own dependency set
│   ├── agent_tools.py            # copy of app/agent_tools.py — see note below
│   ├── lakebase.py               # copy of app/lakebase.py — see note below
│   └── search_job_embeddings.py  # copy of embeddings/search_job_embeddings.py

└── samples/
    ├── adzuna_sample.json        # example raw payload per source
    ├── remoteok_sample.json
    └── usajobs_sample.json
```

`lakebase.py` lives inside `app/` — not at the repo root — because `app.py` and `agent_tools.py` import it with a plain same-directory import (`from lakebase import get_connection`). Since Databricks Apps only uploads the `app/` folder to that app's compute, anything that folder's code imports has to live inside it too. The same rule is why `mcp_server/` carries its own copies of `agent_tools.py`, `lakebase.py`, and `search_job_embeddings.py` instead of importing across folders — it's deployed separately from `app/`, so it needs everything it uses inside its own folder. `etl_pipeline.py`, `fetch_live_jobs.py`, and `embeddings/ingest_job_embeddings.py` don't have this constraint — they run as notebooks/jobs with the full repo checked out, so `ingest_job_embeddings.py` importing `from app.lakebase import get_connection` across folders works fine there.

## Core Components

**`etl/fetch_live_jobs.py`**
Optional replacement for the manual sample upload. Pulls current listings from Adzuna and USAJobs (both need API credentials from `setup_secrets.py`) and RemoteOK (public, no key), writing timestamped JSON files into the same Volume folders `etl_pipeline.py` already reads. Run it before `etl_pipeline.py` in the same Workflow job to keep data fresh.

**`sql/lakebase_schema.sql`**
DDL for the 8 relational tables: `users`, `profiles`, `skills`, `job_postings`, `applications`, `saved_jobs`, `interview_notes`, `contacts`. `job_postings` is deduplicated on `(source_api, external_id)` so re-running the ETL is idempotent.

**`etl/etl_pipeline.py`**
PySpark notebook that reads raw JSON per source from the Volume, normalizes Adzuna/RemoteOK/USAJobs into one common schema, strips HTML/URLs from descriptions, and upserts into `job_postings` via a staging table + `MERGE`. Writes through the native `postgresql` Spark connector (not JDBC), and casts `job_id`/`vector_id` to `::uuid` in the merge.

**`vector_search/sync_lakebase_to_delta.py`**
Reads `job_postings` and `profiles` directly from Lakebase (via the same native `postgresql` connector `etl_pipeline.py` writes with) and overwrites `workspace.default.job_postings_delta` / `workspace.default.profiles_delta`, enabling Change Data Feed on both. Run this before `vector_search_setup.py` — and again any time you want the mirrors (and Vector Search indexes, once synced) to reflect current Lakebase data.

**`vector_search/vector_search_setup.py`**
Creates one Databricks Vector Search endpoint and two Databricks-managed-embedding indexes: `job_postings_index` (over `clean_description`) and `profiles_index` (over `resume_text`), using `databricks-bge-large-en`. Verifies both Delta mirrors exist first (fails with a clear message pointing at `sync_lakebase_to_delta.py` if not) rather than assuming they're already there. Includes an example hybrid semantic query.

**`embeddings/ingest_job_embeddings.py`**
Direct Vector Access alternative to Databricks Vector Search: chunks `job_documents.description_text` (800 chars, 100 overlap), embeds each chunk locally with `sentence-transformers/all-MiniLM-L6-v2` (384-dim), and upserts into `job_embeddings` (pgvector) — idempotent via `content_hash` and `ON CONFLICT (document_id, chunk_index)`. Run as a Databricks Job.

**`embeddings/search_job_embeddings.py`**
Query-side counterpart — embeds a query with the same model and runs a pgvector cosine-similarity search (`<=>`) over `job_embeddings`, joined back to `job_documents`. This is what both `app/agent_tools.py` (if wired up) and the MCP server's `semantic_search_embeddings` tool call.

**`app/agent_tools.py`**
Plain Python functions the agent calls as tools — three read tools (`search_and_rank_jobs`, `explain_job_match`, `surface_stale_applications`) and three write tools (`update_application_stage`, `draft_tailored_materials`, `add_interview_note`), all executing against Lakebase through `lakebase.py`.

**`app/agent.py`**
Wires `agent_tools.py` functions as OpenAI-style function-calling tools for a Databricks Foundation Model serving endpoint, looping tool calls (capped at 5 per turn) until the model returns a final answer.

**`app/app.py`**
Streamlit frontend with four tabs: Profile Setup, Semantic Job Search & Match Explanation, Application Pipeline (Kanban/Table), and Chat with the Agent.

**`app/lakebase.py` + `setup_secrets.py`**
Credential handling: `setup_secrets.py` is a one-time script (run from your terminal) that stores the Lakebase connection URL, plus optional Adzuna/USAJobs API keys, as Databricks secrets under scope `job_copilot`. `lakebase.py` reads the Lakebase secret at runtime inside the deployed app — the URL is never committed to the repo or written into `app.yaml`.

**`mcp_server/server.py`**
A `FastMCP` server that wraps every `agent_tools.py` function plus `semantic_search_embeddings` as MCP tools, deployed as its own Databricks App on the `streamable-http` transport. Any MCP-compliant client (Databricks Playground, Claude Desktop, another agent) can connect to it and call the same read/write tools the Streamlit chat tab uses — over the network, with Databricks Apps' built-in OAuth handling auth.

**`samples/*.json`**
One example raw payload per source API, used to test the ETL pipeline before wiring up live API calls via `fetch_live_jobs.py`.

## Setup & Deployment Instructions

1. **Provision Lakebase.** Create a Lakebase project and a native-password role on it. Enable the `pgvector` extension if you plan to use Direct Vector Access (`embeddings/ingest_job_embeddings.py` also does this automatically on first run).
2. **Store secrets.** Run `python setup_secrets.py` once from a terminal with Databricks auth configured. This stores the Lakebase connection URL under scope `job_copilot`, key `lakebase-url` (required). The same run also prompts — optionally, leave blank to skip — for Adzuna `app_id`/`app_key` and USAJobs `Authorization-Key`/email, needed only if you plan to use `fetch_live_jobs.py`. Read access is granted to workspace users.
3. **Create the Lakebase schema.** Connect with the Lakebase SQL editor (or psql) and run `sql/lakebase_schema.sql`.
4. **Populate the Volume.** Either upload the static payloads from `samples/` into `/Volumes/workspace/default/job_data/{adzuna,remoteok,usajobs}/` by hand, **or** run `etl/fetch_live_jobs.py` as a notebook to pull current listings automatically (uses the API secrets from step 2).
5. **Run the ETL.** Open `etl/etl_pipeline.py` as a notebook and run it (or schedule it, after `fetch_live_jobs.py` if using live data, as a Databricks Workflow for recurring ingestion). It reads `LAKEBASE_URL` from the same `job_copilot/lakebase-url` secret.
6. **Set up Vector Search (Databricks-managed).** Run `vector_search/sync_lakebase_to_delta.py` to create `workspace.default.job_postings_delta` and `workspace.default.profiles_delta` from Lakebase with Change Data Feed enabled, then run `vector_search/vector_search_setup.py` to create the endpoint and both indexes.
7. **Set up Direct Vector Access (optional, alternative/complementary path).** Populate `job_documents` with the text you want searchable, then run `embeddings/ingest_job_embeddings.py` as a Databricks Job to chunk, embed, and upsert into `job_embeddings`.
8. **Configure app secrets/env.** Set `DATABRICKS_HOST` and `DATABRICKS_TOKEN` as app environment variables on `app/` (for the Foundation Model call in `agent.py`). Lakebase access needs no separate env var — `lakebase.py` reads it from the secret automatically.
9. **Deploy the Streamlit app.** From the `app/` folder, run `databricks apps deploy`, using `app.py` + `app.yaml` as the entrypoint.
10. **Deploy the MCP server (optional).** From the `mcp_server/` folder, run `databricks apps deploy` separately, using `server.py` + its own `app.yaml`. This gives every tool a second, network-reachable entry point over MCP.
11. **Verify.** Open the Streamlit app URL, fill in a profile on the Profile Setup tab, then try a semantic search and a chat message. For the MCP server, connect an MCP client (e.g. Databricks Playground or Claude Desktop) to its deployed URL with a Databricks auth token and confirm the tools list appears.

## Status

Portfolio / capstone project — in progress.