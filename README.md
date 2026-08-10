# agentic-job-search

An agentic AI application for intelligent job discovery, job matching, and application management using Databricks Apps, Lakebase, Vector Search/RAG, and AI Agents.

Built 100% inside the Databricks Workspace UI on Databricks Free Edition.

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

## Repo structure

```
agentic-job-search/
├── README.md
├── requirements.txt
├── sql/
│   └── lakebase_schema.sql
├── etl/
│   └── etl_pipeline.py
├── vector_search/
│   └── vector_search_setup.py
├── agent/
│   ├── agent_tools.py
│   └── agent.py
├── app/
│   ├── app.py
│   └── app.yaml
└── samples/
    ├── adzuna_sample.json
    ├── remoteok_sample.json
    └── usajobs_sample.json
```

## Setup

1. Create a Volume at `/Volumes/main/default/job_data/{adzuna,remoteok,usajobs}/` and upload sample payloads from `samples/`.
2. Create a Lakebase project and run `sql/lakebase_schema.sql` against it.
3. Store Lakebase credentials in a Databricks secret scope named `job_copilot`.
4. Run `etl/etl_pipeline.py` as a notebook (or schedule it as a Workflow).
5. Sync `job_postings` / `profiles` to Delta, then run `vector_search/vector_search_setup.py` to create the Vector Search endpoint and indexes.
6. Set `LAKEBASE_DSN`, `DATABRICKS_HOST`, `DATABRICKS_TOKEN` as app env vars.
7. Deploy with `databricks apps deploy`, using `app/app.py` + `app/app.yaml` as the entrypoint.

## Status

Portfolio / capstone project — in progress.
