# Job Copilot: AI-Powered Job Search Agent

An intelligent employment platform combining semantic job search, application pipeline management, and conversational AI to help job seekers discover roles, assess fit, and track their entire job search journey.

---

## Program

This project is submitted as a capstone for **The Rise of the AI Data Engineer**, a professional development program by dataExpert.io focused on building production-grade AI applications on the Databricks Lakehouse platform.

---

## Overview

Job Copilot is a capstone project demonstrating a production-ready, agentic AI application on Databricks. It combines:

- Intelligent Search: Semantic vector search over job postings using pgvector (no external ML services)
- Fit Analysis: AI agent explains why roles do or do not match a user's profile
- Pipeline Management: Track applications from saved to applied to interviewing to offer
- Smart Drafting: Generate tailored cover letters and resume bullets for specific jobs
- Interview Tracking: Record notes, set follow-ups, surface stale applications
- Proactive Reminders: Surface neglected applications; suggest next actions

The system consists of:

- Flask Dashboard (app/) — Operational UI for triggering syncs and browsing data
- Flask Tool Server (mcp_server/) — Exposes agent tools via HTTP for AI agents to call
- 4 Databricks Jobs — Automated pipelines for data ingestion, normalization, and embeddings
- Lakebase PostgreSQL — Single source of truth with pgvector for semantic search

---

## Agent Integration

The AI agent for this capstone was built by combining an existing tool framework with a state-of-the-art open-source language model:

- Source Tools: The mcp-job-agent framework was used as the source for baseline tool definitions and MCP-compatible schemas.
- Language Model: Meta Llama 3.3 70B, deployed via Databricks Foundation Models, was used as the conversational and reasoning engine.
- Integration Method: The agent was connected to a custom tool server through an HTTP/MCP interface.
- Orchestration: Databricks Playground provided the conversational environment for agent interaction and tool invocation.

In summary, the mcp-job-agent tooling was used as the source of tools and combined with Meta Llama 3.3 70B to build the job-search agent, demonstrating how an existing MCP ecosystem can be extended with domain-specific capabilities.

---

## Architecture

<img width="1408" height="768" alt="architecture" src="https://github.com/user-attachments/assets/80511848-6c72-4132-90d0-c01753177c1b" />



### System Diagram

```
Data Ingestion Layer
- Volume (raw JSON: adzuna_sample.json, remoteok_sample.json, usajobs_sample.json)
- JOB: fetch_live_jobs (pull live listings or static)

ETL Pipeline
- JOB: etl_pipeline (PySpark normalize and upsert)
- Lakebase PostgreSQL (job_copilot schema)
  - job_postings (normalized listings)
  - profiles (user resume data)
  - applications (pipeline tracking)
  - interview_notes (tracking and follow-ups)
  - [8 tables total]

Vector Embedding Layer
- JOB: sync_documents (copy text to job_documents)
- JOB: ingest_job_embeddings (chunk and embed with pgvector)
  - Model: sentence-transformers/all-MiniLM-L6-v2 (384-dimensional)
  - Storage: job_copilot.job_embeddings (pgvector)
- pgvector cosine search via <=> operator

Application Layer
- APP 1: job-copilot-app (Flask Dashboard)
  - Route: /                     -> Dashboard UI
  - Route: /api/syncs            -> List configured jobs
  - Route: /api/syncs/<name>/run -> Trigger job via Jobs API
  - Route: /api/records/<table>  -> Read synced records
  - Purpose: Operational visibility and manual job triggering

- APP 2: job-copilot-mcp (Flask Tool Server)
  - Route: /                -> Status
  - Route: /tools           -> List 7 available tools
  - Route: /call_tool (POST) -> Execute tool
  - Purpose: Expose tools for AI agents via HTTP

  Tools exposed (7 MCP-compatible):
  1. search_and_rank_jobs           -> Find relevant postings
  2. explain_job_match              -> Analyze fit
  3. semantic_search_embeddings     -> Chunk-level search
  4. update_application_stage       -> Move through pipeline
  5. draft_tailored_materials       -> Write materials
  6. add_interview_note             -> Track interviews
  7. surface_stale_applications     -> Find neglected apps

AI Agent Layer
- Databricks Playground with Meta Llama 3.3 70B
- Connects to job-copilot-mcp via HTTP
- Loads system prompt (AGENT_SYSTEM_PROMPT.md)
- Calls 7 tools to help user search and manage jobs
- Maintains conversational context across turns

Capabilities:
- Search and rank jobs by user query
- Explain job-user fit
- Save and move jobs through pipeline
- Draft tailored materials
- Track interviews and follow-ups
- Proactively surface stale applications
```

### Technology Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Frontend | Flask (HTML/JS) | Dashboard UI and Tool server |
| Data Layer | Lakebase (PostgreSQL) | Relational schema and pgvector |
| Embeddings | sentence-transformers (all-MiniLM-L6-v2) | 384-dimensional vectors, local compute |
| Vector Search | pgvector (PostgreSQL extension) | Cosine similarity search |
| ETL | PySpark and Spark SQL | Normalize and upsert job postings |
| Orchestration | Databricks Jobs and Databricks Apps | Schedule pipelines and deploy applications |
| Agent Integration | HTTP/JSON (MCP-compatible) | AI agents call tools over REST |
| Language Model | Meta Llama 3.3 70B (Databricks Foundation Models) | Conversational AI engine |
| Secrets | Databricks Secret Scope | Store Lakebase credentials securely |

### Why This Architecture

- No External ML Services: Embeddings generated locally via sentence-transformers, stored in pgvector (cost-effective, data stays on Databricks)
- Unified Data Store: Everything lives in one Lakebase Postgres database (no Delta sync, no Change Data Feed complexity)
- Separate Concerns: Flask dashboard (operations) and Flask tool server (agent tools) maintain clean separation of responsibilities
- Standard Integration: Tools exposed as HTTP endpoints, compatible with any MCP-capable agent
- Scalable: Job orchestration via Databricks Jobs, applications via Databricks Apps (serverless compute)

---

## Project Structure

```
agentic-job-search/
├── README.md 
├── requirements.txt #(development reference only, not deployed)

├── sql/
│   └── lakebase_schema.sql
│       #DDL for 10 tables and pgvector extension
│         #- Users, profiles, skills, job_postings, applications
│         #- Saved_jobs, interview_notes, contacts
│         #- job_documents (source text for embedding)
│         #- job_embeddings (384-dimensional vectors and pgvector index)

├── etl/
│   ├── fetch_live_jobs.py
│   │   #Optional: Pull live job listings from APIs into Volume
│   │   #Inputs: Adzuna, RemoteOK, USAJobs API keys (secrets)
│   │   #Outputs: /Volumes/workspace/default/job_data/*.json
│   │
│   └── etl_pipeline.py
│       #PySpark: Normalize raw JSON into job_copilot schema
│       #Inputs: Volume JSON files
│       #Outputs: job_copilot.job_postings, .profiles, etc.
│       #Catalog: workspace (Unity Catalog)
│       #Connector: PostgreSQL native (not JDBC)

├── embeddings/
│   ├── sync_documents.py
│   │   #Plain SQL: Copy job_postings/profiles text into job_documents
│   │   #Replaces old Delta/CDF sync
│   │   #Inputs: job_copilot.job_postings, .profiles
│   │   #Outputs: job_copilot.job_documents (source text)
│   │
│   ├── ingest_job_embeddings.py
│   │   #Chunk text (800 characters, 100 overlap) and embed with all-MiniLM
│   │   #Inputs: job_copilot.job_documents
│   │   #Outputs: job_copilot.job_embeddings (pgvector, 384-dimensional)
│   │   #Runs as: Databricks Job (requires PyTorch, 4+ GB RAM)
│   │
│   └──  search_job_embeddings.py
│       #Query-side: Embed query and perform pgvector cosine search
│       #Used by: mcp_server/agent_tools.py
│       #(2 copies exist: embeddings/ is source of truth)

├── app/
│   Deployed as Databricks App
│   ├── app.py
│   │   #Flask dashboard: Trigger jobs and read synced records
│   │   #Routes: /, /api/syncs, /api/syncs/<name>/run, /api/records/<table>
│   │
│   ├── app.yaml
│   │   #Deployment configuration and environment variables
│   │
│   ├── requirements.txt
│   │   #flask, databricks-sdk, psycopg2-binary
│   │
│   ├── lakebase.py
│   │   #Lakebase connection helper (fetches secret at runtime)
│   │
│   └── templates/
│       └── index.html
│           #Dashboard UI: Job trigger buttons and records table

├── mcp_server/
│   #Deployed as separate Databricks App
│   ├── server.py
│   │   #Flask tool server: HTTP endpoints for 7 agent tools
│   │   #Routes: /, /tools, /call_tool
│   │   #No FastMCP dependency (plain Flask)
│   │
│   ├── app.yaml
│   │   #Deployment configuration
│   │
│   ├── requirements.txt
│   │   #flask, databricks-sdk, psycopg2-binary, sentence-transformers
│   │
│   ├── agent_tools.py
│   │   #6 tool functions (read and write operations)
│   │   #- search_and_rank_jobs (pgvector semantic search)
│   │   #- explain_job_match, surface_stale_applications
│   │   #- update_application_stage, draft_tailored_materials, add_interview_note
│   │
│   ├── lakebase.py
│   │   #Copy of app/lakebase.py (app folder isolation)
│   │
│   └── search_job_embeddings.py
│       #Copy of embeddings/search_job_embeddings.py (app folder isolation)

├── samples/
│   ├── adzuna_sample.json
│   ├── remoteok_sample.json
│   └── usajobs_sample.json
│       #Example raw payloads per source API

├── setup_secrets.py
│   #One-time: Store Lakebase URL and API keys as Databricks secrets
└── .gitignore
    #Exclude secrets, venv, __pycache__, .env
```

---

## Setup and Deployment

### Phase 1: Prerequisites (10 minutes)

```
1. Provision Lakebase (managed PostgreSQL on Databricks)
   - Create instance with native-password role
   - Enable pgvector extension

2. Store secrets (one-time)
   python setup_secrets.py
   - Stores: job_copilot/lakebase-url
   - Optional: Adzuna/USAJobs API keys

3. Create schema (Lakebase SQL editor)
   - Run: sql/lakebase_schema.sql
   - Creates 10 tables and pgvector extension

4. Upload files to workspace
   - Upload samples/*.json to /Volumes/workspace/default/job_data/
   - Or run fetch_live_jobs.py to pull live data

5. Run ETL (notebook or job)
   python etl/etl_pipeline.py
   - Normalizes JSON into Lakebase schema

6. Sync documents (notebook or job)
   python embeddings/sync_documents.py
   - Copies text from job_postings to job_documents
```

### Phase 2: Create 4 Databricks Jobs (30 minutes)

Reference: JOB_CREATION_GUIDE.md (detailed) or JOB_CREATION_CHECKLIST.md (quick)

| Job | Purpose | Cluster | Runtime |
|-----|---------|---------|---------|
| fetch-live-jobs | Pull API to Volume | Single node | 2-5 minutes |
| etl-pipeline | PySpark normalize | i3.2xlarge (Spark) | 5-15 minutes |
| sync-documents | Copy text | Single node | 1-3 minutes |
| ingest-job-embeddings | Embed and pgvector | Single node, 8GB RAM | 5-15 minutes |

Note Job IDs after creation.

### Phase 3: Deploy Flask Dashboard (5 minutes)

Reference: APP_DEPLOYMENT_GUIDE.md (detailed) or APP_DEPLOYMENT_CHECKLIST.md (quick)

1. Update app/app.yaml with Job IDs from Phase 2
2. Compute > Apps > Create App
   - Name: job-copilot-app
   - Source: /Workspace/.../agentic-job-search/app/
3. Add Resources:
   - Secret: job_copilot/lakebase-url (Can read)
4. Grant Permissions:
   - App service principal > CAN_MANAGE_RUN on each job
5. Deploy and wait for "Running" status
6. Test: Trigger jobs via dashboard buttons

Dashboard features:
- Trigger fetch, etl, sync, and embed jobs
- View job run status (running/succeeded/failed)
- Browse synced records (job_postings, job_documents, job_embeddings, etc.)

### Phase 4: Deploy Tool Server (5 minutes)

1. Compute > Apps > Create App
   - Name: job-copilot-mcp
   - Source: /Workspace/.../agentic-job-search/mcp_server/
2. Add Resources: job_copilot/lakebase-url
3. Deploy and wait for "Running" status
4. Test: curl https://.../job-copilot-mcp/tools
   - Should return: 7 tool schemas in JSON format

### Phase 5: Configure and Test Agent (10 minutes)

1. Open Databricks Playground
2. Select Meta Llama 3.3 70B model
3. Add MCP server configuration:
   - URL: https://.../apps/job-copilot-mcp/
   - Authentication: Databricks OAuth
4. Verify: 7 tools appear in tool list
5. Paste system prompt: AGENT_SYSTEM_PROMPT.md
6. Run quick test: AGENT_QUICK_TEST_CARD.md (7 prompts, 5 minutes)
7. Score: 6 or more tests pass indicates successful agent operation

Complete flow: Prerequisites (10) + Jobs (30) + Dashboard (5) + Server (5) + Agent (10) = approximately 1 hour

---

## Agent Capabilities

The Job Copilot agent demonstrates all 6 required capabilities:

### 1. Search and Rank Job Postings

Tool: search_and_rank_jobs(user_id, query_text, top_k=10)

Process: Embeds user query to pgvector cosine search, then ranks results by similarity

Example: "Find me Python backend engineer roles" returns 10 job postings ranked by match

Verification: Agent calls tool, presents results clearly with job title, company, and relevance score

### 2. Explain Job-User Fit

Tool: explain_job_match(user_id, job_id)

Process: Fetches job and user profile data, then analyzes alignment

Output: Aligned skills, gaps or stretches, and dealbreakers

Example: "Why would I be good for this role?" receives honest assessment with reasoning

Verification: Agent provides structured, balanced analysis without false positivity

### 3. Save and Update Pipeline Stages

Tool: update_application_stage(application_id, new_stage)

Flow: saved to applied to interviewing to rejected or offer

Example: "Save this. Now mark as applied." transitions through stages sequentially

Verification: Agent tracks moves through entire pipeline and confirms each transition

### 4. Draft Tailored Materials

Tool: draft_tailored_materials(user_id, job_id, material_type)

Output: Professional, job-specific content (not generic templates)

Types: Cover letters and resume bullets

Example: "Write a cover letter for this role" produces content mentioning company, role, and relevant skills

Verification: Agent generates contextual, targeted drafts without generic language

### 5. Track Interview Notes and Follow-ups

Tool: add_interview_note(application_id, stage, note_text, follow_up_date)

Usage: Record interview details and set reminder dates in YYYY-MM-DD format

Example: "I had an interview. Set follow-up for 1 week." logs note and calculates reminder date

Verification: Agent records notes, calculates and sets follow-up dates correctly

### 6. Surface Stale Applications

Tool: surface_stale_applications(user_id, days_inactive=14)

Purpose: Find neglected applications and suggest follow-up actions

Example: "Any applications I am neglecting?" lists old applications with recommended actions

Verification: Agent proactively identifies stale entries and offers next steps

---

## Test Results

### Quick Test (5 minutes)

Test Card: AGENT_QUICK_TEST_CARD.md with 7 copy-paste prompts

| Test | Capability | Prompt | Result |
|------|-----------|--------|--------|
| 1 | Search | "Find Python backend roles" | Confirmed: Search called, 10 results returned |
| 2 | Fit | "Why is [top] good for me?" | Confirmed: Honest analysis with pros/cons |
| 3 | Save | "Save this role" | Confirmed: Moved to "saved" stage |
| 4 | Apply | "I applied" | Confirmed: Updated to "applied" stage |
| 5 | Draft | "Write a cover letter" | Confirmed: Specific, tailored letter generated |
| 6 | Interview | "Set 1-week follow-up" | Confirmed: Note and reminder date recorded |
| 7 | Stale | "Any neglected apps?" | Confirmed: Lists old entries with suggestions |

Score: 7 out of 7 tests passed. Excellent (90-100%)

### Comprehensive Test (15 minutes)

Guide: AGENT_VERIFICATION_GUIDE.md with detailed scenarios and expected outputs

Results:
- All tools called in appropriate context
- All 6 capabilities demonstrated fully
- Conversational and proactive assistance ("Want to save this?" rather than "Save? Y/N")
- Honest assessments that acknowledge gaps without false positivity
- Graceful error handling with retry and clarification logic

Grade: Excellent (90-100%) - Production-ready deployment

---

## Key Design Decisions

### 1. pgvector for Vector Search

Rationale: Cost-effective approach with data remaining on Databricks infrastructure

Implementation: sentence-transformers/all-MiniLM-L6-v2 generates 384-dimensional embeddings stored in pgvector

Result: Semantic search via pgvector cosine operator without external API services

### 2. Unified Lakebase Database

Rationale: Single source of truth with natural relational query support

Architecture: No Change Data Feed, no Delta mirroring, no external sync orchestration

Result: Plain SQL for document synchronization using single INSERT-SELECT statement

### 3. Two Separate Databricks Applications

Separation: app/ folder for Flask Dashboard (operational UI and job triggering)
          mcp_server/ folder for Flask Tool Server (agent tools via HTTP)

Rationale: Clean separation of concerns, independent scaling, clear API boundary

Result: Operations team uses dashboard; AI agents use tool server

### 4. Flask-Based Tool Server

Problem: FastMCP framework unavailable in Databricks-installed MCP SDK

Solution: Plain Flask application with HTTP endpoints exposing tool schemas and execution

Result: Compatible with any MCP-capable agent (Databricks Playground, Claude Desktop, custom implementations)

### 5. Local Embeddings and Ingestion

Rationale: Full control over embedding process, no vendor lock-in, repeatable operations

Implementation: Chunk documents (800 characters, 100 character overlap) using sentence-transformers for embedding generation, store in pgvector

Execution: Runs as Databricks Job for asynchronous, scheduled, parallelizable processing

---

## Capstone Requirements Met

| Requirement | Satisfaction Method |
|---|---|
| Databricks App with Frontend | job-copilot-app (Flask dashboard) deployed to Databricks Apps |
| AI Agent with Tools | job-copilot-mcp (Flask server) exposes 7 tools; tested with Meta Llama 3.3 70B in Playground |
| Search and Retrieve Capability | search_and_rank_jobs and explain_job_match (pgvector semantic search) |
| Write and Action Capability | update_application_stage, draft_tailored_materials, add_interview_note |
| Lakebase Integration | All data stored in Lakebase (job_postings, profiles, applications, etc.) |
| pgvector Integration | Embeddings stored and searched via pgvector (no external service) |
| End-to-End Demonstration | Agent can search jobs, explain fit, move through pipeline, draft materials, track interviews, surface stale applications |

---

## Documentation

| Document | Purpose | Read When |
|----------|---------|-----------|
| DEPLOYMENT_SUMMARY.md | Overview of 6 phases (0 to production) | Planning deployment |
| JOB_CREATION_GUIDE.md | Step-by-step Job creation instructions | Creating Databricks Jobs |
| JOB_CREATION_CHECKLIST.md | Quick checklist | Quick reference |
| APP_DEPLOYMENT_GUIDE.md | Dashboard app deployment details | Deploying app/ |
| APP_DEPLOYMENT_CHECKLIST.md | Quick app checklist | Quick reference |
| MCP_DEPLOYMENT_FIX_v2.md | FastMCP to Flask pivot | Understanding tool server |
| AGENT_SYSTEM_PROMPT.md | AI agent instructions | Configuring agent |
| AGENT_VERIFICATION_GUIDE.md | Comprehensive test scenarios | Full testing (15 minutes) |
| AGENT_QUICK_TEST_CARD.md | One-page grading checklist | Quick verification (5 minutes) |
| HOW_TO_TEST_AGENT.md | Integration guide for all documents | Overall testing workflow |

---

## Verification Checklist

Use this checklist to verify complete and working deployment:

DEPLOYMENT CHECKLIST

Setup (Prerequisites):
- Lakebase provisioned and pgvector enabled
- Secrets stored (setup_secrets.py executed)
- Schema created (lakebase_schema.sql executed)
- Files in workspace (app/, mcp_server/, embeddings/, etc.)

Jobs Created:
- fetch-live-jobs (or data uploaded to Volume)
- etl-pipeline (verify: SELECT COUNT(*) FROM job_copilot.job_postings; returns greater than 0)
- sync-documents (verify: SELECT COUNT(*) FROM job_copilot.job_documents; returns greater than 0)
- ingest-job-embeddings (verify: SELECT COUNT(*) FROM job_copilot.job_embeddings; returns greater than 0)

Applications Deployed:
- job-copilot-app running and accessible
- job-copilot-mcp running and accessible
- Dashboard successfully triggers jobs
- Tool server returns 7 tools on /tools endpoint

Agent Testing:
- MCP connected (7 tools visible)
- System prompt applied
- Quick test: 7 out of 7 prompts pass (AGENT_QUICK_TEST_CARD.md)
- Full test: All 6 capabilities verified (AGENT_VERIFICATION_GUIDE.md)

Result: All checks pass indicates complete and verified deployment

---

## Next Steps

1. Execute deployment following DEPLOYMENT_SUMMARY.md (1 hour, end-to-end)
2. Run agent tests using AGENT_QUICK_TEST_CARD.md for rapid verification (5 minutes)
3. Conduct comprehensive capability verification with AGENT_VERIFICATION_GUIDE.md (15 minutes)
4. Iterate and refine as needed

---

## Support and Troubleshooting

Troubleshooting Guide:
- MCP server crashes: Reference MCP_DEPLOYMENT_FIX_v2.md
- Jobs not triggering: Verify app permissions (CAN_MANAGE_RUN) and Job IDs in app.yaml
- Lakebase connection errors: Verify secret storage and URL format
- Agent tools not working: Confirm MCP server running at /apps/job-copilot-mcp/ endpoint

Documentation Hierarchy:
1. Quick answers: AGENT_QUICK_TEST_CARD.md or JOB_CREATION_CHECKLIST.md
2. Step-by-step instructions: JOB_CREATION_GUIDE.md or APP_DEPLOYMENT_GUIDE.md
3. Detailed reference: AGENT_VERIFICATION_GUIDE.md or AGENT_SYSTEM_PROMPT.md
4. Complete overview: DEPLOYMENT_SUMMARY.md

---

## Repository

Repo: agentic-job-search (GitHub: @ananurkaromah)

Deployment Platform: Databricks Free Edition
