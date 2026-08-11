"""
mcp_server/server.py

Exposes the AI Job Hunting Copilot's read/write tools as an MCP server,
so any MCP-compliant client (Databricks Playground, Claude Desktop,
another agent) can call them over a network connection.

Deployed as its own Databricks App (separate from app/, which serves the
Flask sync-dashboard UI). Uses the streamable-HTTP transport, which
Databricks Apps can front with OAuth like any other app.

Run locally for testing:
    python server.py            # serves on http://localhost:8000/mcp

Deployed:
    databricks apps deploy      # from within mcp_server/, using app.yaml
"""

import logging

from mcp.server.fastmcp import FastMCP

# agent_tools.py, lakebase.py, and search_job_embeddings.py are copied into
# this folder (not imported from ../app or ../embeddings) because Databricks
# Apps only uploads mcp_server/ itself at deploy time — same rule as app/
# being self-contained (see README's Project Structure section).
import agent_tools as tools
from search_job_embeddings import search_job_embeddings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

mcp = FastMCP("job-copilot-mcp")


# ----------------------- READ TOOLS -----------------------------------

@mcp.tool()
def search_and_rank_jobs(user_id: str, query_text: str, top_k: int = 10) -> list[dict]:
    """Semantic search over job_postings via pgvector,
    ranked by relevance to query_text."""
    return tools.search_and_rank_jobs(user_id, query_text, top_k)


@mcp.tool()
def semantic_search_embeddings(
    query_text: str, top_k: int = 10, source_type: str | None = None
) -> list[dict]:
    """Chunk-level semantic search over job_embeddings (pgvector), covering
    both job postings and resumes chunked and embedded via
    ingest_job_embeddings.py. Use source_type='job_posting' or 'resume'
    to narrow the search."""
    return search_job_embeddings(query_text, top_k=top_k, source_type=source_type)


@mcp.tool()
def explain_job_match(user_id: str, job_id: str) -> dict:
    """Fetch a job posting plus the user's profile so an agent can
    explain why (or whether) the job fits the candidate."""
    return tools.explain_job_match(user_id, job_id)


@mcp.tool()
def surface_stale_applications(user_id: str, days_inactive: int = 14) -> list[dict]:
    """List applications not updated in `days_inactive` days, to nudge follow-up."""
    return tools.surface_stale_applications(user_id, days_inactive)


# ----------------------- WRITE TOOLS -----------------------------------

@mcp.tool()
def update_application_stage(application_id: str, new_stage: str) -> dict:
    """Move an application to a new pipeline stage: saved, applied,
    interviewing, rejected, or offer."""
    return tools.update_application_stage(application_id, new_stage)


@mcp.tool()
def draft_tailored_materials(user_id: str, job_id: str, material_type: str) -> str:
    """Assemble grounded context (job + candidate profile) for drafting a
    cover_letter or resume_bullets. Returns context text, not the final
    draft — the calling agent generates the actual prose from it."""
    return tools.draft_tailored_materials(user_id, job_id, material_type)


@mcp.tool()
def add_interview_note(
    application_id: str, stage: str, note_text: str, follow_up_date: str | None = None
) -> dict:
    """Record an interview or recruiter note against an application.
    follow_up_date, if given, must be YYYY-MM-DD."""
    return tools.add_interview_note(application_id, stage, note_text, follow_up_date)


if __name__ == "__main__":
    # streamable-http is the transport Databricks Apps can front with OAuth;
    # stdio would only work for local, same-machine MCP clients.
    mcp.run(transport="streamable-http")
