# ==============================================================
# agent_tools.py
# Python function tools the AI Agent calls to read/write Lakebase
# data and query the Vector Search index. Each function is plain
# Python; agent.py wraps these as callable "tools" for the LLM.
# ==============================================================

from datetime import date, datetime

from databricks.vector_search.client import VectorSearchClient

from lakebase import get_connection as _get_conn

VS_ENDPOINT = "job_copilot_vs_endpoint"
JOB_INDEX_NAME = "workspace.default.job_postings_index"

vsc = VectorSearchClient()


# ----------------------- READ TOOLS -----------------------------------

def search_and_rank_jobs(user_id: str, query_text: str, top_k: int = 10) -> list[dict]:
    """Semantic search over job_postings, ranked by relevance to query_text."""
    index = vsc.get_index(VS_ENDPOINT, JOB_INDEX_NAME)
    results = index.similarity_search(
        query_text=query_text,
        columns=["job_id", "title", "company", "location", "salary_range", "clean_description"],
        num_results=top_k,
    )
    rows = results["result"]["data_array"]
    cols = ["job_id", "title", "company", "location", "salary_range", "clean_description", "score"]
    return [dict(zip(cols, r)) for r in rows]


def explain_job_match(user_id: str, job_id: str) -> dict:
    """Fetch a job + the user's profile so the LLM can explain the fit."""
    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT title, company, clean_description FROM job_copilot.job_postings WHERE job_id = %s",
            (job_id,),
        )
        job = cur.fetchone()

        cur.execute(
            "SELECT primary_skills, experience_summary, resume_text "
            "FROM job_copilot.profiles WHERE user_id = %s",
            (user_id,),
        )
        profile = cur.fetchone()

    return {"job": job, "profile": profile}


def surface_stale_applications(user_id: str, days_inactive: int = 14) -> list[dict]:
    """Applications not updated in `days_inactive` days, to nudge follow-up."""
    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT a.application_id, j.title, j.company, a.stage, a.updated_at
            FROM job_copilot.applications a
            JOIN job_copilot.job_postings j ON j.job_id = a.job_id
            WHERE a.user_id = %s
              AND a.stage NOT IN ('rejected', 'offer')
              AND a.updated_at < now() - (%s || ' days')::interval
            ORDER BY a.updated_at ASC
            """,
            (user_id, days_inactive),
        )
        return cur.fetchall()


# ----------------------- WRITE TOOLS -----------------------------------

def update_application_stage(application_id: str, new_stage: str) -> dict:
    """Move an application to a new pipeline stage."""
    valid_stages = {"saved", "applied", "interviewing", "rejected", "offer"}
    if new_stage not in valid_stages:
        raise ValueError(f"new_stage must be one of {valid_stages}")

    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE job_copilot.applications SET stage = %s, updated_at = now() "
            "WHERE application_id = %s RETURNING application_id, stage",
            (new_stage, application_id),
        )
        conn.commit()
        return cur.fetchone()


def draft_tailored_materials(user_id: str, job_id: str, material_type: str) -> str:
    """
    Build the prompt context for a cover letter / tailored resume bullet set.
    The actual text generation happens in agent.py via the Foundation Model
    call — this tool just assembles grounded context (profile + job posting).
    """
    match = explain_job_match(user_id, job_id)
    job, profile = match["job"], match["profile"]

    context = (
        f"Job: {job['title']} at {job['company']}\n"
        f"Job description: {job['clean_description'][:1500]}\n\n"
        f"Candidate skills: {profile['primary_skills']}\n"
        f"Candidate experience: {profile['experience_summary']}\n"
        f"Requested material: {material_type}"
    )
    return context


def add_interview_note(application_id: str, stage: str, note_text: str,
                        follow_up_date: str | None = None) -> dict:
    """Record an interview / recruiter note against an application."""
    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO job_copilot.interview_notes
                (application_id, stage, note_text, follow_up_date)
            VALUES (%s, %s, %s, %s)
            RETURNING note_id, created_at
            """,
            (application_id, stage, note_text, follow_up_date),
        )
        conn.commit()
        return cur.fetchone()