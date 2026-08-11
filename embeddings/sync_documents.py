"""
sync_documents.py
Populates job_documents from job_postings and profiles -- both already
live in the same Lakebase Postgres database, so this is plain SQL
(INSERT ... SELECT), not a Spark job and not a Delta/CDF sync. This
replaces the old vector_search/sync_lakebase_to_delta.py entirely: there
is no external mirror table anymore, no Change Data Feed, no Databricks
Vector Search endpoint. pgvector reads job_embeddings directly.

Run this after etl_pipeline.py (so job_postings is current) and before
ingest_job_embeddings.py (so there's something new to embed). Idempotent:
ON CONFLICT (id) DO UPDATE keeps content_hash current, which is what
ingest_job_embeddings.py checks to decide what needs re-embedding.

Run directly:
    python sync_documents.py
"""

import logging

from app.lakebase import get_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

SYNC_JOB_POSTINGS_SQL = """
INSERT INTO job_copilot.job_documents (id, source_type, description_text, content_hash)
SELECT
    job_id::text,
    'job_posting',
    clean_description,
    md5(clean_description)
FROM job_copilot.job_postings
WHERE clean_description IS NOT NULL
  AND length(trim(clean_description)) > 0
ON CONFLICT (id) DO UPDATE SET
    description_text = EXCLUDED.description_text,
    content_hash = EXCLUDED.content_hash
WHERE job_documents.content_hash IS DISTINCT FROM EXCLUDED.content_hash;
"""

SYNC_PROFILES_SQL = """
INSERT INTO job_copilot.job_documents (id, source_type, description_text, content_hash)
SELECT
    profile_id::text,
    'resume',
    resume_text,
    md5(resume_text)
FROM job_copilot.profiles
WHERE resume_text IS NOT NULL
  AND length(trim(resume_text)) > 0
ON CONFLICT (id) DO UPDATE SET
    description_text = EXCLUDED.description_text,
    content_hash = EXCLUDED.content_hash
WHERE job_documents.content_hash IS DISTINCT FROM EXCLUDED.content_hash;
"""


def run() -> dict:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(SYNC_JOB_POSTINGS_SQL)
        job_postings_synced = cur.rowcount

        cur.execute(SYNC_PROFILES_SQL)
        profiles_synced = cur.rowcount

        conn.commit()

    logger.info("Synced %d job_posting document(s), %d resume document(s).",
                job_postings_synced, profiles_synced)
    return {"job_postings_synced": job_postings_synced, "profiles_synced": profiles_synced}


if __name__ == "__main__":
    run()
