"""
api.py
Flask API + frontend dashboard for triggering sync jobs and reading
synced records. Deployed as its own, self-contained Databricks App
(everything it imports lives in this same folder).

This app is purely operational -- syncs and record inspection. The AI
agent's read/write tools (search, apply, note-taking, etc.) live in
mcp_server/ instead, exposed over MCP for Playground or any other agent
to call. This app doesn't duplicate that logic.

Databricks Apps compute doesn't run Spark, so "trigger a sync" here means
kicking off an existing Databricks Job run via the Jobs API (jobs.run_now)
-- not executing etl_pipeline.py's PySpark inline. Create each pipeline
(fetch_live_jobs.py, etl_pipeline.py, sync_documents.py,
ingest_job_embeddings.py) as a Databricks Job first, note its job ID, and
set that ID as an env var (via app.yaml).

Routes:
    GET  /                                -> dashboard (templates/index.html)
    GET  /api/syncs                       -> list configured sync jobs
    POST /api/syncs/<name>/run            -> trigger that job, returns run_id
    GET  /api/syncs/<name>/runs/<run_id>  -> poll run status
    GET  /api/records/<table>             -> read recent rows from a whitelisted table
"""

import os

from databricks.sdk import WorkspaceClient
from flask import Flask, jsonify, render_template, request

from lakebase import get_connection

app = Flask(__name__)
w = WorkspaceClient()

# Map a short sync name -> Databricks Job ID, read from env vars set in
# app.yaml. Leave an entry unset (empty string) to hide it from the
# dashboard until you've created that job and know its ID.
SYNC_JOBS = {
    "fetch_live_jobs": os.environ.get("FETCH_LIVE_JOBS_JOB_ID", ""),
    "etl_pipeline": os.environ.get("ETL_PIPELINE_JOB_ID", ""),
    "sync_documents": os.environ.get("SYNC_DOCUMENTS_JOB_ID", ""),
    "ingest_job_embeddings": os.environ.get("INGEST_EMBEDDINGS_JOB_ID", ""),
}
SYNC_JOBS = {name: job_id for name, job_id in SYNC_JOBS.items() if job_id}

# Only these tables are readable through /api/records/<table> — never take
# the table name straight from the URL without a whitelist, that's a SQL
# injection surface since it lands in an f-string below.
READABLE_TABLES = {
    "job_postings": "job_copilot.job_postings",
    "applications": "job_copilot.applications",
    "job_documents": "job_copilot.job_documents",
    "job_embeddings": "job_copilot.job_embeddings",
}


@app.route("/")
def dashboard():
    return render_template("index.html", syncs=sorted(SYNC_JOBS.keys()), tables=sorted(READABLE_TABLES.keys()))


@app.route("/api/syncs")
def list_syncs():
    return jsonify({"syncs": sorted(SYNC_JOBS.keys())})


@app.route("/api/syncs/<name>/run", methods=["POST"])
def run_sync(name: str):
    if name not in SYNC_JOBS:
        return jsonify({"error": f"unknown sync '{name}'"}), 404

    run = w.jobs.run_now(job_id=int(SYNC_JOBS[name]))
    return jsonify({"name": name, "run_id": run.run_id})


@app.route("/api/syncs/<name>/runs/<run_id>")
def sync_run_status(name: str, run_id: str):
    if name not in SYNC_JOBS:
        return jsonify({"error": f"unknown sync '{name}'"}), 404

    run = w.jobs.get_run(run_id=int(run_id))
    state = run.state
    return jsonify({
        "run_id": run_id,
        "life_cycle_state": str(state.life_cycle_state) if state else None,
        "result_state": str(state.result_state) if state and state.result_state else None,
        "state_message": state.state_message if state else None,
    })


@app.route("/api/records/<table>")
def read_records(table: str):
    if table not in READABLE_TABLES:
        return jsonify({"error": f"unknown or unlisted table '{table}'"}), 404

    limit = min(int(request.args.get("limit", 50)), 500)
    full_name = READABLE_TABLES[table]

    with get_connection() as conn, conn.cursor() as cur:
        # ORDER BY is best-effort — not every table has the same
        # timestamp column name, so fall back to no ordering if this fails.
        rows = []
        for order_col in ("ingested_at", "created_at", "updated_at", None):
            try:
                order_clause = f"ORDER BY {order_col} DESC" if order_col else ""
                cur.execute(f"SELECT * FROM {full_name} {order_clause} LIMIT %s", (limit,))
                rows = cur.fetchall()
                break
            except Exception:
                conn.rollback()
                continue

    return jsonify({"table": table, "count": len(rows), "rows": [dict(r) for r in rows]})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
