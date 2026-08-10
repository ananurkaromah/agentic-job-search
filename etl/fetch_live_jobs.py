# Databricks notebook source
# ==============================================================
# fetch_live_jobs.py
# Replaces the manual "upload samples/ to the Volume" step with a live
# pull from Adzuna, USAJobs, and RemoteOK. Writes one timestamped JSON
# file per source into the same Volume paths etl_pipeline.py already
# reads from — nothing downstream changes.
#
# Run this as a notebook (manually, or on a schedule via a Databricks
# Workflow) before etl_pipeline.py in the same job.
#
# Credentials come from the `job_copilot` secret scope created by
# setup_secrets.py: adzuna-app-id, adzuna-app-key, usajobs-auth-key,
# usajobs-email. RemoteOK needs no key.
# ==============================================================

# COMMAND ----------
import json
from datetime import datetime, timezone

import requests

VOLUME_PATH = "/Volumes/main/default/job_data"
SEARCH_KEYWORDS = "data engineer"          # adjust to your target role
RUN_TS = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

# COMMAND ----------
# --- Adzuna ------------------------------------------------------------

ADZUNA_APP_ID = dbutils.secrets.get("job_copilot", "adzuna-app-id")
ADZUNA_APP_KEY = dbutils.secrets.get("job_copilot", "adzuna-app-key")

adzuna_resp = requests.get(
    "https://api.adzuna.com/v1/api/jobs/us/search/1",
    params={
        "app_id": ADZUNA_APP_ID,
        "app_key": ADZUNA_APP_KEY,
        "what": SEARCH_KEYWORDS,
        "results_per_page": 50,
        "content-type": "application/json",
    },
    timeout=30,
)
adzuna_resp.raise_for_status()

adzuna_path = f"{VOLUME_PATH}/adzuna/adzuna_{RUN_TS}.json"
dbutils.fs.put(adzuna_path, json.dumps(adzuna_resp.json()), overwrite=True)
print(f"Wrote {adzuna_path}")

# COMMAND ----------
# --- USAJobs -------------------------------------------------------------

USAJOBS_AUTH_KEY = dbutils.secrets.get("job_copilot", "usajobs-auth-key")
USAJOBS_EMAIL = dbutils.secrets.get("job_copilot", "usajobs-email")

usajobs_resp = requests.get(
    "https://data.usajobs.gov/api/search",
    params={"Keyword": SEARCH_KEYWORDS, "ResultsPerPage": 50},
    headers={
        "Host": "data.usajobs.gov",
        "User-Agent": USAJOBS_EMAIL,          # USAJobs requires a registered email here
        "Authorization-Key": USAJOBS_AUTH_KEY,
    },
    timeout=30,
)
usajobs_resp.raise_for_status()

usajobs_path = f"{VOLUME_PATH}/usajobs/usajobs_{RUN_TS}.json"
dbutils.fs.put(usajobs_path, json.dumps(usajobs_resp.json()), overwrite=True)
print(f"Wrote {usajobs_path}")

# COMMAND ----------
# --- RemoteOK (public, no auth) -------------------------------------------

remoteok_resp = requests.get(
    "https://remoteok.com/api",
    params={"tags": "data"},
    headers={"User-Agent": "agentic-job-search/1.0"},  # RemoteOK blocks default UAs
    timeout=30,
)
remoteok_resp.raise_for_status()

remoteok_path = f"{VOLUME_PATH}/remoteok/remoteok_{RUN_TS}.json"
dbutils.fs.put(remoteok_path, json.dumps(remoteok_resp.json()), overwrite=True)
print(f"Wrote {remoteok_path}")

# COMMAND ----------
# Next: run etl_pipeline.py — it reads every *.json file under each
# source's Volume folder, so old and new runs both get picked up
# (job_postings dedupes on source_api + external_id via the ETL's MERGE).
print("Live fetch complete. Run etl_pipeline.py next.")