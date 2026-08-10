# Databricks notebook source
# ==============================================================
# vector_search_setup.py
# Creates a Databricks Vector Search endpoint + index over
# job_postings.clean_description (and a second index over
# profiles.resume_text), then runs an example hybrid semantic query.
# ==============================================================

# COMMAND ----------
%pip install databricks-vector-search
dbutils.library.restartPython()

# COMMAND ----------
from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient()

ENDPOINT_NAME = "job_copilot_vs_endpoint"   # single AI Search Endpoint shared by both indexes
CATALOG = "main"
SCHEMA = "default"

# COMMAND ----------
# --- 1. Create the Vector Search endpoint (idempotent) ----------------

existing_endpoints = [e["name"] for e in vsc.list_endpoints().get("endpoints", [])]
if ENDPOINT_NAME not in existing_endpoints:
    vsc.create_endpoint(name=ENDPOINT_NAME, endpoint_type="STANDARD")
    print(f"Created endpoint {ENDPOINT_NAME}")
else:
    print(f"Endpoint {ENDPOINT_NAME} already exists")

# COMMAND ----------
# --- 2. Source tables must first be synced into Delta tables with -------
# Change Data Feed enabled (Lakebase -> Delta sync, or a Lakeflow
# Connect pipeline). Assume these Delta mirror tables already exist:
#   main.default.job_postings_delta   (job_id, clean_description, ...)
#   main.default.profiles_delta       (profile_id, resume_text, ...)

spark.sql(f"""
    ALTER TABLE {CATALOG}.{SCHEMA}.job_postings_delta
    SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")
spark.sql(f"""
    ALTER TABLE {CATALOG}.{SCHEMA}.profiles_delta
    SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# COMMAND ----------
# --- 3. Create the job_postings index (Databricks-managed embeddings) ---

JOB_INDEX_NAME = f"{CATALOG}.{SCHEMA}.job_postings_index"

if not vsc.get_index(ENDPOINT_NAME, JOB_INDEX_NAME).exists():
    vsc.create_delta_sync_index(
        endpoint_name=ENDPOINT_NAME,
        index_name=JOB_INDEX_NAME,
        source_table_name=f"{CATALOG}.{SCHEMA}.job_postings_delta",
        pipeline_type="TRIGGERED",
        primary_key="job_id",
        embedding_source_column="clean_description",
        embedding_model_endpoint_name="databricks-bge-large-en",
    )
    print(f"Created index {JOB_INDEX_NAME}")

# COMMAND ----------
# --- 4. Create the resume/profile index ----------------------------------

PROFILE_INDEX_NAME = f"{CATALOG}.{SCHEMA}.profiles_index"

if not vsc.get_index(ENDPOINT_NAME, PROFILE_INDEX_NAME).exists():
    vsc.create_delta_sync_index(
        endpoint_name=ENDPOINT_NAME,
        index_name=PROFILE_INDEX_NAME,
        source_table_name=f"{CATALOG}.{SCHEMA}.profiles_delta",
        pipeline_type="TRIGGERED",
        primary_key="profile_id",
        embedding_source_column="resume_text",
        embedding_model_endpoint_name="databricks-bge-large-en",
    )
    print(f"Created index {PROFILE_INDEX_NAME}")

# COMMAND ----------
# --- 5. Example hybrid semantic query -------------------------------------

job_index = vsc.get_index(ENDPOINT_NAME, JOB_INDEX_NAME)

query_text = "remote backend roles that don't require 5+ years of Kubernetes experience"

results = job_index.similarity_search(
    query_text=query_text,
    columns=["job_id", "title", "company", "location", "clean_description"],
    num_results=10,
    # hybrid: combine vector similarity with a keyword filter clause
    filters={"location": "Remote"},
)

for row in results["result"]["data_array"]:
    print(row[1], "-", row[2], "-", row[3])
