# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# ==============================================================
# vector_search_setup.py
# Creates a Databricks Vector Search endpoint + index over
# job_postings.clean_description (and a second index over
# profiles.resume_text), then runs an example hybrid semantic query.
# ==============================================================

# COMMAND ----------

# DBTITLE 1,Cell 2
# MAGIC %pip install databricks-ai-search
# MAGIC dbutils.library.restartPython()
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 3
from databricks.ai_search.client import AISearchClient

vsc = AISearchClient()

ENDPOINT_NAME = "job_copilot_vs_endpoint"   # single AI Search Endpoint shared by both indexes
CATALOG = "workspace"
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

# DBTITLE 1,Cell 5
# --- 2. Source tables must first be synced into Delta tables with -------
# Change Data Feed enabled (Lakebase -> Delta sync, or a Lakeflow
# Connect pipeline). Check if these Delta mirror tables exist:
#   workspace.default.job_postings_delta   (job_id, clean_description, ...)
#   workspace.default.profiles_delta       (profile_id, resume_text, ...)

# Check if tables exist before attempting ALTER
tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").collect()
table_names = [row.tableName for row in tables]

if "job_postings_delta" not in table_names:
    print(f"⚠️  Table {CATALOG}.{SCHEMA}.job_postings_delta does not exist.")
    print("   Create it first by syncing from Lakebase or running the ETL pipeline with Delta output.")
else:
    spark.sql(f"""
        ALTER TABLE {CATALOG}.{SCHEMA}.job_postings_delta
        SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
    """)
    print(f"✓ Enabled Change Data Feed on {CATALOG}.{SCHEMA}.job_postings_delta")

if "profiles_delta" not in table_names:
    print(f"⚠️  Table {CATALOG}.{SCHEMA}.profiles_delta does not exist.")
    print("   Create it first by syncing from Lakebase or via a separate profiles ETL pipeline.")
else:
    spark.sql(f"""
        ALTER TABLE {CATALOG}.{SCHEMA}.profiles_delta
        SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
    """)
    print(f"✓ Enabled Change Data Feed on {CATALOG}.{SCHEMA}.profiles_delta")

# COMMAND ----------

# DBTITLE 1,Cell 6
# --- 3. Create the job_postings index (Databricks-managed embeddings) ---

JOB_INDEX_NAME = f"{CATALOG}.{SCHEMA}.job_postings_index"

# Check if index already exists
try:
    index = vsc.get_index(ENDPOINT_NAME, JOB_INDEX_NAME)
    index_desc = index.describe()
    print(f"Index {JOB_INDEX_NAME} already exists")
except Exception as e:
    if "does not exist" in str(e):
        # Index doesn't exist, create it
        vsc.create_delta_sync_index(
            endpoint_name=ENDPOINT_NAME,
            index_name=JOB_INDEX_NAME,
            source_table_name=f"{CATALOG}.{SCHEMA}.job_postings_delta",
            pipeline_type="TRIGGERED",
            primary_key="job_id",
            embedding_source_column="clean_description",
            embedding_model_endpoint_name="databricks-bge-large-en",
        )
        print(f"✓ Created index {JOB_INDEX_NAME}")
        
        # Get index info after creation
        index = vsc.get_index(ENDPOINT_NAME, JOB_INDEX_NAME)
        index_desc = index.describe()
    else:
        # Some other error, re-raise
        raise

# Display final index status
print(f"\n{'='*60}")
print(f"AI SEARCH INDEX STATUS")
print(f"{'='*60}")
print(f"Index: {JOB_INDEX_NAME}")
print(f"Source: {CATALOG}.{SCHEMA}.job_postings_delta")
if 'status' in index_desc:
    status = index_desc['status']
    print(f"Status: {status.get('state', status.get('detailed_state', 'Unknown'))}")
    print(f"Ready: {status.get('ready', False)}")
print(f"Pipeline: {index_desc.get('delta_sync_index_spec', {}).get('pipeline_type', 'N/A')}")
if 'delta_sync_index_spec' in index_desc and 'embedding_source_columns' in index_desc['delta_sync_index_spec']:
    emb_col = index_desc['delta_sync_index_spec']['embedding_source_columns'][0]
    print(f"Embedding Column: {emb_col['name']}")
    print(f"Embedding Model: {emb_col['embedding_model_endpoint_name']}")
print(f"{'='*60}")

# COMMAND ----------

# DBTITLE 1,Cell 7
# --- 4. Create the resume/profile index ----------------------------------

PROFILE_INDEX_NAME = f"{CATALOG}.{SCHEMA}.profiles_index"

# Skip profile index creation if profiles_delta table doesn't exist
tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").collect()
table_names = [row.tableName for row in tables]

if "profiles_delta" not in table_names:
    print(f"⚠️  Skipping {PROFILE_INDEX_NAME} creation - profiles_delta table does not exist.")
    print("   Create profiles_delta first via a separate ETL pipeline or Lakebase sync.")
else:
    # Check if index already exists
    try:
        index = vsc.get_index(ENDPOINT_NAME, PROFILE_INDEX_NAME)
        print(f"Index {PROFILE_INDEX_NAME} already exists")
    except Exception as e:
        if "does not exist" in str(e):
            # Index doesn't exist, create it
            vsc.create_delta_sync_index(
                endpoint_name=ENDPOINT_NAME,
                index_name=PROFILE_INDEX_NAME,
                source_table_name=f"{CATALOG}.{SCHEMA}.profiles_delta",
                pipeline_type="TRIGGERED",
                primary_key="profile_id",
                embedding_source_column="resume_text",
                embedding_model_endpoint_name="databricks-bge-large-en",
            )
            print(f"✓ Created index {PROFILE_INDEX_NAME}")
        else:
            # Some other error, re-raise
            raise

# COMMAND ----------

# DBTITLE 1,Cell 8
# --- 5. Example hybrid semantic query -------------------------------------

job_index = vsc.get_index(ENDPOINT_NAME, JOB_INDEX_NAME)
index_desc = job_index.describe()

# Check if index is ready
if 'status' in index_desc:
    status = index_desc['status']
    state = status.get('state', status.get('detailed_state', 'Unknown'))
    ready = status.get('ready', False)
    
    print(f"Index Status: {state}")
    print(f"Ready: {ready}")
    
    if not ready:
        print("\n⚠️  Index is still provisioning. Please wait a few minutes and run this cell again.")
        print("   You can check the status in the Vector Search UI or re-run Cell 6 to see the current state.")
    else:
        print("\n✓ Index is ready! Running sample query...\n")
        
        query_text = "remote backend roles that don't require 5+ years of Kubernetes experience"
        
        results = job_index.similarity_search(
            query_text=query_text,
            columns=["job_id", "title", "company", "location", "clean_description"],
            num_results=10,
            # hybrid: combine vector similarity with a keyword filter clause
            filters={"location": "Remote"},
        )
        
        print(f"Query: {query_text}\n")
        print("Top Results:")
        for row in results["result"]["data_array"]:
            print(f"  {row[1]} - {row[2]} - {row[3]}")
else:
    print("⚠️  Could not determine index status.")