# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# ==============================================================
# etl_pipeline.py
# Reads raw JSON payloads (Adzuna, RemoteOK, USAJobs) from a Databricks
# Volume, normalizes them into one schema, and upserts into the
# Lakebase `job_postings` table via the Lakebase Postgres endpoint.
#
# Cluster: any Databricks Free Edition cluster / serverless notebook.
# Run this on a schedule (Workflow) to keep job_postings fresh.
# ==============================================================

# COMMAND ----------

import re
import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

VOLUME_PATH = "/Volumes/workspace/default/job_data"

# COMMAND ----------

# --- 1. Load raw JSON per source --------------------------------------

adzuna_raw = spark.read.option("multiLine", True).json(f"{VOLUME_PATH}/adzuna/*.json")
remoteok_raw = spark.read.option("multiLine", True).json(f"{VOLUME_PATH}/remoteok/*.json")
usajobs_raw = spark.read.option("multiLine", True).json(f"{VOLUME_PATH}/usajobs/*.json")

# COMMAND ----------

# --- 2. Normalize each source into a common schema ---------------------
# Common schema: source_api, external_id, title, company, location,
#                salary_range, raw_description, posted_at

adzuna_df = (
    adzuna_raw
    .select(F.explode("results").alias("r"))
    .select(
        F.lit("adzuna").alias("source_api"),
        F.col("r.id").alias("external_id"),
        F.col("r.title").alias("title"),
        F.col("r.company.display_name").alias("company"),
        F.col("r.location.display_name").alias("location"),
        F.concat(F.lit("$"), F.col("r.salary_min").cast("int"), F.lit("-$"),
                  F.col("r.salary_max").cast("int")).alias("salary_range"),
        F.col("r.description").alias("raw_description"),
        F.to_timestamp("r.created").alias("posted_at"),
    )
)

remoteok_df = (
    remoteok_raw
    # first element of the RemoteOK payload is a legal disclaimer, not a job
    .filter(F.col("id").isNotNull())
    .select(
        F.lit("remoteok").alias("source_api"),
        F.col("id").alias("external_id"),
        F.col("position").alias("title"),
        F.col("company").alias("company"),
        F.col("location").alias("location"),
        F.concat(F.lit("$"), F.col("salary_min").cast("int"), F.lit("-$"),
                  F.col("salary_max").cast("int")).alias("salary_range"),
        F.col("description").alias("raw_description"),
        F.to_timestamp("date").alias("posted_at"),
    )
)

usajobs_df = (
    usajobs_raw
    .select(F.explode("SearchResult.SearchResultItems").alias("i"))
    .select(
        F.lit("usajobs").alias("source_api"),
        F.col("i.MatchedObjectId").alias("external_id"),
        F.col("i.MatchedObjectDescriptor.PositionTitle").alias("title"),
        F.col("i.MatchedObjectDescriptor.OrganizationName").alias("company"),
        F.col("i.MatchedObjectDescriptor.PositionLocationDisplay").alias("location"),
        F.concat(
            F.lit("$"),
            F.col("i.MatchedObjectDescriptor.PositionRemuneration")[0]["MinimumRange"],
            F.lit("-$"),
            F.col("i.MatchedObjectDescriptor.PositionRemuneration")[0]["MaximumRange"],
        ).alias("salary_range"),
        F.col("i.MatchedObjectDescriptor.UserArea.Details.JobSummary").alias("raw_description"),
        F.to_timestamp("i.MatchedObjectDescriptor.PublicationStartDate").alias("posted_at"),
    )
)

unified_df = adzuna_df.unionByName(remoteok_df).unionByName(usajobs_df)

# COMMAND ----------

# --- 3. Clean unstructured description text ------------------------------

@F.udf(StringType())
def clean_text(text):
    if text is None:
        return None
    text = re.sub(r"<[^>]+>", " ", text)          # strip HTML tags
    text = re.sub(r"http\S+", " ", text)           # strip URLs
    text = re.sub(r"\s+", " ", text).strip()       # collapse whitespace
    return text

@F.udf(StringType())
def make_job_id():
    return str(uuid.uuid4())

cleaned_df = (
    unified_df
    .withColumn("clean_description", clean_text(F.col("raw_description")))
    .withColumn("job_id", make_job_id())
    .withColumn("vector_id", F.col("job_id"))   # reuse job_id as the vector index key
    .withColumn("ingested_at", F.lit(datetime.now(timezone.utc)))
)

display(cleaned_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Write to Delta table for Vector Search
# --- 3b. Write to Delta table for Vector Search --------------------------
# This enables the Vector Search indexes to sync from a Delta source
# with Change Data Feed enabled.

DELTA_TABLE = "workspace.default.job_postings_delta"

# Write with MERGE to deduplicate on source_api + external_id
# (matching the Lakebase upsert logic)
from delta.tables import DeltaTable

# Check if table exists
if not spark.catalog.tableExists(DELTA_TABLE):
    # Create table on first run using append mode (safe for new table)
    (
        cleaned_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(DELTA_TABLE)
    )
    # Enable Change Data Feed immediately after creation
    spark.sql(f"ALTER TABLE {DELTA_TABLE} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    print(f"✓ Created {DELTA_TABLE} with Change Data Feed enabled")
    row_count = spark.table(DELTA_TABLE).count()
    print(f"✓ Initial load: {row_count} rows")
else:
    # Table exists - perform MERGE (upsert)
    delta_table = DeltaTable.forName(spark, DELTA_TABLE)

    # Deduplicate source data before merge to avoid multiple source rows matching same target
    from pyspark.sql import Window
    window = Window.partitionBy("source_api", "external_id").orderBy(F.desc("posted_at"))
    deduped_df = cleaned_df.withColumn("row_num", F.row_number().over(window)).filter(F.col("row_num") == 1).drop("row_num")
    
    (
        delta_table.alias("target")
        .merge(
            deduped_df.alias("source"),
            "target.source_api = source.source_api AND target.external_id = source.external_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    row_count = spark.table(DELTA_TABLE).count()
    print(f"✓ Merged data into {DELTA_TABLE} (total: {row_count} rows)")

# COMMAND ----------

# --- 4. Upsert into Lakebase `job_postings` -------------------------------
# Lakebase is Postgres-compatible: write via the native postgresql
# connector using secrets stored in a Databricks secret scope
# (recommended over hardcoding credentials).

# Same secret written once by app/setup_secrets.py (scope="job_copilot",
# key="lakebase-url"), so the notebook and the deployed app always point
# at the same Lakebase instance. dbutils.secrets.get() returns the
# decoded plaintext value directly — no extra base64 step needed here
# (that step only applies to app/lakebase.py, which calls the raw REST API).
from urllib.parse import urlparse

LAKEBASE_URL = dbutils.secrets.get("job_copilot", "lakebase-url")
parsed = urlparse(LAKEBASE_URL)

STAGING_TABLE = "job_copilot.job_postings_staging"

(
    cleaned_df.write
    .format("postgresql")
    .option("host", parsed.hostname)
    .option("port", parsed.port or 5432)
    .option("database", parsed.path.lstrip("/"))
    .option("user", parsed.username)
    .option("password", parsed.password)
    .option("dbtable", STAGING_TABLE)
    .mode("overwrite")
    .save()
)

# COMMAND ----------

# --- 5. Merge staging -> job_postings (dedupe on source_api + external_id) --
# Executed via a JDBC connection so we can run a single MERGE statement.

import psycopg2

conn = psycopg2.connect(LAKEBASE_URL)
conn.autocommit = True

merge_sql = """

WITH deduped AS (
    SELECT DISTINCT ON (source_api, external_id)
        job_id, source_api, external_id, title, company, location, salary_range,
        raw_description, clean_description, posted_at, vector_id, ingested_at
    FROM job_copilot.job_postings_staging
    ORDER BY source_api, external_id, posted_at DESC
)
INSERT INTO job_copilot.job_postings
    (job_id, source_api, external_id, title, company, location, salary_range,
     raw_description, clean_description, posted_at, vector_id, ingested_at)
SELECT job_id::uuid, source_api, external_id, title, company, location, salary_range,
       raw_description, clean_description, posted_at, vector_id::uuid, ingested_at
FROM deduped
ON CONFLICT (source_api, external_id) DO UPDATE SET
    title = EXCLUDED.title,
    company = EXCLUDED.company,
    location = EXCLUDED.location,
    salary_range = EXCLUDED.salary_range,
    raw_description = EXCLUDED.raw_description,
    clean_description = EXCLUDED.clean_description,
    posted_at = EXCLUDED.posted_at,
    ingested_at = EXCLUDED.ingested_at;
"""

with conn.cursor() as cur:
    cur.execute(merge_sql)
    print(f"Merged rows affected: {cur.rowcount}")

conn.close()