# Databricks notebook source
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

VOLUME_PATH = "/Volumes/main/default/job_data"

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
# --- 4. Upsert into Lakebase `job_postings` -------------------------------
# Lakebase is Postgres-compatible: write via the JDBC driver using
# secrets stored in a Databricks secret scope (recommended over
# hardcoding credentials).

LAKEBASE_JDBC_URL = dbutils.secrets.get("job_copilot", "lakebase_jdbc_url")
LAKEBASE_USER = dbutils.secrets.get("job_copilot", "lakebase_user")
LAKEBASE_PASSWORD = dbutils.secrets.get("job_copilot", "lakebase_password")

STAGING_TABLE = "job_copilot.job_postings_staging"

(
    cleaned_df.write
    .format("jdbc")
    .option("url", LAKEBASE_JDBC_URL)
    .option("dbtable", STAGING_TABLE)
    .option("user", LAKEBASE_USER)
    .option("password", LAKEBASE_PASSWORD)
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()
)

# COMMAND ----------
# --- 5. Merge staging -> job_postings (dedupe on source_api + external_id) --
# Executed via a JDBC connection so we can run a single MERGE statement.

import psycopg2

conn = psycopg2.connect(dsn=LAKEBASE_JDBC_URL.replace("jdbc:", ""),
                         user=LAKEBASE_USER, password=LAKEBASE_PASSWORD)
conn.autocommit = True

merge_sql = """
INSERT INTO job_copilot.job_postings
    (job_id, source_api, external_id, title, company, location, salary_range,
     raw_description, clean_description, posted_at, vector_id, ingested_at)
SELECT job_id, source_api, external_id, title, company, location, salary_range,
       raw_description, clean_description, posted_at, vector_id, ingested_at
FROM job_copilot.job_postings_staging
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
