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

# DBTITLE 1,Prepare data for PostgreSQL upsert
# --- 4. Prepare data for PostgreSQL upsert ---------------------------------
# Collect DataFrame to driver and prepare as list of tuples for psycopg2.
# This approach works on serverless compute without JDBC driver.

from urllib.parse import urlparse
import psycopg2
from psycopg2.extras import execute_values

LAKEBASE_URL = dbutils.secrets.get("job_copilot", "lakebase-url")

# Deduplicate before collecting to minimize memory footprint
from pyspark.sql import Window
window = Window.partitionBy("source_api", "external_id").orderBy(F.desc("posted_at"))
deduped_df = (
    cleaned_df
    .withColumn("row_num", F.row_number().over(window))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

# Collect rows and convert to list of tuples
# Order: job_id, source_api, external_id, title, company, location, 
#        salary_range, raw_description, clean_description, posted_at, 
#        vector_id, ingested_at
rows = deduped_df.select(
    "job_id", "source_api", "external_id", "title", "company", 
    "location", "salary_range", "raw_description", "clean_description",
    "posted_at", "vector_id", "ingested_at"
).collect()

data_tuples = [
    (
        row.job_id, row.source_api, row.external_id, row.title, row.company,
        row.location, row.salary_range, row.raw_description, row.clean_description,
        row.posted_at, row.vector_id, row.ingested_at
    )
    for row in rows
]

print(f"Prepared {len(data_tuples)} rows for upsert")

# COMMAND ----------

# DBTITLE 1,Bulk upsert to Lakebase using psycopg2
# --- 5. Bulk upsert directly to job_postings using execute_values ----------
# Use psycopg2.extras.execute_values for efficient bulk insert with ON CONFLICT.
# No staging table needed, no JDBC driver required.

if len(data_tuples) == 0:
    print("No data to upsert")
else:
    conn = psycopg2.connect(LAKEBASE_URL)
    
    try:
        with conn.cursor() as cur:
            # Bulk insert with ON CONFLICT to deduplicate on source_api + external_id
            upsert_sql = """
            INSERT INTO job_copilot.job_postings
                (job_id, source_api, external_id, title, company, location, 
                 salary_range, raw_description, clean_description, posted_at, 
                 vector_id, ingested_at)
            VALUES %s
            ON CONFLICT (source_api, external_id) DO UPDATE SET
                title = EXCLUDED.title,
                company = EXCLUDED.company,
                location = EXCLUDED.location,
                salary_range = EXCLUDED.salary_range,
                raw_description = EXCLUDED.raw_description,
                clean_description = EXCLUDED.clean_description,
                posted_at = EXCLUDED.posted_at,
                vector_id = EXCLUDED.vector_id,
                ingested_at = EXCLUDED.ingested_at
            """
            
            # execute_values handles the bulk insert efficiently
            execute_values(
                cur, 
                upsert_sql, 
                data_tuples,
                template="(%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::uuid, %s)",
                page_size=1000
            )
            
            conn.commit()
            print(f"✓ Upserted {len(data_tuples)} rows to job_copilot.job_postings")
    
    except Exception as e:
        conn.rollback()
        print(f"Error during upsert: {e}")
        raise
    
    finally:
        conn.close()