# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///

# ==============================================================
# sync_lakebase_to_delta.py
# Reads job_postings and profiles from Lakebase and writes them into
# Delta mirror tables with Change Data Feed enabled, so
# vector_search_setup.py has something to build indexes over.
#
# Run this before vector_search_setup.py — and again any time you want
# the Delta mirrors (and therefore the Vector Search indexes, once you
# trigger a sync) to reflect the latest Lakebase data. Safe to re-run:
# overwrites the mirror tables each time and only enables CDF if it
# isn't already on.
# ==============================================================

# COMMAND ----------

from urllib.parse import urlparse

CATALOG = "workspace"
SCHEMA = "default"

LAKEBASE_URL = dbutils.secrets.get("job_copilot", "lakebase-url")
parsed = urlparse(LAKEBASE_URL)

# COMMAND ----------
# --- 1. Read from Lakebase via the native postgresql connector -----------
# Same connector etl_pipeline.py writes with; here we read instead.

def read_lakebase_table(table_name: str):
    return (
        spark.read
        .format("postgresql")
        .option("dbtable", table_name)
        .option("host", parsed.hostname)
        .option("port", parsed.port or 5432)
        .option("database", parsed.path.lstrip("/"))
        .option("user", parsed.username)
        .option("password", parsed.password)
        .load()
    )

job_postings_df = read_lakebase_table("job_copilot.job_postings")
profiles_df = read_lakebase_table("job_copilot.profiles")

print(f"job_postings: {job_postings_df.count()} rows")
print(f"profiles: {profiles_df.count()} rows")

# COMMAND ----------
# --- 2. Write Delta mirror tables (overwrite each run) --------------------

job_postings_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.job_postings_delta")
profiles_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.profiles_delta")

# COMMAND ----------
# --- 3. Enable Change Data Feed on both (idempotent) -----------------------

spark.sql(f"""
    ALTER TABLE {CATALOG}.{SCHEMA}.job_postings_delta
    SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")
spark.sql(f"""
    ALTER TABLE {CATALOG}.{SCHEMA}.profiles_delta
    SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

print("Delta mirrors ready. Next: run vector_search_setup.py, then trigger "
      "an index sync (TRIGGERED pipeline) if the indexes already exist.")