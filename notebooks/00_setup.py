# Databricks notebook source
# MAGIC %md
# MAGIC # 00 Setup
# MAGIC Creates the database, folders, and pipeline metadata tables used by the capstone project.

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "online_retail_capstone"
VOLUME = "files"
DATABASE = f"{CATALOG}.{SCHEMA}"
BASE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

ARCHIVE_PATH = f"{BASE_PATH}/archives"
LANDING_PATH = f"{BASE_PATH}/landing"
RAW_PATH = f"{BASE_PATH}/raw"
CHECKPOINT_PATH = f"{BASE_PATH}/checkpoints"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {DATABASE}.{VOLUME}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

for path in [
    ARCHIVE_PATH,
    LANDING_PATH,
    RAW_PATH,
    CHECKPOINT_PATH,
]:
    dbutils.fs.mkdirs(path)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {DATABASE}.pipeline_file_log (
      file_name STRING,
      source_path STRING,
      target_path STRING,
      stage STRING,
      processed_at TIMESTAMP
    )
    USING DELTA
    """
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {DATABASE}.pipeline_run_log (
      run_id STRING,
      stage STRING,
      status STRING,
      row_count BIGINT,
      message STRING,
      processed_at TIMESTAMP
    )
    USING DELTA
    """
)

print(f"Schema ready: {DATABASE}")
print(f"Volume path ready: {BASE_PATH}")
