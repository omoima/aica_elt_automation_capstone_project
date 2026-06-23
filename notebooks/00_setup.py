# Databricks notebook source
# MAGIC %md
# MAGIC # 00 Setup
# MAGIC Creates the database, folders, and pipeline metadata tables used by the capstone project.

# COMMAND ----------

from pyspark.sql import functions as F

BASE_PATH = "dbfs:/FileStore/online_retail_capstone"
DATABASE = "online_retail_capstone"

ARCHIVE_PATH = f"{BASE_PATH}/archives"
LANDING_PATH = f"{BASE_PATH}/landing"
RAW_PATH = f"{BASE_PATH}/raw"
BRONZE_PATH = f"{BASE_PATH}/delta/bronze_online_retail"
SILVER_PATH = f"{BASE_PATH}/delta/silver_online_retail"
GOLD_PATH = f"{BASE_PATH}/delta/gold"
CHECKPOINT_PATH = f"{BASE_PATH}/checkpoints"

for path in [
    ARCHIVE_PATH,
    LANDING_PATH,
    RAW_PATH,
    BRONZE_PATH,
    SILVER_PATH,
    GOLD_PATH,
    CHECKPOINT_PATH,
]:
    dbutils.fs.mkdirs(path)

spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
spark.sql(f"USE {DATABASE}")

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
    LOCATION '{BASE_PATH}/delta/pipeline_file_log'
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
    LOCATION '{BASE_PATH}/delta/pipeline_run_log'
    """
)

print(f"Database ready: {DATABASE}")
print(f"Base path ready: {BASE_PATH}")

