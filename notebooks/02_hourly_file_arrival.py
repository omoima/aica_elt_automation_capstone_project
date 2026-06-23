# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Hourly File Arrival
# MAGIC Simulates production ingestion by moving one monthly CSV file from `archives/` to `landing/` each run.

# COMMAND ----------

from pyspark.sql import Row, functions as F

CATALOG = "workspace"
SCHEMA = "online_retail_capstone"
VOLUME = "files"
DATABASE = f"{CATALOG}.{SCHEMA}"
BASE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
ARCHIVE_PATH = f"{BASE_PATH}/archives"
LANDING_PATH = f"{BASE_PATH}/landing"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")
dbutils.fs.mkdirs(ARCHIVE_PATH)
dbutils.fs.mkdirs(LANDING_PATH)

archive_files = sorted(
    [
        file
        for file in dbutils.fs.ls(ARCHIVE_PATH)
        if file.name.startswith("sales_") and file.name.endswith(".csv")
    ],
    key=lambda file: file.name,
)

if not archive_files:
    message = "No archive files left to move."
    print(message)
    dbutils.notebook.exit(message)

next_file = archive_files[0]
target_path = f"{LANDING_PATH}/{next_file.name}"

dbutils.fs.mv(next_file.path, target_path)

log_df = spark.createDataFrame(
    [
        Row(
            file_name=next_file.name,
            source_path=next_file.path,
            target_path=target_path,
            stage="landing",
        )
    ]
).withColumn("processed_at", F.current_timestamp())

log_df.write.format("delta").mode("append").saveAsTable(f"{DATABASE}.pipeline_file_log")

print(f"Moved {next_file.path} to {target_path}")
