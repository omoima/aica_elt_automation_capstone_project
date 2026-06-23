# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Bronze Ingestion
# MAGIC Appends only newly landed CSV files to the bronze Delta table.

# COMMAND ----------

from pyspark.sql import Row, functions as F

BASE_PATH = "dbfs:/FileStore/online_retail_capstone"
DATABASE = "online_retail_capstone"
LANDING_PATH = f"{BASE_PATH}/landing"
BRONZE_PATH = f"{BASE_PATH}/delta/bronze_online_retail"

spark.sql(f"USE {DATABASE}")

landing_files = sorted(
    [
        file
        for file in dbutils.fs.ls(LANDING_PATH)
        if file.name.startswith("sales_") and file.name.endswith(".csv")
    ],
    key=lambda file: file.name,
)

processed_files = {
    row.file_name
    for row in spark.table(f"{DATABASE}.pipeline_file_log")
    .where(F.col("stage") == "bronze")
    .select("file_name")
    .distinct()
    .collect()
}

new_files = [file for file in landing_files if file.name not in processed_files]

if not new_files:
    message = "No new landing files for bronze ingestion."
    print(message)
    dbutils.notebook.exit(message)

paths = [file.path for file in new_files]

bronze_df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv(paths)
    .withColumn("_source_file", F.input_file_name())
    .withColumn("_ingested_at", F.current_timestamp())
)

bronze_df.write.format("delta").mode("append").option("mergeSchema", "true").save(BRONZE_PATH)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {DATABASE}.bronze_online_retail
    USING DELTA
    LOCATION '{BRONZE_PATH}'
    """
)

log_rows = [
    Row(
        file_name=file.name,
        source_path=file.path,
        target_path=BRONZE_PATH,
        stage="bronze",
    )
    for file in new_files
]

spark.createDataFrame(log_rows).withColumn("processed_at", F.current_timestamp()).write.format(
    "delta"
).mode("append").saveAsTable(f"{DATABASE}.pipeline_file_log")

row_count = bronze_df.count()
print(f"Bronze ingestion complete. files={len(new_files)} rows={row_count}")

