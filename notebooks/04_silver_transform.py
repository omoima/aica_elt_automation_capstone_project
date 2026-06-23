# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Silver Transform
# MAGIC Cleans and enriches only bronze files that have not yet been transformed into silver.

# COMMAND ----------

from pyspark.sql import Row, functions as F

BASE_PATH = "dbfs:/FileStore/online_retail_capstone"
DATABASE = "online_retail_capstone"
SILVER_PATH = f"{BASE_PATH}/delta/silver_online_retail"

spark.sql(f"USE {DATABASE}")

bronze_files = {
    row.file_name
    for row in spark.table(f"{DATABASE}.pipeline_file_log")
    .where(F.col("stage") == "bronze")
    .select("file_name")
    .distinct()
    .collect()
}

silver_files = {
    row.file_name
    for row in spark.table(f"{DATABASE}.pipeline_file_log")
    .where(F.col("stage") == "silver")
    .select("file_name")
    .distinct()
    .collect()
}

new_file_names = sorted(bronze_files - silver_files)

if not new_file_names:
    message = "No new bronze files for silver transformation."
    print(message)
    dbutils.notebook.exit(message)

new_source_paths = [f"{BASE_PATH}/landing/{file_name}" for file_name in new_file_names]

silver_df = (
    spark.table(f"{DATABASE}.bronze_online_retail")
    .withColumn("_file_name", F.regexp_extract(F.col("_source_file"), r"([^/]+$)", 1))
    .where(F.col("_file_name").isin(new_file_names))
    .withColumnRenamed("InvoiceNo", "invoice_no")
    .withColumnRenamed("StockCode", "stock_code")
    .withColumnRenamed("Description", "description")
    .withColumnRenamed("Quantity", "quantity")
    .withColumnRenamed("InvoiceDate", "invoice_date")
    .withColumnRenamed("UnitPrice", "unit_price")
    .withColumnRenamed("CustomerID", "customer_id")
    .withColumnRenamed("Country", "country")
    .withColumn("invoice_date", F.to_timestamp("invoice_date"))
    .withColumn("quantity", F.col("quantity").cast("integer"))
    .withColumn("unit_price", F.col("unit_price").cast("double"))
    .withColumn("customer_id", F.col("customer_id").cast("long"))
    .withColumn("description", F.trim(F.col("description")))
    .withColumn("country", F.trim(F.col("country")))
    .withColumn("revenue", F.round(F.col("quantity") * F.col("unit_price"), 2))
    .withColumn("invoice_date_key", F.to_date("invoice_date"))
    .withColumn("invoice_year", F.year("invoice_date"))
    .withColumn("invoice_month", F.month("invoice_date"))
    .where(F.col("invoice_no").isNotNull())
    .where(F.col("stock_code").isNotNull())
    .where(F.col("invoice_date").isNotNull())
    .where(F.col("quantity") > 0)
    .where(F.col("unit_price") > 0)
    .where(~F.upper(F.col("invoice_no")).startswith("C"))
    .select(
        "invoice_no",
        "stock_code",
        "description",
        "quantity",
        "invoice_date",
        "invoice_date_key",
        "invoice_year",
        "invoice_month",
        "unit_price",
        "customer_id",
        "country",
        "revenue",
        "_file_name",
        "_ingested_at",
    )
)

silver_df.write.format("delta").mode("append").option("mergeSchema", "true").save(SILVER_PATH)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {DATABASE}.silver_online_retail
    USING DELTA
    LOCATION '{SILVER_PATH}'
    """
)

log_rows = [
    Row(
        file_name=file_name,
        source_path=source_path,
        target_path=SILVER_PATH,
        stage="silver",
    )
    for file_name, source_path in zip(new_file_names, new_source_paths)
]

spark.createDataFrame(log_rows).withColumn("processed_at", F.current_timestamp()).write.format(
    "delta"
).mode("append").saveAsTable(f"{DATABASE}.pipeline_file_log")

row_count = silver_df.count()
print(f"Silver transformation complete. files={len(new_file_names)} rows={row_count}")

