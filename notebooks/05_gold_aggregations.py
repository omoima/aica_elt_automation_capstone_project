# Databricks notebook source
# MAGIC %md
# MAGIC # 05 Gold Aggregations
# MAGIC Builds business-ready gold tables from the updated silver Delta table.

# COMMAND ----------

from pyspark.sql import functions as F

BASE_PATH = "dbfs:/FileStore/online_retail_capstone"
DATABASE = "online_retail_capstone"
GOLD_PATH = f"{BASE_PATH}/delta/gold"

spark.sql(f"USE {DATABASE}")

silver_df = spark.table(f"{DATABASE}.silver_online_retail")

weekly_revenue = (
    silver_df.withColumn("week_start", F.date_trunc("week", F.col("invoice_date")).cast("date"))
    .groupBy("week_start")
    .agg(
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.countDistinct("invoice_no").alias("total_invoices"),
        F.sum("quantity").alias("total_units"),
    )
    .orderBy("week_start")
)

top_products = (
    silver_df.groupBy("stock_code", "description")
    .agg(
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.sum("quantity").alias("total_units"),
    )
    .orderBy(F.desc("total_revenue"))
    .limit(5)
)

top_customers = (
    silver_df.where(F.col("customer_id").isNotNull())
    .groupBy("customer_id")
    .agg(
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.countDistinct("invoice_no").alias("total_invoices"),
        F.sum("quantity").alias("total_units"),
    )
    .orderBy(F.desc("total_revenue"))
    .limit(5)
)

revenue_by_country = (
    silver_df.groupBy("country")
    .agg(
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.countDistinct("invoice_no").alias("total_invoices"),
        F.sum("quantity").alias("total_units"),
    )
    .orderBy(F.desc("total_revenue"))
)

gold_tables = {
    "gold_weekly_revenue": weekly_revenue,
    "gold_top_products": top_products,
    "gold_top_customers": top_customers,
    "gold_revenue_by_country": revenue_by_country,
}

for table_name, table_df in gold_tables.items():
    table_path = f"{GOLD_PATH}/{table_name}"
    table_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(table_path)
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.{table_name}
        USING DELTA
        LOCATION '{table_path}'
        """
    )
    spark.sql(f"REFRESH TABLE {DATABASE}.{table_name}")
    print(f"Updated {DATABASE}.{table_name}: rows={table_df.count()}")

print("Gold aggregation complete.")

