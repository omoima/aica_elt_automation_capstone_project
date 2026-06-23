# AI Community Africa Capstone Project

End-to-end incremental data pipeline using Databricks and PySpark for the UCI Online Retail dataset.

## Project Structure

```text
notebooks/
  00_setup.py
  01_download_and_partition.py
  02_hourly_file_arrival.py
  03_bronze_ingestion.py
  04_silver_transform.py
  05_gold_aggregations.py
dashboard/
  dashboard_queries.sql
jobs/
  databricks_job_template.json
docs/
  submission_checklist.md
```

## Pipeline Summary

The project simulates production-style hourly data arrival from a single historical dataset.

1. `01_download_and_partition.py` downloads the Online Retail Excel dataset and writes monthly CSV files to `archives/`.
2. `02_hourly_file_arrival.py` moves one file per run from `archives/` to `landing/`.
3. `03_bronze_ingestion.py` appends only new landing files to the bronze Delta table.
4. `04_silver_transform.py` transforms only unprocessed bronze files into cleaned silver records.
5. `05_gold_aggregations.py` builds four business-facing gold Delta tables:
   - Weekly revenue
   - Top 5 products by revenue
   - Top 5 customers by spend
   - Revenue by country

## Databricks Setup

Import the files in `notebooks/` as Databricks notebooks, then run:

```text
00_setup
01_download_and_partition
02_hourly_file_arrival
03_bronze_ingestion
04_silver_transform
05_gold_aggregations
```

Create a Databricks Workflow using `jobs/databricks_job_template.json` as a guide. The hourly task should run notebooks `02` through `05` in order.

Default storage path:

```text
dbfs:/FileStore/online_retail_capstone
```

Default schema:

```text
online_retail_capstone
```

## Dashboard

Use the queries in `dashboard/dashboard_queries.sql` to create four Databricks SQL dashboard tiles from the gold tables.

## Submission Evidence

Use `docs/submission_checklist.md` to track the required screenshots:

- Data workflow screenshot
- First successful pipeline run screenshot
- Last successful pipeline run screenshot
- Dashboard after first pipeline run
- Dashboard after last pipeline run

