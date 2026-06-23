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

1. `00_setup.py` creates the Unity Catalog schema, volume folders, and pipeline log tables.
2. `01_download_and_partition.py` uses an uploaded Online Retail zip file when present, otherwise downloads it, then writes monthly CSV files to `archives/`.
3. `02_hourly_file_arrival.py` moves one file per run from `archives/` to `landing/`.
4. `03_bronze_ingestion.py` appends only new landing files to the bronze Delta table.
5. `04_silver_transform.py` transforms only unprocessed bronze files into cleaned silver records.
6. `05_gold_aggregations.py` builds four business-facing gold Delta tables:
   - Weekly revenue
   - Top 5 products by revenue
   - Top 5 customers by spend
   - Revenue by country

## Databricks Setup

Import the files in `notebooks/` as Databricks notebooks. In this workspace the notebooks are under:

```text
/Workspace/Users/oarabilemoima@gmail.com/online-retail-capstone/notebooks
```

Run the notebooks once in this order:

```text
00_setup
01_download_and_partition
02_hourly_file_arrival
03_bronze_ingestion
04_silver_transform
05_gold_aggregations
```

Create a Databricks Workflow using `jobs/databricks_job_template.json` as a guide. The hourly task should run notebooks `02` through `05` in order.

Default Unity Catalog volume path for uploaded/raw CSV files:

```text
/Volumes/workspace/online_retail_capstone/files
```

Default catalog and schema:

```text
workspace.online_retail_capstone
```

Delta tables are created as managed Unity Catalog tables in that schema.

If you already downloaded `online+retail.zip`, run `00_setup.py` first, then upload the zip to the Unity Catalog volume path:

```text
/Volumes/workspace/online_retail_capstone/files/raw/online+retail.zip
```

Notebook `01_download_and_partition.py` uses that uploaded zip when present. If it is not present, it downloads the dataset from UCI.

Notebook `01_download_and_partition.py` installs `openpyxl` with `%pip` because pandas requires it to read the Excel file inside the UCI zip.

`03_bronze_ingestion.py` uses Unity Catalog file metadata (`_metadata.file_path`) instead of the older `input_file_name()` function, which is not supported on Unity Catalog serverless compute.

`05_gold_aggregations.py` avoids `REFRESH TABLE` because that command is not supported on Databricks serverless compute.

## Dashboard

Use the queries in `dashboard/dashboard_queries.sql` to create four Databricks SQL dashboard tiles from the gold tables.

Dashboard tables:

```text
workspace.online_retail_capstone.gold_weekly_revenue
workspace.online_retail_capstone.gold_top_products
workspace.online_retail_capstone.gold_top_customers
workspace.online_retail_capstone.gold_revenue_by_country
```

## Submission Evidence

Use `docs/submission_checklist.md` to track the required screenshots:

- Data workflow screenshot
- First successful pipeline run screenshot
- Last successful pipeline run screenshot
- Dashboard after first pipeline run
- Dashboard after last pipeline run
