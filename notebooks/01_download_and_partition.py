# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Download and Partition Dataset
# MAGIC Downloads the UCI Online Retail dataset and splits it into monthly CSV files in `archives/`.

# COMMAND ----------

import os
import shutil
import subprocess
import tempfile

import pandas as pd

BASE_PATH = "dbfs:/FileStore/online_retail_capstone"
DATABASE = "online_retail_capstone"
ARCHIVE_PATH = f"{BASE_PATH}/archives"
RAW_PATH = f"{BASE_PATH}/raw"

DATASET_URL = "https://archive.ics.uci.edu/static/public/352/online+retail.zip"

dbutils.fs.mkdirs(ARCHIVE_PATH)
dbutils.fs.mkdirs(RAW_PATH)

with tempfile.TemporaryDirectory() as tmpdir:
    zip_path = os.path.join(tmpdir, "online_retail.zip")
    extract_dir = os.path.join(tmpdir, "extract")
    os.makedirs(extract_dir, exist_ok=True)

    subprocess.check_call(["curl", "-L", DATASET_URL, "-o", zip_path])
    shutil.unpack_archive(zip_path, extract_dir)

    excel_files = [
        os.path.join(extract_dir, name)
        for name in os.listdir(extract_dir)
        if name.lower().endswith((".xlsx", ".xls"))
    ]
    if not excel_files:
        raise FileNotFoundError("No Excel file found in the downloaded UCI archive.")

    source_excel = excel_files[0]
    raw_copy = os.path.join(tmpdir, "Online Retail.xlsx")
    shutil.copy(source_excel, raw_copy)
    dbutils.fs.cp(f"file:{raw_copy}", f"{RAW_PATH}/Online Retail.xlsx")

    pdf = pd.read_excel(source_excel)
    pdf.columns = [column.strip().replace(" ", "") for column in pdf.columns]
    pdf["InvoiceDate"] = pd.to_datetime(pdf["InvoiceDate"])
    pdf["year_month"] = pdf["InvoiceDate"].dt.strftime("%Y_%m")

    for year_month, month_df in pdf.groupby("year_month"):
        output_path = os.path.join(tmpdir, f"sales_{year_month}.csv")
        month_df = month_df.drop(columns=["year_month"])
        month_df.to_csv(output_path, index=False)
        dbutils.fs.cp(f"file:{output_path}", f"{ARCHIVE_PATH}/sales_{year_month}.csv")
        print(f"Created archive file: sales_{year_month}.csv rows={len(month_df)}")

print("Dataset partitioning complete.")

