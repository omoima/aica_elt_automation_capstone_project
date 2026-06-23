# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Download and Partition Dataset
# MAGIC Uses an uploaded UCI Online Retail zip when present, otherwise downloads it, then splits it into monthly CSV files in `archives/`.

# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------

import os
import shutil
import tempfile
import urllib.request

import pandas as pd

CATALOG = "workspace"
SCHEMA = "online_retail_capstone"
VOLUME = "files"
DATABASE = f"{CATALOG}.{SCHEMA}"
BASE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
ARCHIVE_PATH = f"{BASE_PATH}/archives"
RAW_PATH = f"{BASE_PATH}/raw"
UPLOADED_ZIP_PATH = f"{RAW_PATH}/online+retail.zip"

DATASET_URL = "https://archive.ics.uci.edu/static/public/352/online+retail.zip"

dbutils.fs.mkdirs(ARCHIVE_PATH)
dbutils.fs.mkdirs(RAW_PATH)

with tempfile.TemporaryDirectory() as tmpdir:
    extract_dir = os.path.join(tmpdir, "extract")
    os.makedirs(extract_dir, exist_ok=True)

    uploaded_zip_exists = any(
        file.name == "online+retail.zip" for file in dbutils.fs.ls(RAW_PATH)
    )
    if uploaded_zip_exists:
        zip_path = UPLOADED_ZIP_PATH
        print(f"Using uploaded dataset zip: {UPLOADED_ZIP_PATH}")
    else:
        urllib.request.urlretrieve(DATASET_URL, UPLOADED_ZIP_PATH)
        zip_path = UPLOADED_ZIP_PATH
        print(f"Downloaded dataset zip to: {UPLOADED_ZIP_PATH}")

    shutil.unpack_archive(zip_path, extract_dir)

    excel_files = [
        os.path.join(extract_dir, name)
        for name in os.listdir(extract_dir)
        if name.lower().endswith((".xlsx", ".xls"))
    ]
    if not excel_files:
        raise FileNotFoundError("No Excel file found in the UCI archive.")

    source_excel = excel_files[0]
    shutil.copy(source_excel, f"{RAW_PATH}/Online Retail.xlsx")

    pdf = pd.read_excel(source_excel)
    pdf.columns = [column.strip().replace(" ", "") for column in pdf.columns]
    pdf["InvoiceDate"] = pd.to_datetime(pdf["InvoiceDate"])
    pdf["year_month"] = pdf["InvoiceDate"].dt.strftime("%Y_%m")

    for year_month, month_df in pdf.groupby("year_month"):
        output_path = f"{ARCHIVE_PATH}/sales_{year_month}.csv"
        month_df = month_df.drop(columns=["year_month"])
        month_df.to_csv(output_path, index=False)
        print(f"Created archive file: sales_{year_month}.csv rows={len(month_df)}")

print("Dataset partitioning complete.")
