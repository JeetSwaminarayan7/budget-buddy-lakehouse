# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup
# MAGIC
# MAGIC Creates a schema for the project and a **volume** to hold the raw files, then copies the sample
# MAGIC CSVs from this repo into the volume.
# MAGIC
# MAGIC A volume is Unity Catalog's place for files (as opposed to tables). Raw files land there, and the
# MAGIC bronze notebook reads them from it.
# MAGIC
# MAGIC Run this once.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "budget_buddy")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.raw")

volume = f"/Volumes/{catalog}/{schema}/raw"
print("Raw files go in", volume)

# COMMAND ----------

# MAGIC %md
# MAGIC Copy the sample data from the repo into the volume. Only the January to September file goes into
# MAGIC `transactions/`; October waits in `incoming/` so you can land it later and watch the pipeline pick
# MAGIC up just the new file.
# MAGIC
# MAGIC If this cell fails, upload the files by hand instead: Catalog → workspace → budget_buddy → raw →
# MAGIC "Upload to this volume", keeping the same folders as `data/` in the repo.

# COMMAND ----------

import os
import shutil

repo_data = os.path.abspath("../data")

copies = {
    f"{repo_data}/raw/transactions/transactions_2026_q1_q3.csv": f"{volume}/transactions/",
    f"{repo_data}/raw/reference/users.csv": f"{volume}/reference/",
    f"{repo_data}/raw/reference/categories.csv": f"{volume}/reference/",
    f"{repo_data}/incoming/transactions_2026_10.csv": f"{volume}/incoming/",
}
for src, dest in copies.items():
    os.makedirs(dest, exist_ok=True)
    shutil.copy(src, dest)
    print("copied", os.path.basename(src), "->", dest)

# COMMAND ----------

display(dbutils.fs.ls(f"{volume}/transactions"))
