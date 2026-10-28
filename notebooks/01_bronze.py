# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze: land the raw files
# MAGIC
# MAGIC Bronze is a copy of the data exactly as it arrived. Every column is kept as text, nothing is
# MAGIC cleaned or dropped, and each row records which file it came from and when it was loaded. If a
# MAGIC cleaning rule later turns out to be wrong, silver can be rebuilt from bronze without asking the
# MAGIC source system for the data again.
# MAGIC
# MAGIC **Auto Loader** (`cloudFiles`) reads the folder and keeps a checkpoint of which files it has
# MAGIC already loaded, so each run only picks up new files. `availableNow` means: load whatever is new,
# MAGIC then stop, like a batch job, rather than running forever.

# COMMAND ----------

import sys, os
sys.path.append(os.path.abspath("../src"))

from budget_buddy_lakehouse.transforms import RAW_TRANSACTION_SCHEMA, with_ingest_metadata

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "budget_buddy")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

volume = f"/Volumes/{catalog}/{schema}/raw"
bronze_table = f"{catalog}.{schema}.bronze_transactions"

# COMMAND ----------

stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .schema(RAW_TRANSACTION_SCHEMA)
    .load(f"{volume}/transactions")
)

query = (
    with_ingest_metadata(stream)
    .writeStream
    .option("checkpointLocation", f"{volume}/_checkpoints/bronze_transactions")
    .trigger(availableNow=True)
    .toTable(bronze_table)
)
query.awaitTermination()
print(query.lastProgress and f"Loaded {query.lastProgress['numInputRows']} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC Rows per file. Run this notebook again and nothing new is loaded, because Auto Loader remembers
# MAGIC the file. Then run the last cell to land October's file, and run the notebook again.

# COMMAND ----------

display(spark.sql(f"""
    SELECT _source_file, count(*) AS rows, max(_ingested_at) AS loaded_at
    FROM {bronze_table}
    GROUP BY _source_file
"""))

# COMMAND ----------

# Land October: move it from incoming/ into transactions/, as if a new export arrived
dbutils.fs.mv(f"{volume}/incoming/transactions_2026_10.csv", f"{volume}/transactions/transactions_2026_10.csv")
