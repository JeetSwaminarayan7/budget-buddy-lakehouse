# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · What Delta Lake gives you
# MAGIC
# MAGIC Delta tables are Parquet files plus a transaction log (`_delta_log`) recording every change. That
# MAGIC log is what makes the things below possible. Run this after 01 to 03, ideally after landing
# MAGIC October so the tables have more than one version.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "budget_buddy")
db = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}"
silver = f"{db}.silver_transactions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## History
# MAGIC Every write is a numbered version, with what operation made it and how many rows it touched.

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {silver}")
        .select("version", "timestamp", "operation", "operationMetrics"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time travel
# MAGIC Query the table as it was at an earlier version. Useful for "what did the report say last week?"
# MAGIC and for checking what a bad load changed.

# COMMAND ----------

display(spark.sql(f"""
    SELECT 'version 0' AS version, count(*) AS rows FROM {silver} VERSION AS OF 0
    UNION ALL
    SELECT 'now', count(*) FROM {silver}
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Undoing a mistake
# MAGIC Delete some rows by accident, see them gone, then put the table back.

# COMMAND ----------

before = spark.table(silver).count()
version_before = spark.sql(f"DESCRIBE HISTORY {silver} LIMIT 1").first()["version"]

spark.sql(f"DELETE FROM {silver} WHERE category_name = 'Groceries'")
print("after the accident:", spark.table(silver).count(), "rows (was", before, ")")

spark.sql(f"RESTORE TABLE {silver} TO VERSION AS OF {version_before}")
print("after RESTORE:", spark.table(silver).count(), "rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema enforcement
# MAGIC Delta refuses writes that don't match the table's columns, so a broken upstream change can't
# MAGIC quietly corrupt the table. You have to opt in to adding columns with `mergeSchema`.

# COMMAND ----------

from pyspark.sql import functions as F

extra_column = spark.table(silver).limit(1).withColumn("unexpected", F.lit("oops"))
try:
    extra_column.write.format("delta").mode("append").saveAsTable(silver)
except Exception as e:
    print("Rejected, as it should be:\n", str(e).splitlines()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tidying the files
# MAGIC Lots of small writes leave lots of small files. `OPTIMIZE` compacts them, and `ZORDER BY` sorts the
# MAGIC data so queries filtering on `user_id` can skip files that can't match.

# COMMAND ----------

display(spark.sql(f"OPTIMIZE {silver} ZORDER BY (user_id)"))
