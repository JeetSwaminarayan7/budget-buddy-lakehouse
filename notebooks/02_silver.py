# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver: clean, validate, deduplicate
# MAGIC
# MAGIC Silver is one trustworthy row per real transaction:
# MAGIC
# MAGIC - **Typed and tidied.** Both date formats parsed, `£` stripped, descriptions trimmed and uppercased.
# MAGIC - **Validated.** Rows that fail a rule (unparseable date, bad or negative amount, unknown type,
# MAGIC   missing id) go to `silver_transactions_quarantine` with the reason, instead of disappearing.
# MAGIC - **Deduplicated.** Every transaction gets a `transaction_key`, a hash of user, date, description,
# MAGIC   amount and type. A re-uploaded statement produces the same keys with new ids.
# MAGIC - **Merged, not overwritten.** `MERGE` inserts only keys silver hasn't seen. A duplicate arriving in a
# MAGIC   later file is skipped, and rerunning this notebook changes nothing.

# COMMAND ----------

import sys, os
sys.path.append(os.path.abspath("../src"))

from budget_buddy_lakehouse import transforms as T
from budget_buddy_lakehouse import delta_ops as D

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "budget_buddy")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
db = f"{catalog}.{schema}"
volume = f"/Volumes/{catalog}/{schema}/raw"

# COMMAND ----------

categories = T.clean_categories(spark.read.option("header", "true").csv(f"{volume}/reference/categories.csv"))
users = T.clean_users(spark.read.option("header", "true").csv(f"{volume}/reference/users.csv"))
D.overwrite(categories, f"{db}.silver_categories")
D.overwrite(users, f"{db}.silver_users")

silver, quarantine = T.build_silver(spark.table(f"{db}.bronze_transactions"), categories)

inserted = D.merge_into_silver(spark, silver, f"{db}.silver_transactions")
D.overwrite(quarantine, f"{db}.silver_transactions_quarantine")

print(f"Inserted {inserted} new transactions")
print(f"silver_transactions now has {spark.table(f'{db}.silver_transactions').count()} rows")
print(f"{quarantine.count()} rows quarantined")

# COMMAND ----------

# MAGIC %md
# MAGIC What failed and why. After landing October you should see five rows here, one for each rule.

# COMMAND ----------

display(spark.table(f"{db}.silver_transactions_quarantine")
        .select("_dq_error", "transaction_id", "_source_file"))

# COMMAND ----------

# MAGIC %md
# MAGIC How many duplicate rows bronze has that silver doesn't.

# COMMAND ----------

display(spark.sql(f"""
    SELECT
      (SELECT count(*) FROM {db}.bronze_transactions)            AS bronze_rows,
      (SELECT count(*) FROM {db}.silver_transactions)            AS silver_rows,
      (SELECT count(*) FROM {db}.silver_transactions_quarantine) AS quarantined_rows
"""))
