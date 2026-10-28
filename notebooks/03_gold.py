# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold: tables for reporting
# MAGIC
# MAGIC Gold tables answer business questions directly and are what a dashboard would read. They're
# MAGIC rebuilt from silver on every run, which is simple and cheap at this size.
# MAGIC
# MAGIC - `gold_monthly_category_spend`: spending per user, month and category
# MAGIC - `gold_monthly_cashflow`: income, spending and net per user per month, against their savings goal

# COMMAND ----------

import sys, os
sys.path.append(os.path.abspath("../src"))

from budget_buddy_lakehouse import transforms as T
from budget_buddy_lakehouse import delta_ops as D

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "budget_buddy")
db = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}"

# COMMAND ----------

silver = spark.table(f"{db}.silver_transactions")
users = spark.table(f"{db}.silver_users")

D.overwrite(T.monthly_category_spend(silver), f"{db}.gold_monthly_category_spend")
D.overwrite(T.monthly_cashflow(silver, users), f"{db}.gold_monthly_cashflow")

# COMMAND ----------

# MAGIC %md
# MAGIC Where the money goes, across all users. Click the **+** above the results to turn it into a chart.

# COMMAND ----------

display(spark.sql(f"""
    SELECT transaction_month, category_name, round(sum(spend_gbp), 2) AS spend_gbp
    FROM {db}.gold_monthly_category_spend
    GROUP BY ALL
    ORDER BY transaction_month, spend_gbp DESC
"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT transaction_month,
           count_if(hit_savings_goal) AS users_hit_goal,
           count(*)                   AS users,
           round(avg(savings_rate_pct), 1) AS avg_savings_rate_pct
    FROM {db}.gold_monthly_cashflow
    GROUP BY transaction_month
    ORDER BY transaction_month
"""))
