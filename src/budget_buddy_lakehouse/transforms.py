"""
The transformations for each layer. These are plain DataFrame in,
DataFrame out functions with no reading or writing, so they work the same
on Databricks, on a laptop, and in tests.

SQL functions like try_cast are called through F.expr because they're the
same on open-source Spark and every Databricks runtime.
"""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

# ── Bronze: raw files, kept exactly as they arrived ──────────────────────────

# Every column is read as text. Bronze never fails on bad data; it just
# keeps a copy of what came in. Cleaning is silver's job.
RAW_TRANSACTION_SCHEMA = StructType([
    StructField(name, StringType()) for name in
    ["id", "user_id", "date", "description", "amount", "type", "category_id", "source", "created_at"]
])


def with_ingest_metadata(df: DataFrame) -> DataFrame:
    """Add which file each row came from and when it was loaded."""
    return (
        df.withColumn("_source_file", F.col("_metadata.file_path"))
          .withColumn("_ingested_at", F.current_timestamp())
    )


def read_raw_transactions(spark: SparkSession, path: str) -> DataFrame:
    """Batch-read every transactions CSV under a folder, as bronze rows."""
    df = (
        spark.read
        .option("header", "true")
        .schema(RAW_TRANSACTION_SCHEMA)
        .csv(path)
    )
    return with_ingest_metadata(df)


# ── Silver: cleaned, typed, validated and deduplicated ───────────────────────

def _try_date(col: str) -> F.Column:
    # The app wrote dates as 2026-03-05 or 05/03/2026
    return F.coalesce(
        F.expr(f"try_to_timestamp(trim({col}), 'yyyy-MM-dd')"),
        F.expr(f"try_to_timestamp(trim({col}), 'dd/MM/yyyy')"),
    ).cast("date")


def clean_transactions(bronze: DataFrame) -> DataFrame:
    """
    Fix types and tidy text. Doesn't drop anything: rows that can't be
    cleaned get a reason in _dq_error instead.
    """
    df = bronze.select(
        F.expr("try_cast(trim(id) as int)").alias("transaction_id"),
        F.expr("try_cast(trim(user_id) as int)").alias("user_id"),
        _try_date("date").alias("transaction_date"),
        F.upper(F.regexp_replace(F.trim("description"), r"\s+", " ")).alias("description"),
        F.expr("try_cast(replace(trim(amount), '£', '') as decimal(12, 2))").alias("amount_gbp"),
        F.lower(F.trim("type")).alias("transaction_type"),
        F.expr("try_cast(nullif(trim(category_id), '') as int)").alias("category_id"),
        F.lower(F.trim("source")).alias("source"),
        F.expr("try_cast(trim(created_at) as timestamp)").alias("created_at"),
        "_source_file",
        "_ingested_at",
    )

    df = df.withColumn(
        "signed_amount_gbp",
        F.when(F.col("transaction_type") == "credit", F.col("amount_gbp")).otherwise(-F.col("amount_gbp")),
    )

    # The same real-world transaction always gets the same key, even if it
    # arrives twice with different ids (a statement uploaded again)
    df = df.withColumn("transaction_key", F.sha2(F.concat_ws(
        "|", "user_id", "transaction_date", "description", "amount_gbp", "transaction_type"), 256))

    return df.withColumn(
        "_dq_error",
        F.when(F.col("transaction_id").isNull(), "missing or bad id")
         .when(F.col("user_id").isNull(), "missing or bad user_id")
         .when(F.col("transaction_date").isNull(), "unparseable date")
         .when(F.col("amount_gbp").isNull(), "bad amount")
         .when(F.col("amount_gbp") <= 0, "amount not positive")
         .when(~F.col("transaction_type").isin("debit", "credit"), "unknown transaction type"),
    )


def split_valid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return (rows that passed every check, rows that failed one)."""
    return df.filter(F.col("_dq_error").isNull()).drop("_dq_error"), df.filter(F.col("_dq_error").isNotNull())


def deduplicate(df: DataFrame) -> DataFrame:
    """Keep the first copy (lowest id) of each transaction_key."""
    w = Window.partitionBy("transaction_key").orderBy("transaction_id")
    return df.withColumn("_copy", F.row_number().over(w)).filter("_copy = 1").drop("_copy")


def clean_categories(raw: DataFrame) -> DataFrame:
    return raw.select(
        F.expr("try_cast(trim(id) as int)").alias("category_id"),
        F.regexp_replace(F.trim("name"), r"\s+", " ").alias("category_name"),
    )


def clean_users(raw: DataFrame) -> DataFrame:
    return raw.select(
        F.expr("try_cast(trim(id) as int)").alias("user_id"),
        F.lower(F.trim("username")).alias("username"),
        F.expr("try_cast(trim(monthly_budget) as decimal(12, 2))").alias("monthly_budget_gbp"),
        F.expr("try_cast(trim(savings_goal) as decimal(12, 2))").alias("savings_goal_gbp"),
    )


def add_categories(transactions: DataFrame, categories: DataFrame) -> DataFrame:
    """Attach the category name and the month to each transaction."""
    return (
        transactions.join(F.broadcast(categories), "category_id", "left")
        .withColumn("category_name", F.coalesce("category_name", F.lit("Uncategorised")))
        .withColumn("transaction_month", F.trunc("transaction_date", "month"))
    )


def build_silver(bronze: DataFrame, categories: DataFrame) -> tuple[DataFrame, DataFrame]:
    """bronze -> (silver rows, quarantined rows)."""
    valid, quarantine = split_valid(clean_transactions(bronze))
    silver = add_categories(deduplicate(valid), categories)
    return silver, quarantine


# ── Gold: business-level tables for reporting ────────────────────────────────

def monthly_category_spend(silver: DataFrame) -> DataFrame:
    return (
        silver.filter(F.col("transaction_type") == "debit")
        .groupBy("user_id", "transaction_month", "category_name")
        .agg(
            F.count("*").alias("transaction_count"),
            F.sum("amount_gbp").alias("spend_gbp"),
        )
    )


def monthly_cashflow(silver: DataFrame, users: DataFrame) -> DataFrame:
    monthly = silver.groupBy("user_id", "transaction_month").agg(
        F.sum(F.when(F.col("transaction_type") == "credit", F.col("amount_gbp")).otherwise(0)).alias("income_gbp"),
        F.sum(F.when(F.col("transaction_type") == "debit", F.col("amount_gbp")).otherwise(0)).alias("spend_gbp"),
        F.sum("signed_amount_gbp").alias("net_gbp"),
    )
    return (
        monthly.join(users.select("user_id", "savings_goal_gbp"), "user_id")
        .withColumn("hit_savings_goal", F.col("net_gbp") >= F.col("savings_goal_gbp"))
        .withColumn("savings_rate_pct",
                    F.round(F.col("net_gbp") / F.nullif(F.col("income_gbp"), F.lit(0)) * 100, 1))
    )
