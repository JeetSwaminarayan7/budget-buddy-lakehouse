"""Writing each layer to Delta tables."""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def table_exists(spark: SparkSession, name: str) -> bool:
    return spark.catalog.tableExists(name)


def append_new_files_to_bronze(spark: SparkSession, raw: DataFrame, table: str) -> int:
    """
    Append raw rows, skipping any file already loaded, so running it twice
    doesn't load the same file twice. On Databricks, Auto Loader does this
    job for you (see notebooks/01_bronze.py). Returns rows appended.
    """
    if table_exists(spark, table):
        loaded = spark.table(table).select("_source_file").distinct()
        raw = raw.join(loaded, "_source_file", "left_anti")
    rows = raw.count()
    if rows:
        raw.write.format("delta").mode("append").saveAsTable(table)
    return rows


def merge_into_silver(spark: SparkSession, silver: DataFrame, table: str) -> int:
    """
    Insert transactions silver hasn't seen before, matched on transaction_key.
    A duplicate that turns up in a later batch matches an existing row and is
    skipped, and rerunning the same batch changes nothing. Returns rows inserted.
    """
    if not table_exists(spark, table):
        silver.write.format("delta").saveAsTable(table)
        return spark.table(table).count()

    (
        DeltaTable.forName(spark, table).alias("t")
        .merge(silver.alias("s"), "t.transaction_key = s.transaction_key")
        .whenNotMatchedInsertAll()
        .execute()
    )
    metrics = DeltaTable.forName(spark, table).history(1).select("operationMetrics").first()[0]
    return int(metrics.get("numTargetRowsInserted", 0))


def overwrite(df: DataFrame, table: str) -> None:
    """Replace a table's contents. Used for gold, which is rebuilt each run."""
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)


def table_versions(spark: SparkSession, table: str) -> DataFrame:
    """Every change made to a table, newest first (Delta keeps them all)."""
    return (
        DeltaTable.forName(spark, table).history()
        .select("version", "timestamp", "operation",
                F.col("operationMetrics").getItem("numOutputRows").alias("rows_written"),
                F.col("operationMetrics").getItem("numTargetRowsInserted").alias("rows_inserted"))
    )
