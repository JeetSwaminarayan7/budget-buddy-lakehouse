"""Delta Lake behaviour: MERGE, time travel, schema enforcement. Needs Delta."""

import pytest
from pyspark.sql import functions as F

from budget_buddy_lakehouse import delta_ops as D

COLS = ["transaction_key", "transaction_id", "amount_gbp"]


@pytest.fixture
def table(delta_spark, request):
    name = f"default.test_{request.node.name}"
    delta_spark.sql(f"DROP TABLE IF EXISTS {name}")
    yield name
    delta_spark.sql(f"DROP TABLE IF EXISTS {name}")


def batch(spark, rows):
    return spark.createDataFrame(rows, COLS)


def test_merge_inserts_only_new_keys(delta_spark, table):
    assert D.merge_into_silver(delta_spark, batch(delta_spark, [("a", 1, 10.0), ("b", 2, 20.0)]), table) == 2
    # "b" again is a duplicate from a later upload; only "c" is new
    assert D.merge_into_silver(delta_spark, batch(delta_spark, [("b", 9, 20.0), ("c", 3, 30.0)]), table) == 1
    rows = {r["transaction_key"]: r["transaction_id"] for r in delta_spark.table(table).collect()}
    assert rows == {"a": 1, "b": 2, "c": 3}  # kept the first copy of "b"


def test_rerunning_the_same_batch_changes_nothing(delta_spark, table):
    b = batch(delta_spark, [("a", 1, 10.0)])
    D.merge_into_silver(delta_spark, b, table)
    assert D.merge_into_silver(delta_spark, b, table) == 0
    assert delta_spark.table(table).count() == 1


def test_bronze_skips_files_already_loaded(delta_spark, table):
    raw = delta_spark.createDataFrame([("x", "f1.csv"), ("y", "f1.csv")], ["id", "_source_file"])
    assert D.append_new_files_to_bronze(delta_spark, raw, table) == 2
    more = delta_spark.createDataFrame([("x", "f1.csv"), ("z", "f2.csv")], ["id", "_source_file"])
    assert D.append_new_files_to_bronze(delta_spark, more, table) == 1
    assert delta_spark.table(table).count() == 3


def test_time_travel_reads_an_old_version(delta_spark, table):
    D.merge_into_silver(delta_spark, batch(delta_spark, [("a", 1, 10.0)]), table)
    D.merge_into_silver(delta_spark, batch(delta_spark, [("b", 2, 20.0)]), table)
    assert delta_spark.sql(f"SELECT * FROM {table} VERSION AS OF 0").count() == 1
    assert delta_spark.table(table).count() == 2
    ops = [r["operation"] for r in D.table_versions(delta_spark, table).collect()]
    assert ops[0] == "MERGE"


def test_schema_enforcement_rejects_unexpected_columns(delta_spark, table):
    D.overwrite(batch(delta_spark, [("a", 1, 10.0)]), table)
    extra = batch(delta_spark, [("b", 2, 20.0)]).withColumn("surprise", F.lit("!"))
    with pytest.raises(Exception, match="(?i)schema"):
        extra.write.format("delta").mode("append").saveAsTable(table)
    # Allowed when you ask for it on purpose
    extra.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table)
    assert "surprise" in delta_spark.table(table).columns
