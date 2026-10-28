import os
import shutil
import sys
import tempfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pyspark.sql import SparkSession  # noqa: E402


def _builder(warehouse):
    return (
        SparkSession.builder.master("local[2]")
        .appName("budget-buddy-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.session.timeZone", "UTC")
    )


@pytest.fixture(scope="session")
def spark_with_delta_flag():
    """
    A local SparkSession with Delta Lake if its jars can be downloaded,
    otherwise plain Spark. Tests marked `delta` are skipped without it.
    """
    warehouse = tempfile.mkdtemp(prefix="bb-warehouse-")
    has_delta = False
    if os.environ.get("NO_DELTA") != "1":
        try:
            from delta import configure_spark_with_delta_pip
            builder = _builder(warehouse) \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            spark = configure_spark_with_delta_pip(builder).getOrCreate()
            spark.range(1).write.format("delta").mode("overwrite").save(os.path.join(warehouse, "_probe"))
            has_delta = True
        except Exception as e:  # jars not downloadable, e.g. no internet
            print(f"\nDelta Lake not available, running without it: {type(e).__name__}")
            SparkSession.getActiveSession() and SparkSession.getActiveSession().stop()
            spark = None
    if not has_delta:
        spark = _builder(warehouse).getOrCreate()
    yield spark, has_delta
    spark.stop()
    shutil.rmtree(warehouse, ignore_errors=True)


@pytest.fixture(scope="session")
def spark(spark_with_delta_flag):
    return spark_with_delta_flag[0]


@pytest.fixture
def delta_spark(spark_with_delta_flag):
    spark, has_delta = spark_with_delta_flag
    if not has_delta:
        pytest.skip("Delta Lake jars not available")
    return spark
