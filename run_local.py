"""
Run the bronze -> silver -> gold pipeline on your own machine with Spark
and Delta Lake, using the same code as the Databricks notebooks.

  pip install -r requirements.txt
  python run_local.py                 # first run: January to September
  python run_local.py --land-october  # land October's file, then run again

Tables are saved under ./spark-warehouse. Delete that folder to start over.
The first run downloads the Delta Lake jars, so it needs internet.
"""

import argparse
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from delta import configure_spark_with_delta_pip  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

from budget_buddy_lakehouse import delta_ops as D  # noqa: E402
from budget_buddy_lakehouse import transforms as T  # noqa: E402

DATA = ROOT / "data"
DB = "budget_buddy"


def get_spark():
    builder = (
        SparkSession.builder.master("local[*]")
        .appName("budget-buddy-lakehouse")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(ROOT / "spark-warehouse"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        # Keep the table catalogue between runs
        .config("javax.jdo.option.ConnectionURL", f"jdbc:derby:;databaseName={ROOT / 'metastore_db'};create=true")
        .enableHiveSupport()
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--land-october", action="store_true",
                        help="move October's file from data/incoming into data/raw/transactions first")
    args = parser.parse_args()

    if args.land_october:
        src = DATA / "incoming" / "transactions_2026_10.csv"
        if src.exists():
            shutil.move(src, DATA / "raw" / "transactions" / src.name)
            print("Landed", src.name)

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB}")

    # Bronze
    raw = T.read_raw_transactions(spark, str(DATA / "raw" / "transactions"))
    added = D.append_new_files_to_bronze(spark, raw, f"{DB}.bronze_transactions")
    print(f"bronze: +{added} rows")

    # Silver
    categories = T.clean_categories(spark.read.option("header", "true").csv(str(DATA / "raw" / "reference" / "categories.csv")))
    users = T.clean_users(spark.read.option("header", "true").csv(str(DATA / "raw" / "reference" / "users.csv")))
    D.overwrite(categories, f"{DB}.silver_categories")
    D.overwrite(users, f"{DB}.silver_users")

    silver, quarantine = T.build_silver(spark.table(f"{DB}.bronze_transactions"), categories)
    inserted = D.merge_into_silver(spark, silver, f"{DB}.silver_transactions")
    D.overwrite(quarantine, f"{DB}.silver_transactions_quarantine")
    print(f"silver: +{inserted} rows, {quarantine.count()} quarantined")

    # Gold
    silver = spark.table(f"{DB}.silver_transactions")
    D.overwrite(T.monthly_category_spend(silver), f"{DB}.gold_monthly_category_spend")
    D.overwrite(T.monthly_cashflow(silver, spark.table(f"{DB}.silver_users")), f"{DB}.gold_monthly_cashflow")

    counts = {t: spark.table(f"{DB}.{t}").count() for t in [
        "bronze_transactions", "silver_transactions", "silver_transactions_quarantine",
        "gold_monthly_category_spend", "gold_monthly_cashflow"]}
    print("COUNTS", counts)

    print("\nsilver_transactions history:")
    D.table_versions(spark, f"{DB}.silver_transactions").show(truncate=False)

    q = spark.table(f"{DB}.silver_transactions_quarantine")
    if q.count():
        print("Quarantined:")
        q.select("_dq_error", "transaction_id", "_source_file").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
