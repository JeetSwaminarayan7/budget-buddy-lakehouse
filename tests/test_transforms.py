"""The transformation logic, on plain Spark (no Delta needed)."""

import csv
from datetime import date
from decimal import Decimal

import pytest

from budget_buddy_lakehouse import transforms as T

HEADER = ["id", "user_id", "date", "description", "amount", "type", "category_id", "source", "created_at"]


@pytest.fixture
def write_csv(tmp_path):
    def _write(rows, name="transactions.csv"):
        folder = tmp_path / "raw"
        folder.mkdir(exist_ok=True)
        with open(folder / name, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(HEADER)
            w.writerows(rows)
        return str(folder)
    return _write


def row(id, date="2026-03-05", desc="TESCO STORES", amount="10.00", type="debit", cat="1", user="1"):
    return [id, user, date, desc, amount, type, cat, "pdf", "2026-03-31 10:00:00"]


@pytest.fixture
def categories(spark):
    raw = spark.createDataFrame([("1", "1", "  Groceries "), ("2", "1", "Income")], ["id", "user_id", "name"])
    return T.clean_categories(raw)


def test_bronze_keeps_raw_text_and_adds_metadata(spark, write_csv):
    path = write_csv([row("1", amount="£12.50")])
    bronze = T.read_raw_transactions(spark, path).collect()[0]
    assert bronze["amount"] == "£12.50"             # untouched
    assert bronze["_source_file"].endswith("transactions.csv")
    assert bronze["_ingested_at"] is not None


def test_cleaning_handles_both_date_formats_and_mess(spark, write_csv, categories):
    path = write_csv([
        row("1", date="2026-03-05", desc="  tesco   stores ", amount="£12.50", type=" DEBIT "),
        row("2", date="06/03/2026", desc="Waitrose", amount="20", type="Debit", cat=""),
    ])
    silver, quarantine = T.build_silver(T.read_raw_transactions(spark, path), categories)
    rows = {r["transaction_id"]: r for r in silver.collect()}

    assert quarantine.count() == 0
    assert rows[1]["transaction_date"] == date(2026, 3, 5)
    assert rows[2]["transaction_date"] == date(2026, 3, 6)
    assert rows[1]["description"] == "TESCO STORES"
    assert rows[1]["amount_gbp"] == Decimal("12.50")
    assert rows[1]["signed_amount_gbp"] == Decimal("-12.50")
    assert rows[1]["transaction_type"] == "debit"
    assert rows[1]["category_name"] == "Groceries"
    assert rows[2]["category_name"] == "Uncategorised"
    assert rows[1]["transaction_month"] == date(2026, 3, 1)


@pytest.mark.parametrize("bad, reason", [
    (row("1", date="31/31/2026"), "unparseable date"),
    (row("1", amount="abc"), "bad amount"),
    (row("1", amount="-5"), "amount not positive"),
    (row("1", type="refund"), "unknown transaction type"),
    (row(""), "missing or bad id"),
])
def test_bad_rows_go_to_quarantine_with_a_reason(spark, write_csv, categories, bad, reason):
    path = write_csv([bad, row("99")])
    silver, quarantine = T.build_silver(T.read_raw_transactions(spark, path), categories)
    assert silver.count() == 1
    assert [r["_dq_error"] for r in quarantine.collect()] == [reason]


def test_duplicates_keep_the_first_copy(spark, write_csv, categories):
    # Same transaction twice with different ids, as when a statement is re-uploaded
    path = write_csv([row("5"), row("6"), row("7", amount="99.00")])
    silver, _ = T.build_silver(T.read_raw_transactions(spark, path), categories)
    assert sorted(r["transaction_id"] for r in silver.collect()) == [5, 7]


def test_gold_tables_agree_with_each_other(spark, write_csv, categories):
    path = write_csv([
        row("1", amount="10.00"),
        row("2", amount="15.00", desc="SAINSBURYS"),
        row("3", amount="2000.00", type="credit", cat="2", desc="SALARY"),
    ])
    silver, _ = T.build_silver(T.read_raw_transactions(spark, path), categories)
    users = T.clean_users(spark.createDataFrame(
        [("1", "user01", "1000", "500")], ["id", "username", "monthly_budget", "savings_goal"]))

    spend = T.monthly_category_spend(silver).collect()
    cash = T.monthly_cashflow(silver, users).collect()[0]

    assert [(r["category_name"], r["spend_gbp"], r["transaction_count"]) for r in spend] == \
        [("Groceries", Decimal("25.00"), 2)]
    assert cash["income_gbp"] == Decimal("2000.00")
    assert cash["spend_gbp"] == Decimal("25.00")
    assert cash["net_gbp"] == Decimal("1975.00")
    assert cash["hit_savings_goal"] is True
    assert cash["savings_rate_pct"] == Decimal("98.8")
