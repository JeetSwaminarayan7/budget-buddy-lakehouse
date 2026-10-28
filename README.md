# Budget Buddy lakehouse (Databricks, PySpark, Delta Lake)

A medallion pipeline in PySpark for [Budget Buddy](https://github.com/JeetSwaminarayan7/budget-buddy), my personal finance app. Raw transaction exports go through bronze, silver and gold Delta tables on Databricks. The same code also runs on a laptop with open-source Spark and Delta Lake, which is how it's tested.

```
  raw CSV files              bronze                     silver                          gold
  (Unity Catalog volume)     (as it arrived)            (clean, one row each)           (for reporting)
  ─────────────────          ──────────────             ─────────────────────           ───────────────
  transactions/*.csv  ──▶  bronze_transactions  ──▶  silver_transactions       ──▶  gold_monthly_category_spend
     Auto Loader,             every column text,        typed, validated,                gold_monthly_cashflow
     new files only           + source file,            deduplicated, MERGE
                              + load time               silver_transactions_quarantine
  reference/*.csv  ────────────────────────────────▶  silver_users, silver_categories
```

## The layers

**Bronze** keeps the data exactly as it arrived: every column as text, nothing dropped, plus `_source_file` and `_ingested_at` on every row. If a cleaning rule turns out to be wrong, silver can be rebuilt from bronze without going back to the source. On Databricks, Auto Loader reads the folder and remembers which files it has already loaded, so each run only takes new files.

**Silver** is one trustworthy row per real transaction:

- both date formats parsed (`2026-03-05` and `05/03/2026`), `£` stripped, descriptions tidied
- rows that fail a rule go to `silver_transactions_quarantine` with the reason: unparseable date, bad amount, amount not positive, unknown type, or missing id. Nothing silently disappears
- each transaction gets a `transaction_key`, a hash of user, date, description, amount and type, so a re-uploaded statement produces the same keys even with new ids
- new rows are added with Delta `MERGE` on that key. A duplicate arriving in a later file is skipped, and rerunning the pipeline changes nothing

**Gold** has two reporting tables, rebuilt from silver each run: spending per user, month and category, and monthly income, spending and net against each user's savings goal.

The sample data is the same as my [dbt project](https://github.com/JeetSwaminarayan7/budget-buddy-analytics), and both land on the same numbers: 7,388 raw rows become 7,294 clean ones after removing duplicates. October's file adds 837 rows, five of them broken on purpose, one for each quarantine rule.

## Where the code is

```
├── src/budget_buddy_lakehouse/
│   ├── transforms.py     # the PySpark logic for each layer: DataFrame in, DataFrame out
│   └── delta_ops.py      # writing to Delta: append, MERGE, overwrite, history
├── notebooks/            # Databricks notebooks that call the code above
│   ├── 00_setup.py
│   ├── 01_bronze.py
│   ├── 02_silver.py
│   ├── 03_gold.py
│   └── 04_delta_features.py
├── tests/                # pytest, on local Spark
├── run_local.py          # the whole pipeline without Databricks
├── databricks.yml        # optional: deploy it as a scheduled Databricks job
└── data/
    ├── raw/              # January to September, plus users and categories
    └── incoming/         # October, landed later to show incremental loading
```

The logic is kept out of the notebooks on purpose. Notebooks are hard to test and hard to review in a pull request; plain Python modules are neither.

## Running it on Databricks Free Edition

Community Edition was retired at the start of 2026. Free Edition replaced it, also free, with serverless compute and Unity Catalog.

1. Sign up for Free Edition at [login.databricks.com](https://login.databricks.com/?intent=CE_SIGN_UP).
2. In your workspace, go to **Workspace → Create → Git folder**, paste this repo's GitHub URL, and create it. It's a public repo, so no credentials are needed.
3. Open `notebooks/00_setup.py` and click **Run all**. It creates the `workspace.budget_buddy` schema and a `raw` volume, and copies the sample files into it. Compute starts by itself.
4. Run `01_bronze`, `02_silver` and `03_gold` in order. Each one prints what it did.
5. Look at the tables under **Catalog → workspace → budget_buddy**. Click a table to see its columns, sample data and history.

Then see incremental loading working:

6. Run the last cell in `01_bronze`. It moves October's file into the folder Auto Loader watches.
7. Run `01_bronze` again. It loads only the new file, 837 rows. Run it a third time and it loads nothing.
8. Run `02_silver` and `03_gold` again. Silver inserts only the new transactions, and the quarantine table now shows the five broken October rows with their reasons.
9. Run `04_delta_features` to see table history, time travel, undoing a bad delete with `RESTORE`, schema enforcement rejecting an unexpected column, and `OPTIMIZE ... ZORDER BY`.

### Running it as a job (optional)

`databricks.yml` is a Databricks Asset Bundle that deploys bronze → silver → gold as one job, scheduled for 06:30 daily (paused until you switch it on). With the [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install) installed:

```bash
databricks auth login --host https://<your-workspace>.cloud.databricks.com
databricks bundle deploy
databricks bundle run medallion
```

The job then shows under **Jobs & Pipelines**, with each task and its run history.

## Running it on your laptop

You need Python 3.10 or newer and Java 17 or newer (`java -version` to check; [Temurin](https://adoptium.net/) if you need it).

```bash
python -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt

python run_local.py                 # Jan to Sep
python run_local.py --land-october  # land October, run again
python run_local.py                 # nothing new, nothing changes
```

The first run downloads the Delta Lake jars, so it needs internet. Each run prints the row counts per table, the history of `silver_transactions` and anything quarantined. Delete `spark-warehouse/` and `metastore_db/`, and move October's file back to `data/incoming/`, to start again.

## Tests

```bash
pytest -v tests
```

- `tests/test_transforms.py`: both date formats, each quarantine rule, duplicates keeping the first copy, and the two gold tables agreeing with each other.
- `tests/test_delta.py`: `MERGE` inserting only new keys and keeping the first copy of a duplicate, rerunning a batch changing nothing, bronze skipping files it already has, time travel, and schema enforcement. These need the Delta jars and are skipped if they can't be downloaded.

CI (`.github/workflows/ci.yml`) runs all the tests, then runs the pipeline twice and checks the second run added exactly October's 837 rows to bronze and quarantined exactly five, and that a third run adds nothing.

## Things I'd do next

- Replace the batch `MERGE` from the whole of bronze with a streaming read of bronze and `foreachBatch`, so silver only processes new rows. Fine at 8,000 rows, slow at 800 million.
- Move the data-quality rules into Lakeflow Declarative Pipelines (formerly Delta Live Tables) expectations.
- Turn on liquid clustering for the gold tables instead of `ZORDER`.
