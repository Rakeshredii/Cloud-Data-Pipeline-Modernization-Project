
# Cloud Data Pipeline Modernization Framework — Azure Databricks + ADF

**Goal:** support a shift from manual extracts to automated analytics with a reusable, config-driven pipeline.

**What this repo shows**
- Parameterized ADF pipeline orchestrating Databricks notebooks.
- Bronze/Silver/Gold pattern with PySpark transformations.
- Practical data quality gates (row-count, duplicates, schema drift, referential checks).
- Example SQL/Python DQ scripts and a lightweight runbook.

## Quickstart
1. Upload the contents of `data/` to DBFS at `dbfs:/FileStore/data/`.
2. Import and run notebooks in order: `ingestion.py` → `cleaning.py` → `transformation.py`.
3. (Optional) Import `pipelines/adf_pipeline.json` into Azure Data Factory.
4. Run `dq_checks/dq_checks.py` locally to simulate alerts.

## Tech
Azure Data Factory · Azure Databricks (PySpark/SQL) · Python · SQL

## Notes
All data are synthetic; adapt thresholds and schemas for real environments.
