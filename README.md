# Data Pipeline — End-to-End PySpark Lakehouse

An end-to-end data engineering project: incremental ingestion from a public API
into a Delta Lake medallion architecture (Bronze → Silver → Gold), processed with
PySpark, orchestrated by Dagster, validated for data quality, and served via DuckDB.

## Tech stack
- **Compute:** Apache Spark (PySpark)
- **Storage:** Delta Lake on MinIO (S3-compatible)
- **Orchestration:** Dagster
- **Data quality:** Great Expectations
- **Serving:** DuckDB
- **CI/CD:** GitHub Actions

## Status
In active development — Phase 0 (foundations) complete.
Medallion layers built: Bronze ✅ · Silver ✅ · Gold ✅ (daily repo activity).

## Local setup
1. Clone the repo
2. Create a virtual environment: `python -m venv venv`
3. Activate it, then install deps: `pip install -r requirements.txt`
4. Start MinIO: `docker compose up -d`