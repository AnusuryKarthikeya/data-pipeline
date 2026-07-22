# Data Pipeline — End-to-End PySpark Lakehouse

An end-to-end data engineering project: incremental ingestion of GitHub public
events into a Delta Lake medallion architecture (Bronze → Silver → Gold),
processed with PySpark, gated by data-quality checks, and served for analytics
via DuckDB.

## Tech stack
What the code actually uses today:

- **Compute:** Apache Spark (PySpark) + Delta Lake
- **Storage:** local Delta tables on disk (`data/…`)
- **Orchestration:** `pipeline.orchestrate` (a lightweight stage runner)
- **Data quality:** `pipeline.quality` (row/null/uniqueness gates)
- **Serving:** DuckDB over the Gold Delta table
- **CI/CD:** GitHub Actions (ruff + pytest)

## Planned
My roadmap — keywords I'm working toward, not part of the stack yet:

- **Dagster** to replace my hand-rolled `orchestrate` stage runner
- **Great Expectations** to replace my lightweight `quality` checks
- **MinIO / S3** as remote object storage instead of local disk (there's a
  `docker compose` MinIO service, but Spark still writes local Delta files)

## Architecture

```
GitHub /events API
      │  ingest.py        watermark-based incremental fetch
      ▼
  data/raw/*.jsonl        raw landing zone (one file per run)
      │  bronze.py        flatten + ingestion metadata
      ▼
  data/bronze/events      Bronze Delta table
      │  silver.py        dedupe by event_id, MERGE upsert
      ▼
  data/silver/events      Silver Delta table  ──▶ quality gate
      │  gold.py          aggregate to daily repo activity
      ▼
  data/gold/…             Gold Delta table    ──▶ quality gate
      │  serve.py         DuckDB analytics
      ▼
  reports / queries
```

## Status
Medallion pipeline complete and tested end to end:
Bronze ✅ · Silver ✅ · Gold ✅ · data-quality gates ✅ · DuckDB serving ✅ ·
orchestration ✅.

## Local setup
1. Clone the repo
2. Create a virtual environment: `python -m venv venv`
3. Activate it, then install deps: `pip install -r requirements.txt`
4. (Optional) Start MinIO for S3-compatible storage: `docker compose up -d`

## Running the pipeline

Run the whole thing (fetch → bronze → silver → gold → serve), with quality gates
after Silver and Gold:

```bash
python -m pipeline.orchestrate                # full run
python -m pipeline.orchestrate --skip-ingest  # reuse existing raw data
python -m pipeline.orchestrate --no-serve     # skip the analytics summary
```

Or run any stage on its own:

```bash
python -m pipeline.ingest     # fetch new events into data/raw/
python -m pipeline.bronze     # build the Bronze Delta table
python -m pipeline.silver     # upsert into Silver
python -m pipeline.gold       # aggregate into Gold
python -m pipeline.serve      # print the DuckDB analytics report
```

## Testing

```bash
pytest          # unit tests for every layer, quality checks, and serving
ruff check .    # lint
```