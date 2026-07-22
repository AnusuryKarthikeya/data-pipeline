"""End-to-end pipeline runner: ingest -> bronze -> silver -> gold -> serve.

Chains the medallion stages in order and gates on data-quality checks after
Silver and Gold. This is a lightweight orchestrator (a stand-in for Dagster in
this learning project): each stage is an idempotent function already defined in
its own module, so running the pipeline is just calling them in sequence.

Usage:
    python -m pipeline.orchestrate                # full run
    python -m pipeline.orchestrate --skip-ingest  # skip the network fetch
    python -m pipeline.orchestrate --no-serve     # skip the DuckDB summary
"""
import argparse

from pipeline import bronze, gold, ingest, quality, serve, silver
from pipeline.spark_session import get_spark


def run_quality(layer: str) -> None:
    """Read a built Delta table back and run its data-quality checks."""
    spark = get_spark(f"quality-{layer}")
    try:
        if layer == "silver":
            df = spark.read.format("delta").load(silver.SILVER_PATH)
            quality.run_checks(quality.validate_silver(df), layer="silver")
        elif layer == "gold":
            df = spark.read.format("delta").load(gold.GOLD_PATH)
            quality.run_checks(quality.validate_gold(df), layer="gold")
    finally:
        spark.stop()


def run_pipeline(*, skip_ingest: bool = False, serve_report: bool = True) -> None:
    """Run the pipeline end to end, stopping if a quality gate fails."""
    if skip_ingest:
        print(">> Skipping ingestion (using existing raw data)")
    else:
        print(">> Stage: ingest")
        ingest.ingest()

    print(">> Stage: bronze")
    bronze.build_bronze()

    print(">> Stage: silver")
    silver.build_silver()
    run_quality("silver")

    print(">> Stage: gold")
    gold.build_gold()
    run_quality("gold")

    if serve_report:
        print(">> Stage: serve")
        serve.report()

    print(">> Pipeline complete.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the medallion data pipeline.")
    parser.add_argument(
        "--skip-ingest",
        action="store_true",
        help="Skip the GitHub API fetch and reuse existing raw files.",
    )
    parser.add_argument(
        "--no-serve",
        action="store_true",
        help="Skip the DuckDB analytics summary at the end.",
    )
    args = parser.parse_args()
    run_pipeline(skip_ingest=args.skip_ingest, serve_report=not args.no_serve)


if __name__ == "__main__":
    main()
