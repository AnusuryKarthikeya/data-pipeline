"""Build the Bronze Delta table from raw GitHub event files."""
import os
from pathlib import Path

# Windows: point Spark at the winutils/hadoop.dll we downloaded, before Spark starts.
os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")
os.environ["PATH"] = os.environ["PATH"] + os.pathsep + r"C:\hadoop\bin"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip

ROOT = Path(__file__).resolve().parents[2]
RAW_GLOB = str(ROOT / "data" / "raw" / "*.jsonl")
BRONZE_PATH = str(ROOT / "data" / "bronze" / "events")


def build_spark() -> SparkSession:
    """Create a SparkSession configured to use Delta Lake."""
    builder = (
        SparkSession.builder.appName("bronze")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    # This helper downloads the matching Delta jars on first run (needs internet).
    return configure_spark_with_delta_pip(builder).getOrCreate()


def build_bronze() -> None:
    spark = build_spark()

    # Spark reads newline-delimited JSON natively; the glob picks up every run's file.
    raw = spark.read.json(RAW_GLOB)

    # Flatten the few fields we care about and stamp ingestion metadata.
    bronze = (
        raw.select(
            F.col("id").cast("string").alias("event_id"),
            F.col("type").alias("event_type"),
            F.col("actor.login").alias("actor_login"),
            F.col("repo.name").alias("repo_name"),
            F.to_timestamp("created_at").alias("created_at"),
        )
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )

    # Rebuild Bronze from the raw zone each run (idempotent: same raw -> same Bronze).
    bronze.write.format("delta").mode("overwrite").save(BRONZE_PATH)

    result = spark.read.format("delta").load(BRONZE_PATH)
    print(f"Bronze table written to {BRONZE_PATH}")
    print(f"Row count: {result.count()}")
    result.show(5, truncate=False)

    spark.stop()


if __name__ == "__main__":
    build_bronze()