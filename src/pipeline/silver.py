"""Build the Silver Delta table by upserting cleaned Bronze data via MERGE."""
from pathlib import Path

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from delta.tables import DeltaTable

from pipeline.spark_session import get_spark

ROOT = Path(__file__).resolve().parents[2]
BRONZE_PATH = str(ROOT / "data" / "bronze" / "events")
SILVER_PATH = str(ROOT / "data" / "silver" / "events")


def transform_to_silver(bronze: DataFrame) -> DataFrame:
    """Clean and deduplicate Bronze events. Pure logic, no I/O -> testable."""
    # Drop rows with no key, then keep ONE row per event_id (the latest by created_at).
    deduped = (
        bronze.where(F.col("event_id").isNotNull())
        .withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("event_id").orderBy(F.col("created_at").desc())
            ),
        )
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )
    return deduped.select(
        "event_id", "event_type", "actor_login", "repo_name", "created_at"
    ).withColumn("_processed_at", F.current_timestamp())


def build_silver() -> None:
    spark = get_spark("silver")

    bronze = spark.read.format("delta").load(BRONZE_PATH)
    silver = transform_to_silver(bronze)

    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        # Table exists -> upsert: update matching rows, insert new ones.
        target = DeltaTable.forPath(spark, SILVER_PATH)
        (
            target.alias("t")
            .merge(silver.alias("s"), "t.event_id = s.event_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print("Merged into existing Silver table (updates + inserts).")
    else:
        # First run -> the table doesn't exist yet, so create it.
        silver.write.format("delta").save(SILVER_PATH)
        print("Created new Silver table.")

    result = spark.read.format("delta").load(SILVER_PATH)
    print(f"Silver row count: {result.count()}")
    result.show(5, truncate=False)
    spark.stop()


if __name__ == "__main__":
    build_silver()