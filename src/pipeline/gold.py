"""Build the Gold Delta table: daily activity aggregated per repository."""
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipeline.spark_session import get_spark

ROOT = Path(__file__).resolve().parents[2]
SILVER_PATH = str(ROOT / "data" / "silver" / "events")
GOLD_PATH = str(ROOT / "data" / "gold" / "daily_repo_activity")


def transform_to_gold(silver: DataFrame) -> DataFrame:
    """Aggregate Silver events to daily activity per repo. Pure logic, no I/O -> testable."""
    return (
        silver.withColumn("event_date", F.to_date("created_at"))
        .groupBy("repo_name", "event_date")
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("actor_login").alias("distinct_actors"),
            F.min("created_at").alias("first_event_at"),
            F.max("created_at").alias("last_event_at"),
        )
        .withColumn("_built_at", F.current_timestamp())
    )


def build_gold() -> None:
    spark = get_spark("gold")

    silver = spark.read.format("delta").load(SILVER_PATH)
    gold = transform_to_gold(silver)
    # I fully recompute from Silver: the aggregates are cheap to rebuild, so I
    # overwrite rather than MERGE. I partition by date to prune time-range queries.
    gold.write.format("delta").mode("overwrite").partitionBy("event_date").save(GOLD_PATH)

    result = spark.read.format("delta").load(GOLD_PATH)
    print(f"Gold table written to {GOLD_PATH}")
    print(f"Row count: {result.count()}")
    result.show(5, truncate=False)
    spark.stop()


if __name__ == "__main__":
    build_gold()
