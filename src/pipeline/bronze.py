"""Build the Bronze Delta table incrementally from raw GitHub event files."""
import json
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable

from pipeline.spark_session import get_spark

ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = ROOT / "data" / "raw"
BRONZE_PATH = str(ROOT / "data" / "bronze" / "events")
# I remember which raw files I've already loaded so I never re-read the whole history.
STATE_FILE = ROOT / "data" / "state" / "bronze_watermark.json"


def transform_to_bronze(raw: DataFrame) -> DataFrame:
    """Flatten raw events and add ingestion metadata. Pure logic, no I/O -> testable."""
    return (
        raw.select(
            F.col("id").cast("string").alias("event_id"),
            F.col("type").alias("event_type"),
            F.col("actor.login").alias("actor_login"),
            F.col("repo.name").alias("repo_name"),
            F.to_timestamp("created_at").alias("created_at"),
        )
        .withColumn("_ingested_at", F.current_timestamp())
        # I partition Bronze by the date I ingested, so runs land in their own folder.
        .withColumn("_ingest_date", F.current_date())
        .withColumn("_source_file", F.input_file_name())
    )


def load_watermark() -> str:
    """Return the name of the newest raw file I've already loaded ("" on first run)."""
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())["last_file"]
    return ""


def save_watermark(last_file: str) -> None:
    """Remember the newest raw file I just loaded so the next run skips it."""
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps({"last_file": last_file}))


def new_raw_files(watermark: str) -> list[Path]:
    """Return raw files newer than my watermark, oldest first.

    The raw files are named events_<UTC timestamp>.jsonl, so sorting by name
    sorts them by time. A file is "new" when its name is greater than the last
    one I loaded, which lets me read only the fresh files instead of all of them.
    """
    if not RAW_DIR.exists():
        return []
    return sorted(p for p in RAW_DIR.glob("*.jsonl") if p.name > watermark)


def build_bronze() -> None:
    spark = get_spark("bronze")

    # I only read the raw files I haven't loaded yet, so runtime tracks new data,
    # not the total history sitting in data/raw.
    watermark = load_watermark()
    files = new_raw_files(watermark)
    if not files:
        print("No new raw files since last Bronze build. Nothing to do.")
        spark.stop()
        return

    raw = spark.read.json([str(p) for p in files])
    bronze = transform_to_bronze(raw)

    if DeltaTable.isDeltaTable(spark, BRONZE_PATH):
        # Table exists -> insert only event_ids I haven't seen. Merging on the key
        # keeps this idempotent: re-running the same files never duplicates rows.
        target = DeltaTable.forPath(spark, BRONZE_PATH)
        (
            target.alias("t")
            .merge(bronze.alias("s"), "t.event_id = s.event_id")
            .whenNotMatchedInsertAll()
            .execute()
        )
        print("Merged new events into existing Bronze table.")
    else:
        # First run -> create the table, partitioned by ingest date.
        bronze.write.format("delta").partitionBy("_ingest_date").save(BRONZE_PATH)
        print("Created new Bronze table.")

    # I only advance the watermark after a successful write.
    save_watermark(files[-1].name)

    result = spark.read.format("delta").load(BRONZE_PATH)
    print(f"Bronze table written to {BRONZE_PATH}")
    print(f"Loaded {len(files)} new raw file(s); total row count: {result.count()}")
    result.show(5, truncate=False)
    spark.stop()


if __name__ == "__main__":
    build_bronze()
