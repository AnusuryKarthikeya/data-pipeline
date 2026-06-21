from datetime import datetime

from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from pipeline.silver import transform_to_silver

_BRONZE_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("actor_login", StringType()),
    StructField("repo_name", StringType()),
    StructField("created_at", TimestampType()),
    StructField("_ingested_at", TimestampType()),
    StructField("_source_file", StringType()),
])


def _make_bronze(spark, rows):
    return spark.createDataFrame(rows, _BRONZE_SCHEMA)


def test_null_event_id_dropped(spark):
    t = datetime(2024, 1, 1, 12, 0, 0)
    bronze = _make_bronze(spark, [
        ("100", "PushEvent", "alice", "alice/repo", t, t, "f.jsonl"),
        (None,  "ForkEvent", "bob",   "bob/repo",   t, t, "f.jsonl"),
    ])
    result = transform_to_silver(bronze)
    assert result.count() == 1
    assert result.collect()[0]["event_id"] == "100"


def test_deduplication_keeps_latest(spark):
    early = datetime(2024, 1, 1, 10, 0, 0)
    late  = datetime(2024, 1, 1, 12, 0, 0)
    bronze = _make_bronze(spark, [
        ("100", "PushEvent", "alice", "alice/repo", late,  late,  "f1.jsonl"),
        ("100", "PushEvent", "alice", "alice/repo", early, early, "f2.jsonl"),
    ])
    result = transform_to_silver(bronze)
    assert result.count() == 1
    assert result.collect()[0]["created_at"] == late


def test_output_columns(spark):
    t = datetime(2024, 1, 1, 12, 0, 0)
    bronze = _make_bronze(spark, [
        ("100", "PushEvent", "alice", "alice/repo", t, t, "f.jsonl"),
    ])
    result = transform_to_silver(bronze)
    cols = set(result.columns)
    assert cols == {"event_id", "event_type", "actor_login", "repo_name", "created_at", "_processed_at"}
    assert "_ingested_at" not in cols
    assert "_source_file" not in cols
