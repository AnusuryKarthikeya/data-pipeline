from datetime import datetime

from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from pipeline.gold import transform_to_gold

# Silver output schema: what Gold consumes.
_SILVER_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("actor_login", StringType()),
    StructField("repo_name", StringType()),
    StructField("created_at", TimestampType()),
    StructField("_processed_at", TimestampType()),
])


def _make_silver(spark, rows):
    return spark.createDataFrame(rows, _SILVER_SCHEMA)


def test_grain_is_repo_per_day(spark):
    # Two events on the same repo and same day collapse into one row.
    t1 = datetime(2024, 1, 1, 10, 0, 0)
    t2 = datetime(2024, 1, 1, 15, 0, 0)
    silver = _make_silver(spark, [
        ("1", "PushEvent",  "alice", "alice/repo", t1, t1),
        ("2", "WatchEvent", "bob",   "alice/repo", t2, t2),
    ])
    result = transform_to_gold(silver)
    assert result.count() == 1
    row = result.collect()[0]
    assert row["event_count"] == 2
    assert row["first_event_at"] == t1
    assert row["last_event_at"] == t2


def test_distinct_actors_deduped(spark):
    t = datetime(2024, 1, 1, 12, 0, 0)
    silver = _make_silver(spark, [
        ("1", "PushEvent", "alice", "alice/repo", t, t),
        ("2", "PushEvent", "alice", "alice/repo", t, t),  # same actor
        ("3", "PushEvent", "bob",   "alice/repo", t, t),
    ])
    result = transform_to_gold(silver)
    row = result.collect()[0]
    assert row["event_count"] == 3
    assert row["distinct_actors"] == 2


def test_different_days_separate_rows(spark):
    day1 = datetime(2024, 1, 1, 9, 0, 0)
    day2 = datetime(2024, 1, 2, 9, 0, 0)
    silver = _make_silver(spark, [
        ("1", "PushEvent", "alice", "alice/repo", day1, day1),
        ("2", "PushEvent", "alice", "alice/repo", day2, day2),
    ])
    result = transform_to_gold(silver)
    assert result.count() == 2
    counts = {r["event_date"]: r["event_count"] for r in result.collect()}
    assert counts[day1.date()] == 1
    assert counts[day2.date()] == 1


def test_output_columns(spark):
    t = datetime(2024, 1, 1, 12, 0, 0)
    silver = _make_silver(spark, [
        ("1", "PushEvent", "alice", "alice/repo", t, t),
    ])
    result = transform_to_gold(silver)
    assert set(result.columns) == {
        "repo_name", "event_date", "event_count", "distinct_actors",
        "first_event_at", "last_event_at", "_built_at",
    }
