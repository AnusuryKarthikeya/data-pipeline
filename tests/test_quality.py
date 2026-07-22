from datetime import datetime

import pytest
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from pipeline.quality import (
    DataQualityError,
    check_no_nulls,
    check_non_empty,
    check_unique,
    run_checks,
    validate_silver,
)

_SILVER_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("actor_login", StringType()),
    StructField("repo_name", StringType()),
    StructField("created_at", TimestampType()),
    StructField("_processed_at", TimestampType()),
])

_T = datetime(2024, 1, 1, 12, 0, 0)


def _silver(spark, rows):
    return spark.createDataFrame(rows, _SILVER_SCHEMA)


def test_check_non_empty(spark):
    empty = _silver(spark, [])
    populated = _silver(spark, [("1", "PushEvent", "alice", "alice/repo", _T, _T)])
    assert check_non_empty(empty).passed is False
    assert check_non_empty(populated).passed is True


def test_check_no_nulls(spark):
    good = _silver(spark, [("1", "PushEvent", "alice", "alice/repo", _T, _T)])
    bad = _silver(spark, [("1", "PushEvent", "alice", None, _T, _T)])
    assert check_no_nulls(good, ["repo_name"]).passed is True
    result = check_no_nulls(bad, ["repo_name"])
    assert result.passed is False
    assert "repo_name" in result.detail


def test_check_unique(spark):
    dupes = _silver(spark, [
        ("1", "PushEvent", "alice", "alice/repo", _T, _T),
        ("1", "PushEvent", "alice", "alice/repo", _T, _T),
    ])
    unique = _silver(spark, [
        ("1", "PushEvent", "alice", "alice/repo", _T, _T),
        ("2", "PushEvent", "bob", "bob/repo", _T, _T),
    ])
    assert check_unique(unique, ["event_id"]).passed is True
    assert check_unique(dupes, ["event_id"]).passed is False


def test_validate_silver_passes_clean_data(spark):
    clean = _silver(spark, [
        ("1", "PushEvent", "alice", "alice/repo", _T, _T),
        ("2", "WatchEvent", "bob", "bob/repo", _T, _T),
    ])
    results = validate_silver(clean)
    assert all(r.passed for r in results)
    # run_checks returns the results and does not raise when everything passes.
    assert run_checks(results, layer="silver") == results


def test_run_checks_raises_on_failure(spark):
    dirty = _silver(spark, [
        ("1", "PushEvent", "alice", None, _T, _T),  # null repo_name
    ])
    with pytest.raises(DataQualityError):
        run_checks(validate_silver(dirty), layer="silver")
