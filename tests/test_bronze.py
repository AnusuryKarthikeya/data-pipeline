from pipeline.bronze import transform_to_bronze


def test_transform_to_bronze(spark):
    # Build a tiny raw DataFrame the same way production reads it: from JSON lines.
    raw_json = [
        '{"id": "100", "type": "PushEvent", "actor": {"login": "alice"}, "repo": {"name": "alice/repo"}, "created_at": "2024-01-01T12:00:00Z"}',
        '{"id": "101", "type": "WatchEvent", "actor": {"login": "bob"}, "repo": {"name": "bob/repo"}, "created_at": "2024-01-01T12:05:00Z"}',
    ]
    raw = spark.read.json(spark.sparkContext.parallelize(raw_json))

    result = transform_to_bronze(raw)

    # The expected columns exist
    expected_cols = {"event_id", "event_type", "actor_login", "repo_name",
                     "created_at", "_ingested_at", "_source_file"}
    assert expected_cols.issubset(set(result.columns))

    # The flattening pulled nested fields out correctly
    rows = {r["event_id"]: r for r in result.collect()}
    assert rows["100"]["actor_login"] == "alice"
    assert rows["100"]["repo_name"] == "alice/repo"
    assert rows["101"]["event_type"] == "WatchEvent"

    # created_at was parsed from a string into a real timestamp type
    assert dict(result.dtypes)["created_at"] == "timestamp"