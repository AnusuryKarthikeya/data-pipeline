from datetime import date

import duckdb
import pytest

from pipeline.serve import busiest_day, daily_totals, top_repos


@pytest.fixture
def gold_con():
    """A DuckDB connection with a small synthetic `gold` view.

    Mirrors the Gold schema so the analytics SQL is exercised without Spark or a
    real Delta table.
    """
    con = duckdb.connect()
    con.execute(
        """
        CREATE TABLE gold (
            repo_name       VARCHAR,
            event_date      DATE,
            event_count     BIGINT,
            distinct_actors BIGINT,
            first_event_at  TIMESTAMP,
            last_event_at   TIMESTAMP,
            _built_at       TIMESTAMP
        )
        """
    )
    con.executemany(
        "INSERT INTO gold VALUES (?, ?, ?, ?, NULL, NULL, NULL)",
        [
            ("alice/repo", date(2024, 1, 1), 10, 3),
            ("alice/repo", date(2024, 1, 2), 5, 2),
            ("bob/repo", date(2024, 1, 1), 20, 4),
            ("bob/repo", date(2024, 1, 2), 1, 1),
        ],
    )
    yield con
    con.close()


def test_top_repos_ranks_by_total_events(gold_con):
    rows = top_repos(gold_con, limit=10)
    assert rows[0][0] == "bob/repo"   # 20 + 1 = 21
    assert rows[0][1] == 21
    assert rows[1][0] == "alice/repo"  # 10 + 5 = 15
    assert rows[1][1] == 15


def test_top_repos_respects_limit(gold_con):
    assert len(top_repos(gold_con, limit=1)) == 1


def test_daily_totals_aggregate_and_order(gold_con):
    rows = daily_totals(gold_con)
    # Ordered most-recent first.
    assert [r[0] for r in rows] == [date(2024, 1, 2), date(2024, 1, 1)]
    totals = {r[0]: r[1] for r in rows}
    assert totals[date(2024, 1, 1)] == 30  # 10 + 20
    assert totals[date(2024, 1, 2)] == 6   # 5 + 1
    active = {r[0]: r[2] for r in rows}
    assert active[date(2024, 1, 1)] == 2   # two repos active


def test_busiest_day(gold_con):
    day, events = busiest_day(gold_con)
    assert day == date(2024, 1, 1)
    assert events == 30


def test_busiest_day_empty_returns_none():
    con = duckdb.connect()
    con.execute("CREATE TABLE gold (repo_name VARCHAR, event_date DATE, event_count BIGINT)")
    assert busiest_day(con) is None
    con.close()
