"""DuckDB serving layer over the Gold Delta table.

Serving deliberately does NOT use Spark: the Gold table is small and read-only
at this stage, so I resolve its active parquet files from the Delta log with
deltalake, then query them directly with DuckDB (no JVM, no extensions, fully
offline). The analytics functions take a DuckDB connection with a `gold` view
registered, which keeps the SQL testable against any small in-memory table.
"""
from pathlib import Path

import duckdb
from deltalake import DeltaTable

ROOT = Path(__file__).resolve().parents[2]
GOLD_PATH = str(ROOT / "data" / "gold" / "daily_repo_activity")


def connect_gold(gold_path: str = GOLD_PATH) -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection with the Gold table registered as view `gold`.

    I ask the Delta log for the current version's active files (so overwritten
    parquet files are ignored) and let DuckDB read them, recovering the
    `event_date` partition column from the hive-style directory names.
    """
    files = DeltaTable(gold_path).file_uris()
    con = duckdb.connect()
    relation = con.read_parquet(files, hive_partitioning=True)
    con.register("gold", relation)
    return con


def top_repos(con: duckdb.DuckDBPyConnection, limit: int = 10) -> list[tuple]:
    """Most active repositories by total event count across all days."""
    return con.execute(
        """
        SELECT repo_name,
               SUM(event_count)     AS total_events,
               SUM(distinct_actors) AS total_actor_days
        FROM gold
        GROUP BY repo_name
        ORDER BY total_events DESC
        LIMIT ?
        """,
        [limit],
    ).fetchall()


def daily_totals(con: duckdb.DuckDBPyConnection) -> list[tuple]:
    """Total events and active repositories per day, most recent first."""
    return con.execute(
        """
        SELECT event_date,
               SUM(event_count)         AS total_events,
               COUNT(DISTINCT repo_name) AS active_repos
        FROM gold
        GROUP BY event_date
        ORDER BY event_date DESC
        """
    ).fetchall()


def busiest_day(con: duckdb.DuckDBPyConnection) -> tuple | None:
    """The single day with the most events overall (None if the table is empty)."""
    return con.execute(
        """
        SELECT event_date, SUM(event_count) AS total_events
        FROM gold
        GROUP BY event_date
        ORDER BY total_events DESC
        LIMIT 1
        """
    ).fetchone()


def report(gold_path: str = GOLD_PATH) -> None:
    """Print a small analytics summary of the Gold table."""
    con = connect_gold(gold_path)

    print("== Top repositories by activity ==")
    for repo, events, actor_days in top_repos(con, limit=10):
        print(f"  {repo:<40} {events:>6} events  ({actor_days} actor-days)")

    print("\n== Daily totals ==")
    for date, events, repos in daily_totals(con):
        print(f"  {date}  {events:>6} events across {repos} repos")

    day = busiest_day(con)
    if day:
        print(f"\nBusiest day: {day[0]} with {day[1]} events")

    con.close()


if __name__ == "__main__":
    report()
