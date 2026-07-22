"""Lightweight data-quality checks for the medallion layers.

Each check is a pure function over a DataFrame that returns a CheckResult, so
the logic is testable without any I/O. The layer-level helpers (validate_silver,
validate_gold) bundle the checks that matter for that table, and run_checks
raises when any check fails -- suitable for gating a pipeline run.
"""
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    detail: str


class DataQualityError(Exception):
    """Raised when one or more data-quality checks fail."""


def check_non_empty(df: DataFrame, name: str = "non_empty") -> CheckResult:
    """Fail if the table has no rows."""
    count = df.count()
    return CheckResult(name, count > 0, f"row_count={count}")


def check_no_nulls(df: DataFrame, columns: list[str]) -> CheckResult:
    """Fail if any of the given columns contain a null value."""
    conditions = [F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in columns]
    counts = df.select(conditions).collect()[0].asDict()
    offenders = {c: n for c, n in counts.items() if n > 0}
    passed = not offenders
    detail = "no nulls" if passed else f"nulls found: {offenders}"
    return CheckResult(f"no_nulls({','.join(columns)})", passed, detail)


def check_unique(df: DataFrame, keys: list[str]) -> CheckResult:
    """Fail if the given key columns are not unique across the table."""
    total = df.count()
    distinct = df.select(*keys).distinct().count()
    passed = total == distinct
    dupes = total - distinct
    detail = "unique" if passed else f"{dupes} duplicate row(s) on {keys}"
    return CheckResult(f"unique({','.join(keys)})", passed, detail)


def validate_silver(df: DataFrame) -> list[CheckResult]:
    """Checks the Silver table must satisfy: populated, keyed, deduplicated."""
    return [
        check_non_empty(df, "silver_non_empty"),
        check_no_nulls(df, ["event_id", "event_type", "repo_name", "created_at"]),
        check_unique(df, ["event_id"]),
    ]


def validate_gold(df: DataFrame) -> list[CheckResult]:
    """Checks the Gold table must satisfy: populated, one row per repo per day."""
    return [
        check_non_empty(df, "gold_non_empty"),
        check_no_nulls(df, ["repo_name", "event_date", "event_count"]),
        check_unique(df, ["repo_name", "event_date"]),
    ]


def run_checks(results: list[CheckResult], *, layer: str) -> list[CheckResult]:
    """Print each result and raise DataQualityError if any check failed."""
    print(f"Data-quality checks for {layer}:")
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        print(f"  [{status}] {r.name}: {r.detail}")

    failed = [r for r in results if not r.passed]
    if failed:
        names = ", ".join(r.name for r in failed)
        raise DataQualityError(f"{layer}: {len(failed)} check(s) failed -> {names}")
    return results
