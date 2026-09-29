"""Spark batch jobs of the daily DAG (run by Airflow with spark-submit):

    spark-submit --master local[1] --driver-memory 512m fleet/batch/jobs.py \
        load      --business-date 2026-01-01 [--airflow-run-id ...]
    spark-submit ... fleet/batch/jobs.py reconcile --business-date 2026-01-01

load       landing CSV -> clean/cast/trim -> vehicle_expenses            (pipeline expenses_load)
reconcile  stream_events + vehicle_expenses -> daily_vehicle_profitability (pipeline reconciliation)

Both jobs:
- record a pipeline_runs row (running -> success / failed, with row counts),
- write their result through a per-run staging table and replace the business date in
  ONE PostgreSQL transaction (fleet.batch.runs.replace_date_from_staging), so re-runs
  and Airflow retries never duplicate rows,
- exit with code 1 on any error, which fails the Airflow task.

The transformations themselves live in fleet/batch/transforms.py (unit-tested).
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from fleet.batch import runs
from fleet.batch.transforms import (
    EXPENSE_CSV_SCHEMA,
    PROFITABILITY_COLUMNS,
    VEHICLE_EXPENSE_COLUMNS,
    clean_expenses,
    reconcile,
    vehicle_day_aggregates,
)
from fleet.batch.validation import ExpenseValidationError, validate_expense_file
from fleet.common.config import Settings, load_settings
from fleet.common.contracts import expense_file_path
from fleet.common.logs import get_logger

log = get_logger("spark-batch")

JDBC_FETCH_SIZE = 5000


def build_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.session.timeZone", "UTC"
        )  # timestamps in UTC; dates come from stream_events
        .config("spark.sql.shuffle.partitions", "1")  # ~20 vehicles per day: tiny shuffles
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _jdbc_options(settings: Settings) -> dict[str, str]:
    return {
        "url": settings.postgres_jdbc_url,
        "user": settings.postgres_user,
        "password": settings.postgres_password,
        "driver": "org.postgresql.Driver",
    }


def read_table(spark: SparkSession, settings: Settings, table: str) -> DataFrame:
    """Lazy JDBC read. Filters / column selections applied afterwards are pushed down
    into the SQL Spark sends (visible as PushedFilters in df.explain())."""
    return (
        spark.read.format("jdbc")
        .options(**_jdbc_options(settings))
        .option("dbtable", table)
        .option("fetchsize", JDBC_FETCH_SIZE)
        .load()
    )


def write_staging(df: DataFrame, settings: Settings, staging: str) -> None:
    """Bulk-insert into a new staging table (Spark creates it; errors if it exists)."""
    df.write.format("jdbc").options(**_jdbc_options(settings)).option("dbtable", staging).mode(
        "errorifexists"
    ).save()


def _replace_date(
    df: DataFrame, settings: Settings, target: str, columns: tuple[str, ...], stats: runs.RunStats
) -> int:
    staging = runs.staging_table(target, stats.run_id)
    runs.drop_table(settings, staging)  # left over from a crashed attempt with this run_id?
    try:
        write_staging(df, settings, staging)
        return runs.replace_date_from_staging(
            settings, target, staging, columns, stats.business_date, stats.run_id
        )
    finally:
        runs.drop_table(settings, staging)  # no-op after a successful swap


# ---------------------------------------------------------------------------
# (a) load_expenses
# ---------------------------------------------------------------------------
def run_load_expenses(
    spark: SparkSession,
    settings: Settings,
    business_date: str,
    airflow_run_id: str | None = None,
) -> runs.RunStats:
    day = date.fromisoformat(business_date)
    path = expense_file_path(settings.landing_dir, business_date)
    with runs.tracked_run(settings, runs.EXPENSES_LOAD, business_date, airflow_run_id) as stats:
        # Defence in depth: the job may be run by hand, without the Airflow validation
        # task in front of it. An invalid file is rejected before anything is written.
        result = validate_expense_file(path, business_date)
        if not result.is_valid:
            runs.record_validation(settings, result, runs.EXPENSES_LOAD)
            stats.rows_read, stats.rows_rejected, stats.rows_written = (
                result.records_total,
                result.records_rejected,
                0,
            )
            raise ExpenseValidationError(result)

        raw = spark.read.csv(path, header=True, schema=EXPENSE_CSV_SCHEMA, mode="PERMISSIVE")
        clean, rejected = clean_expenses(raw, day, path)
        clean = clean.cache()
        total, valid = raw.count(), clean.count()
        stats.rows_read, stats.rows_rejected = total, total - valid
        runs.record_quality(
            settings,
            runs.EXPENSES_LOAD,
            path,
            business_date,
            total,
            valid,
            total - valid,
            {"rejected_by_spark_cleaning": total - valid} if total != valid else {},
        )
        if total != valid:
            examples = [r.asDict() for r in rejected.limit(5).collect()]
            raise ValueError(
                f"{total - valid} of {total} expense rows failed Spark cleaning; nothing"
                f" loaded. Examples: {examples}"
            )

        stats.rows_written = _replace_date(
            clean, settings, "vehicle_expenses", VEHICLE_EXPENSE_COLUMNS, stats
        )
        clean.unpersist()
    return stats


# ---------------------------------------------------------------------------
# (b) reconcile
# ---------------------------------------------------------------------------
def run_reconcile(
    spark: SparkSession,
    settings: Settings,
    business_date: str,
    airflow_run_id: str | None = None,
) -> runs.RunStats:
    day = date.fromisoformat(business_date)
    with runs.tracked_run(settings, runs.RECONCILIATION, business_date, airflow_run_id) as stats:
        on_day = F.col("business_date") == F.lit(day)  # pushed down to PostgreSQL
        events = (
            read_table(spark, settings, "stream_events")
            .where(on_day)
            .select("vehicle_id", "status", "speed", "fare", "event_timestamp")
        )
        expenses = read_table(spark, settings, "vehicle_expenses").where(on_day).cache()
        expense_rows = expenses.count()
        if expense_rows == 0:
            raise RuntimeError(
                f"no vehicle_expenses rows for {business_date}: run the load job first"
            )

        aggregates = vehicle_day_aggregates(events, settings.stream_distance_max_gap_minutes)
        aggregates = aggregates.cache()
        event_rows = aggregates.agg(F.sum("event_count")).first()[0] or 0
        if event_rows == 0:
            log.warning(
                "no stream events for this business date; every vehicle gets 0 earnings",
                extra={"fields": {"business_date": business_date}},
            )

        result, missing = reconcile(
            aggregates, expenses, day, settings.profitable_min_profit, settings.watch_min_profit
        )
        result = result.cache()
        missing_rows = missing.collect()
        reported = result.count()
        if missing_rows:
            log.warning(
                "vehicles with stream events but no expense row are left out of the report",
                extra={
                    "fields": {
                        "business_date": business_date,
                        "vehicles": [r.vehicle_id for r in missing_rows],
                        "events": sum(r.event_count for r in missing_rows),
                        "earnings": str(sum(r.earnings for r in missing_rows)),
                    }
                },
            )
        runs.record_quality(
            settings,
            runs.RECONCILIATION,
            f"reconciliation:{business_date}",
            business_date,
            reported + len(missing_rows),
            reported,
            len(missing_rows),
            {"missing_expense_row": len(missing_rows)} if missing_rows else {},
        )

        stats.rows_read = int(event_rows) + expense_rows
        stats.rows_rejected = len(missing_rows)
        stats.rows_written = _replace_date(
            result, settings, "daily_vehicle_profitability", PROFITABILITY_COLUMNS, stats
        )
        mix = Counter(
            r.profitability_status for r in result.select("profitability_status").collect()
        )
        stats.extra = {
            "events": int(event_rows),
            "expense_rows": expense_rows,
            "status_mix": dict(mix),
        }
        for df in (result, aggregates, expenses):
            df.unpersist()
    return stats


JOBS = {"load": run_load_expenses, "reconcile": run_reconcile}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fleet Spark batch jobs")
    parser.add_argument("job", choices=sorted(JOBS))
    parser.add_argument("--business-date", required=True, type=date.fromisoformat)
    parser.add_argument("--airflow-run-id", default=None)
    args = parser.parse_args(argv)

    settings = load_settings()
    spark = build_spark(f"fleet-batch-{args.job}")
    try:
        JOBS[args.job](spark, settings, args.business_date.isoformat(), args.airflow_run_id)
    except Exception:
        log.exception(
            "batch job failed",
            extra={"fields": {"job": args.job, "business_date": args.business_date.isoformat()}},
        )
        return 1
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
