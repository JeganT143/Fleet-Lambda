"""Batch bookkeeping in PostgreSQL: pipeline_runs, data_quality_stats, and the
"replace one business date" transaction used by both Spark batch jobs.

Pipeline names (pipeline_runs.pipeline_name / data_quality_stats.pipeline_name):

    expenses_validation   pure-Python validation of the landing file (Airflow task)
    expenses_load         Spark: CSV -> vehicle_expenses
    reconciliation        Spark: stream_events + vehicle_expenses -> daily_vehicle_profitability
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import psycopg

from fleet.batch.validation import ExpenseValidationError, ValidationResult, validate_expense_file
from fleet.common.config import Settings
from fleet.common.contracts import expense_file_path
from fleet.common.logs import get_logger

log = get_logger("batch")

VALIDATION = "expenses_validation"
EXPENSES_LOAD = "expenses_load"
RECONCILIATION = "reconciliation"

MAX_ERROR_CHARS = 2000


# ---------------------------------------------------------------------------
# pipeline_runs
# ---------------------------------------------------------------------------
@dataclass
class RunStats:
    """Counters a job fills in while it runs; written to pipeline_runs at the end."""

    run_id: int
    pipeline_name: str
    business_date: str
    rows_read: int | None = None
    rows_written: int | None = None
    rows_rejected: int | None = None
    extra: dict = field(default_factory=dict)  # only logged


def start_run(
    settings: Settings, pipeline_name: str, business_date: str, airflow_run_id: str | None
) -> int:
    # autocommit: the 'running' row is visible immediately (and survives a crash)
    with psycopg.connect(settings.postgres_dsn, autocommit=True) as conn:
        row = conn.execute(
            "INSERT INTO pipeline_runs (pipeline_name, business_date, status, airflow_run_id)"
            " VALUES (%s, %s, 'running', %s) RETURNING run_id",
            (pipeline_name, business_date, airflow_run_id),
        ).fetchone()
    return row[0]


def finish_run(settings: Settings, stats: RunStats, status: str, error: str | None = None) -> None:
    with psycopg.connect(settings.postgres_dsn, autocommit=True) as conn:
        conn.execute(
            "UPDATE pipeline_runs SET status = %s, finished_at = now(), rows_read = %s,"
            " rows_written = %s, rows_rejected = %s, error_message = %s WHERE run_id = %s",
            (
                status,
                stats.rows_read,
                stats.rows_written,
                stats.rows_rejected,
                error[:MAX_ERROR_CHARS] if error else None,
                stats.run_id,
            ),
        )


@contextmanager
def tracked_run(
    settings: Settings, pipeline_name: str, business_date: str, airflow_run_id: str | None = None
) -> Iterator[RunStats]:
    """Record a pipeline run: 'running' now, then 'success' or 'failed' (+ error message).

    The exception is re-raised after it is recorded, so the job / Airflow task fails.
    """
    run_id = start_run(settings, pipeline_name, business_date, airflow_run_id)
    stats = RunStats(run_id=run_id, pipeline_name=pipeline_name, business_date=business_date)
    fields = {"pipeline": pipeline_name, "business_date": business_date, "run_id": run_id}
    log.info("pipeline run started", extra={"fields": {**fields, "airflow_run_id": airflow_run_id}})
    try:
        yield stats
    except BaseException as exc:
        error = f"{type(exc).__name__}: {exc}"
        finish_run(settings, stats, "failed", error)
        log.error("pipeline run failed", extra={"fields": {**fields, "error": error}})
        raise
    finish_run(settings, stats, "success")
    log.info(
        "pipeline run succeeded",
        extra={
            "fields": {
                **fields,
                "rows_read": stats.rows_read,
                "rows_written": stats.rows_written,
                "rows_rejected": stats.rows_rejected,
                **stats.extra,
            }
        },
    )


# ---------------------------------------------------------------------------
# data_quality_stats
# ---------------------------------------------------------------------------
def record_quality(
    settings: Settings,
    pipeline_name: str,
    batch_ref: str,
    business_date: str,
    total: int,
    valid: int,
    rejected: int,
    rule_failures: dict,
) -> None:
    """One statistics row per (pipeline, batch_ref); a re-run replaces it (one transaction)."""
    with psycopg.connect(settings.postgres_dsn) as conn:
        conn.execute(
            "DELETE FROM data_quality_stats WHERE pipeline_name = %s AND batch_ref = %s",
            (pipeline_name, batch_ref),
        )
        conn.execute(
            "INSERT INTO data_quality_stats (pipeline_name, batch_ref, business_date,"
            " records_total, records_valid, records_rejected, rule_failures)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)",
            (
                pipeline_name,
                batch_ref,
                business_date,
                total,
                valid,
                rejected,
                json.dumps(rule_failures),
            ),
        )


def record_validation(settings: Settings, result: ValidationResult, pipeline_name: str) -> None:
    record_quality(
        settings,
        pipeline_name,
        result.file_path,
        result.business_date,
        result.records_total,
        result.records_valid,
        result.records_rejected,
        dict(result.rule_failures),
    )


def validate_and_record(
    settings: Settings,
    business_date: str,
    airflow_run_id: str | None = None,
    pipeline_name: str = VALIDATION,
) -> ValidationResult:
    """Validate the landing file of a date, record stats + a pipeline run.

    Raises ExpenseValidationError (invalid content) or FileNotFoundError; in both cases
    a 'failed' pipeline_runs row exists afterwards and nothing has been loaded.
    """
    path = expense_file_path(settings.landing_dir, business_date)
    with tracked_run(settings, pipeline_name, business_date, airflow_run_id) as stats:
        if not Path(path).is_file():
            raise FileNotFoundError(f"expense file not found: {path}")
        result = validate_expense_file(path, business_date)
        record_validation(settings, result, pipeline_name)
        stats.rows_read = result.records_total
        stats.rows_rejected = result.records_rejected
        stats.rows_written = 0
        stats.extra = {"rule_failures": dict(result.rule_failures), "file": path}
        if not result.is_valid:
            raise ExpenseValidationError(result)
    return result


# ---------------------------------------------------------------------------
# Idempotent "replace one business date" (used by both Spark jobs)
# ---------------------------------------------------------------------------
_IDENT = re.compile(r"^[a-z_][a-z0-9_]*$")


def staging_table(target: str, run_id: int) -> str:
    """Per-run staging table name, e.g. stg_vehicle_expenses_42."""
    name = f"stg_{target}_{run_id}"
    if not _IDENT.match(name):
        raise ValueError(f"unsafe table name {name!r}")
    return name


def drop_table(settings: Settings, table: str) -> None:
    if not _IDENT.match(table):
        raise ValueError(f"unsafe table name {table!r}")
    with psycopg.connect(settings.postgres_dsn, autocommit=True) as conn:
        conn.execute(f"DROP TABLE IF EXISTS {table}")


def replace_date_from_staging(
    settings: Settings,
    target: str,
    staging: str,
    columns: tuple[str, ...],
    business_date: str,
    run_id: int,
) -> int:
    """Swap one business date of `target` for the rows in `staging`, atomically.

    Why a staging table + one psycopg transaction (and not Spark writing directly)?
    Spark's JDBC writer commits per partition and can only append or overwrite a WHOLE
    table; it cannot "delete this date, then insert" atomically. So Spark bulk-writes
    the new rows into a private per-run staging table (cheap, scales with the data), and
    this single transaction then does DELETE date -> INSERT ... SELECT -> DROP staging.
    Readers (the API) see either the old day or the new day, never a mix or an empty
    day, and a crash at any point leaves the target table unchanged. Re-running a date
    therefore replaces its rows instead of duplicating them.
    """
    for ident in (target, staging, *columns):
        if not _IDENT.match(ident):
            raise ValueError(f"unsafe identifier {ident!r}")
    cols = ", ".join(columns)
    with psycopg.connect(settings.postgres_dsn) as conn:  # one transaction
        deleted = conn.execute(
            f"DELETE FROM {target} WHERE business_date = %s", (business_date,)
        ).rowcount
        inserted = conn.execute(
            f"INSERT INTO {target} ({cols}, run_id) SELECT {cols}, %s FROM {staging}"
            " WHERE business_date = %s",
            (run_id, business_date),
        ).rowcount
        conn.execute(f"DROP TABLE {staging}")
    log.info(
        "business date replaced",
        extra={
            "fields": {
                "table": target,
                "business_date": business_date,
                "rows_deleted": deleted,
                "rows_inserted": inserted,
                "run_id": run_id,
            }
        },
    )
    return inserted


def latest_successful_run(settings: Settings, pipeline_name: str, business_date: str | date):
    with psycopg.connect(settings.postgres_dsn) as conn:
        return conn.execute(
            "SELECT run_id, finished_at FROM pipeline_runs WHERE pipeline_name = %s"
            " AND business_date = %s AND status = 'success' ORDER BY run_id DESC LIMIT 1",
            (pipeline_name, business_date),
        ).fetchone()
