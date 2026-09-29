"""Batch layer -> PostgreSQL: the real Spark jobs on a fixture date.

Test data conventions (tests/conftest.py): vehicles V9xx, business dates in 2099.
Stream events get ingestion_ts in the year 2000 so live components that look at the
"latest ingested event" (generator, DAG, alert checks) never see them.
Every row created here is deleted again.
"""

from __future__ import annotations

import dataclasses
import uuid
from datetime import date
from decimal import Decimal
from pathlib import Path

import psycopg
import pytest

pytestmark = [pytest.mark.integration, pytest.mark.spark]

jobs = pytest.importorskip("fleet.batch.jobs", reason="pyspark not installed")
from fleet.alerts.evaluator import raise_low_profitability_alerts  # noqa: E402
from fleet.batch import runs  # noqa: E402
from fleet.batch.generator import corrupt_expenses, to_csv, write_atomically  # noqa: E402
from fleet.batch.validation import ExpenseValidationError  # noqa: E402
from fleet.common.contracts import expense_file_path  # noqa: E402

DAY = "2099-03-10"
BAD_DAY = "2099-03-11"
DAYS = (DAY, BAD_DAY)

EXPENSE_CSV = f"""vehicle_id,fuel_cost,maintenance_cost,distance_covered,service_flag,business_date
V901,100.00,50.00,20.00,false,{DAY}
V902,20.00,30.00,5.00,false,{DAY}
V903,10.00,5.00,0.00,true,{DAY}
"""

# (vehicle, status, time, speed, fare)
EVENTS = [
    ("V901", "idle", "08:00", 0, "0"),
    ("V901", "enroute", "08:05", 30, "0"),
    ("V901", "on_trip", "08:10", 60, "0"),
    ("V901", "on_trip", "08:15", 48, "250.50"),
    ("V901", "on_trip", "09:00", 40, "0"),  # 45-minute gap: not counted as distance
    ("V901", "on_trip", "09:04", 45, "199.50"),
    ("V902", "idle", "08:00", 0, "0"),
    ("V902", "idle", "08:05", 0, "0"),
    ("V904", "on_trip", "10:00", 30, "1000.00"),  # no expense row
]


def cleanup(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        for d in DAYS:
            conn.execute(
                "DELETE FROM alerts WHERE alert_type = 'low_profitability' AND business_date = %s"
                " AND vehicle_id LIKE 'V9%%'",
                (d,),
            )
            conn.execute("DELETE FROM daily_vehicle_profitability WHERE business_date = %s", (d,))
            conn.execute("DELETE FROM vehicle_expenses WHERE business_date = %s", (d,))
            conn.execute(
                "DELETE FROM stream_events WHERE business_date = %s AND vehicle_id LIKE 'V9%%'",
                (d,),
            )
            conn.execute(
                "DELETE FROM data_quality_stats WHERE business_date = %s AND pipeline_name IN"
                " ('expenses_validation', 'expenses_load', 'reconciliation')",
                (d,),
            )
            conn.execute(
                "DELETE FROM pipeline_runs WHERE business_date = %s AND pipeline_name IN"
                " ('expenses_validation', 'expenses_load', 'reconciliation')",
                (d,),
            )


@pytest.fixture
def batch_settings(settings, tmp_path):
    s = dataclasses.replace(settings, landing_dir=str(tmp_path))
    cleanup(s.postgres_dsn)
    yield s
    cleanup(s.postgres_dsn)


def insert_events(dsn: str) -> None:
    with psycopg.connect(dsn) as conn:
        for vid, status, hhmm, speed, fare in EVENTS:
            conn.execute(
                "INSERT INTO stream_events (event_id, trip_id, driver_id, vehicle_id, latitude,"
                " longitude, speed, status, fare, event_timestamp, business_date, zone,"
                " kafka_partition, kafka_offset, ingestion_ts, processing_ts)"
                " VALUES (%s, NULL, %s, %s, 13.05, 80.225, %s, %s, %s, %s, %s, 'central',"
                " 9998, 0, '2000-01-01T00:00:00Z', '2000-01-01T00:00:00Z')",
                (
                    uuid.uuid5(uuid.NAMESPACE_URL, f"batch-it-{vid}-{hhmm}"),
                    "D" + vid[1:],
                    vid,
                    speed,
                    status,
                    Decimal(fare),
                    f"{DAY}T{hhmm}:00Z",
                    DAY,
                ),
            )


def fetch(dsn: str, sql: str, *params) -> list[dict]:
    with psycopg.connect(dsn, row_factory=psycopg.rows.dict_row) as conn:
        return conn.execute(sql, params).fetchall()


def profitability(dsn: str) -> dict[str, dict]:
    rows = fetch(
        dsn,
        "SELECT * FROM daily_vehicle_profitability WHERE business_date = %s ORDER BY vehicle_id",
        DAY,
    )
    # run_id / computed_at legitimately change on a re-run
    return {
        r["vehicle_id"]: {k: v for k, v in r.items() if k not in ("run_id", "computed_at")}
        for r in rows
    }


def test_load_and_reconcile_are_correct_and_idempotent(spark, batch_settings):
    s = batch_settings
    write_atomically(Path(expense_file_path(s.landing_dir, DAY)), EXPENSE_CSV)
    insert_events(s.postgres_dsn)

    load = jobs.run_load_expenses(spark, s, DAY, airflow_run_id="test-run-1")
    rec = jobs.run_reconcile(spark, s, DAY, airflow_run_id="test-run-1")
    assert (load.rows_read, load.rows_written, load.rows_rejected) == (3, 3, 0)
    assert (rec.rows_written, rec.rows_rejected) == (3, 1)  # V904 has no expense row
    assert rec.rows_read == len(EVENTS) + 3

    expenses = fetch(
        s.postgres_dsn,
        "SELECT vehicle_id, fuel_cost, service_flag, run_id FROM vehicle_expenses"
        " WHERE business_date = %s ORDER BY vehicle_id",
        DAY,
    )
    assert [(e["vehicle_id"], e["fuel_cost"], e["service_flag"]) for e in expenses] == [
        ("V901", Decimal("100.00"), False),
        ("V902", Decimal("20.00"), False),
        ("V903", Decimal("10.00"), True),
    ]
    assert {e["run_id"] for e in expenses} == {load.run_id}

    first = profitability(s.postgres_dsn)
    assert sorted(first) == ["V901", "V902", "V903"]
    v1 = first["V901"]
    assert (v1["trips"], v1["event_count"], v1["on_trip_event_count"]) == (2, 6, 4)
    assert v1["earnings"] == Decimal("450.00")
    assert v1["stream_distance_km"] == Decimal("14.50")
    assert v1["distance_km"] == Decimal("20.00")
    assert v1["total_operating_cost"] == Decimal("150.00")
    assert v1["estimated_profit"] == Decimal("300.00")
    assert v1["utilization_rate"] == pytest.approx(4 / 6)
    assert v1["profitability_status"] == "watch"
    assert first["V902"]["estimated_profit"] == Decimal("-50.00")
    assert first["V903"]["trips"] == 0 and first["V903"]["utilization_rate"] == 0.0
    assert first["V903"]["profitability_status"] == "unprofitable"

    runs_rows = fetch(
        s.postgres_dsn,
        "SELECT pipeline_name, status, rows_written, airflow_run_id, finished_at"
        " FROM pipeline_runs WHERE business_date = %s ORDER BY run_id",
        DAY,
    )
    assert [(r["pipeline_name"], r["status"], r["rows_written"]) for r in runs_rows] == [
        ("expenses_load", "success", 3),
        ("reconciliation", "success", 3),
    ]
    assert all(r["airflow_run_id"] == "test-run-1" and r["finished_at"] for r in runs_rows)
    dq = fetch(
        s.postgres_dsn,
        "SELECT records_rejected, rule_failures FROM data_quality_stats"
        " WHERE pipeline_name = 'reconciliation' AND business_date = %s",
        DAY,
    )
    assert dq == [{"records_rejected": 1, "rule_failures": {"missing_expense_row": 1}}]

    # ---- re-run: same rows, same values, no duplicates, a new success run each
    jobs.run_load_expenses(spark, s, DAY, airflow_run_id="test-run-2")
    jobs.run_reconcile(spark, s, DAY, airflow_run_id="test-run-2")
    assert profitability(s.postgres_dsn) == first
    counts = fetch(
        s.postgres_dsn,
        "SELECT (SELECT COUNT(*) FROM vehicle_expenses WHERE business_date = %s) AS e,"
        " (SELECT COUNT(*) FROM daily_vehicle_profitability WHERE business_date = %s) AS p,"
        " (SELECT COUNT(*) FROM pipeline_runs WHERE business_date = %s AND status = 'success'"
        "  AND pipeline_name = 'reconciliation') AS r",
        DAY,
        DAY,
        DAY,
    )[0]
    assert counts == {"e": 3, "p": 3, "r": 2}
    # no staging tables left behind
    staging = [
        runs.staging_table(t, r.run_id)
        for t, r in (("vehicle_expenses", load), ("daily_vehicle_profitability", rec))
    ]
    assert fetch(s.postgres_dsn, "SELECT 1 FROM pg_tables WHERE tablename = ANY(%s)", staging) == []

    # ---- low-profitability alerts: raised once, de-duplicated on re-evaluation
    first_alerts = raise_low_profitability_alerts(DAY, s)
    assert first_alerts["raised"] == 2  # V902 (-50) and V903 (-15) are below 0
    again = raise_low_profitability_alerts(DAY, s)
    assert again["raised"] == 0 and again["below_threshold"] == 2
    alerts = fetch(
        s.postgres_dsn,
        "SELECT vehicle_id, severity, status, dedup_key FROM alerts"
        " WHERE alert_type = 'low_profitability' AND business_date = %s ORDER BY vehicle_id",
        DAY,
    )
    assert [(a["vehicle_id"], a["severity"], a["status"]) for a in alerts] == [
        ("V902", "warning", "open"),
        ("V903", "warning", "open"),
    ]
    assert alerts[0]["dedup_key"] == f"low_profitability:V902:{DAY}"


def test_invalid_file_fails_clearly_and_loads_nothing(spark, batch_settings):
    s = batch_settings
    path = Path(expense_file_path(s.landing_dir, BAD_DAY))
    write_atomically(path, to_csv(corrupt_expenses(date.fromisoformat(BAD_DAY), 10, 42)))

    # 1) the Airflow validation task
    with pytest.raises(ExpenseValidationError) as exc:
        runs.validate_and_record(s, BAD_DAY, airflow_run_id="test-bad")
    message = str(exc.value)
    assert "INVALID" in message and "negative_value=1" in message
    assert "duplicate_vehicle_id=1" in message

    # 2) the Spark load job refuses the same file on its own
    with pytest.raises(ExpenseValidationError):
        jobs.run_load_expenses(spark, s, BAD_DAY, airflow_run_id="test-bad")

    assert (
        fetch(s.postgres_dsn, "SELECT 1 FROM vehicle_expenses WHERE business_date = %s", BAD_DAY)
        == []
    )
    failed = fetch(
        s.postgres_dsn,
        "SELECT pipeline_name, status, rows_read, rows_rejected, rows_written, error_message"
        " FROM pipeline_runs WHERE business_date = %s ORDER BY run_id",
        BAD_DAY,
    )
    assert [(r["pipeline_name"], r["status"]) for r in failed] == [
        ("expenses_validation", "failed"),
        ("expenses_load", "failed"),
    ]
    assert failed[0]["rows_read"] == 11 and failed[0]["rows_written"] == 0
    assert failed[0]["rows_rejected"] >= 6
    assert "ExpenseValidationError" in failed[0]["error_message"]
    dq = fetch(
        s.postgres_dsn,
        "SELECT pipeline_name, records_total, records_rejected, rule_failures"
        " FROM data_quality_stats WHERE business_date = %s ORDER BY pipeline_name",
        BAD_DAY,
    )
    assert [d["pipeline_name"] for d in dq] == ["expenses_load", "expenses_validation"]
    assert dq[1]["rule_failures"]["invalid_vehicle_id"] == 1


def test_missing_file_fails_with_a_failed_run(batch_settings):
    with pytest.raises(FileNotFoundError):
        runs.validate_and_record(batch_settings, BAD_DAY)
    rows = fetch(
        batch_settings.postgres_dsn,
        "SELECT status, error_message FROM pipeline_runs WHERE business_date = %s",
        BAD_DAY,
    )
    assert rows[0]["status"] == "failed" and "not found" in rows[0]["error_message"]
