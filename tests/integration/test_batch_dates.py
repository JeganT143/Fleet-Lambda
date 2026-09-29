"""Which business date does the daily DAG pick? (fleet/batch/dates.py against PostgreSQL)

Isolation from the live pipeline:
- test events are V9xx vehicles on 2099 dates with ingestion_ts in the year 2000, so the
  live "latest ingested event" (simulated today) is never a test row;
- the test passes its own `today` (2099-04-04) and a temporary landing dir, and only looks
  at 2099-04 dates in the results (live 2026 dates have no file in the temp dir anyway);
- the reconciled date is marked by a pipeline_runs row for 2099-04-01 only.
"""

from __future__ import annotations

import uuid
from datetime import date
from pathlib import Path

import psycopg
import pytest

from fleet.batch.dates import completed_business_dates, pending_business_dates
from fleet.common.contracts import expense_file_path

pytestmark = pytest.mark.integration

D1, D2, D3, TODAY = (date(2099, 4, d) for d in (1, 2, 3, 4))
TEST_DATES = (D1, D2, D3, TODAY)


def in_test_range(dates: list[date]) -> list[date]:
    return [d for d in dates if D1 <= d <= TODAY]


def cleanup(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "DELETE FROM stream_events WHERE vehicle_id = 'V961'"
            " AND business_date BETWEEN %s AND %s",
            (D1, TODAY),
        )
        conn.execute(
            "DELETE FROM pipeline_runs WHERE pipeline_name = 'reconciliation'"
            " AND business_date BETWEEN %s AND %s",
            (D1, TODAY),
        )


@pytest.fixture
def db(settings):
    cleanup(settings.postgres_dsn)
    with psycopg.connect(settings.postgres_dsn, row_factory=psycopg.rows.dict_row) as conn:
        for d in TEST_DATES:
            conn.execute(
                "INSERT INTO stream_events (event_id, trip_id, driver_id, vehicle_id, latitude,"
                " longitude, speed, status, fare, event_timestamp, business_date, zone,"
                " kafka_partition, kafka_offset, ingestion_ts, processing_ts)"
                " VALUES (%s, NULL, 'D961', 'V961', 6.91, 79.915, 0, 'idle', 0, %s, %s,"
                " 'Battaramulla', 9997, 0, '2000-01-01T00:00:00Z', '2000-01-01T00:00:00Z')",
                (uuid.uuid5(uuid.NAMESPACE_URL, f"dates-it-{d}"), f"{d}T12:00:00Z", d),
            )
        # D1 was already reconciled successfully; D2 only has a FAILED run
        conn.execute(
            "INSERT INTO pipeline_runs (pipeline_name, business_date, status)"
            " VALUES ('reconciliation', %s, 'success'), ('reconciliation', %s, 'failed')",
            (D1, D2),
        )
        conn.commit()
        yield conn
    cleanup(settings.postgres_dsn)


def touch_file(landing_dir: str, d: date) -> None:
    path = Path(expense_file_path(landing_dir, d.isoformat()))
    path.parent.mkdir(parents=True)
    path.write_text("header only\n")


def test_completed_dates_are_before_today(db):
    assert in_test_range(completed_business_dates(db, today=TODAY)) == [D1, D2, D3]
    assert in_test_range(completed_business_dates(db, today=D3)) == [D1, D2]


def test_oldest_pending_date_with_a_file_and_no_successful_reconciliation(db, tmp_path):
    landing = str(tmp_path)
    for d in (D1, D3, TODAY):  # D2 has NO file
        touch_file(landing, d)
    pending = in_test_range(pending_business_dates(db, landing, today=TODAY))
    # D1: reconciled -> skipped; D2: no file -> not chosen; TODAY: not finished yet
    assert pending == [D3]

    touch_file(landing, D2)  # the file for D2 arrives (its earlier run had failed)
    assert in_test_range(pending_business_dates(db, landing, today=TODAY)) == [D2, D3]


def test_nothing_pending_without_files(db, tmp_path):
    assert in_test_range(pending_business_dates(db, str(tmp_path), today=TODAY)) == []
