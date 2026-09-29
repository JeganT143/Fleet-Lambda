"""Which simulated business dates are finished? (shared by the generator and the DAG)

Simulated time is owned by the producer (docs/decisions.md ADR-003), so "now" in
simulated time is read from the data: the business_date of the most recently
INGESTED event. A business date d is *completed* once the stream has moved past it,
i.e. d < current business date.

Using the latest ingested event (instead of MAX(business_date)) means rows inserted by
tests with far-future dates (2099) and an old ingestion_ts can never make the live day
look finished. Both queries use indexes (ingestion_ts DESC, (business_date, vehicle_id)).
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import psycopg

from fleet.common.contracts import expense_file_path

CURRENT_DATE_SQL = """
SELECT business_date FROM stream_events ORDER BY ingestion_ts DESC LIMIT 1
"""

# Recursive "loose index scan": walks the distinct business dates through the
# (business_date, vehicle_id) index instead of reading every event.
COMPLETED_DATES_SQL = """
WITH RECURSIVE d AS (
    SELECT MIN(business_date) AS bd FROM stream_events
    UNION ALL
    SELECT (SELECT MIN(business_date) FROM stream_events WHERE business_date > d.bd)
    FROM d WHERE d.bd IS NOT NULL
)
SELECT bd AS business_date FROM d WHERE bd IS NOT NULL AND bd < %(current)s ORDER BY bd
"""


def current_business_date(conn: psycopg.Connection) -> date | None:
    """Simulated 'today': business date of the latest ingested event (None if no events)."""
    row = conn.execute(CURRENT_DATE_SQL).fetchone()
    if row is None:
        return None
    return row["business_date"] if isinstance(row, dict) else row[0]


def completed_business_dates(conn: psycopg.Connection, today: date | None = None) -> list[date]:
    """All business dates with stream events that are strictly before simulated 'today'.

    today defaults to current_business_date(conn); tests pass their own.
    """
    current = today or current_business_date(conn)
    if current is None:
        return []
    rows = conn.execute(COMPLETED_DATES_SQL, {"current": current}).fetchall()
    return [r["business_date"] if isinstance(r, dict) else r[0] for r in rows]


RECONCILED_DATES_SQL = """
SELECT DISTINCT business_date FROM pipeline_runs
WHERE pipeline_name = 'reconciliation' AND status = 'success'
"""


def pending_business_dates(
    conn: psycopg.Connection, landing_dir: str, today: date | None = None
) -> list[date]:
    """Completed dates that have a landing file but no successful reconciliation yet,
    oldest first. The DAG processes the first one on each scheduled run."""
    reconciled = {
        r["business_date"] if isinstance(r, dict) else r[0]
        for r in conn.execute(RECONCILED_DATES_SQL).fetchall()
    }
    return [
        d
        for d in completed_business_dates(conn, today)
        if d not in reconciled and Path(expense_file_path(landing_dir, d.isoformat())).is_file()
    ]
