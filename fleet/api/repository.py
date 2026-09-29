"""Read-only database access for the API (all SQL lives here).

`FleetRepository` is a thin wrapper around one psycopg 3 connection with dict rows.
Every query is parameterised. It only reads and aggregates (SUM / COUNT / latest
row); derived business values (profit, utilisation, statuses, idle ratio) are
computed by the streaming and batch layers and read here as stored.

Connection strategy: one connection per request, opened lazily on the first query
and closed when the request ends (see `get_repository`). Why not a pool:
  * the API is low-traffic (a dashboard / demo), and a local PostgreSQL connection
    costs a few milliseconds;
  * no extra dependency (psycopg_pool) and no pool state to go stale when
    PostgreSQL restarts - every request simply reconnects;
  * lazy opening lets /health report "database unavailable" itself instead of
    failing inside the dependency.
The session is set read-only, so a bug in this module cannot write to the tables
the pipelines own. All queries of one request run in one transaction and therefore
see a consistent snapshot.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date
from typing import Any

import psycopg
from psycopg.rows import dict_row

from fleet.common.config import Settings, load_settings

Row = dict[str, Any]

CONNECT_TIMEOUT_SECONDS = 3

_PROFITABILITY_COLUMNS = """
    vehicle_id, business_date, trips, event_count, on_trip_event_count,
    stream_distance_km, distance_km, earnings, fuel_cost, maintenance_cost,
    total_operating_cost, estimated_profit, utilization_rate, service_flag,
    profitability_status, computed_at
"""

_ALERT_COLUMNS = """
    alert_id, alert_type, severity, vehicle_id, business_date, created_at,
    message, status, resolved_at
"""


class FleetRepository:
    """Read-only queries over the serving tables of sql/001_schema.sql."""

    def __init__(
        self,
        settings: Settings | None = None,
        connection: psycopg.Connection | None = None,
    ) -> None:
        """Use `connection` if given (borrowed, never closed here), else open one lazily."""
        self._settings = settings
        self._conn = connection
        self._owns_connection = connection is None

    # ------------------------------------------------------------------ plumbing
    def _connection(self) -> psycopg.Connection:
        if self._conn is None:
            settings = self._settings or load_settings()
            conn = psycopg.connect(
                settings.postgres_dsn,
                row_factory=dict_row,
                connect_timeout=CONNECT_TIMEOUT_SECONDS,
                application_name="fleet-api",
            )
            conn.read_only = True
            self._conn = conn
        return self._conn

    def _all(self, sql: str, params: dict[str, Any] | None = None) -> list[Row]:
        with self._connection().cursor(row_factory=dict_row) as cur:
            cur.execute(sql, params or {})
            return list(cur.fetchall())

    def _one(self, sql: str, params: dict[str, Any] | None = None) -> Row | None:
        with self._connection().cursor(row_factory=dict_row) as cur:
            cur.execute(sql, params or {})
            return cur.fetchone()

    def close(self) -> None:
        """Close an owned connection; the open read-only transaction is discarded."""
        if self._owns_connection and self._conn is not None:
            conn, self._conn = self._conn, None
            conn.close()

    # ------------------------------------------------------------------ health
    def ping(self) -> None:
        self._one("SELECT 1 AS ok")

    # ------------------------------------------------------------------ fleet
    def latest_fleet_window(self) -> Row | None:
        return self._one(
            """
            SELECT window_start, window_end, business_date, vehicles_reporting,
                   active_vehicles, idle_vehicles, event_count, idle_event_count,
                   idle_ratio, trips_completed, total_earnings, avg_fare, updated_at
            FROM realtime_vehicle_metrics
            ORDER BY window_start DESC
            LIMIT 1
            """
        )

    def fleet_day_totals(self, business_date: date) -> Row:
        row = self._one(
            """
            SELECT COUNT(*)                         AS windows,
                   COALESCE(SUM(event_count), 0)     AS event_count,
                   COALESCE(SUM(trips_completed), 0) AS trips_completed,
                   COALESCE(SUM(total_earnings), 0)  AS total_earnings
            FROM realtime_vehicle_metrics
            WHERE business_date = %(business_date)s
            """,
            {"business_date": business_date},
        )
        assert row is not None  # an aggregate without GROUP BY always returns a row
        return row

    def last_ingestion(self) -> Row:
        row = self._one(
            """
            SELECT MAX(ingestion_ts)    AS last_ingestion_ts,
                   MAX(event_timestamp) AS last_event_timestamp
            FROM stream_events
            """
        )
        assert row is not None
        return row

    def count_open_alerts(self) -> int:
        row = self._one("SELECT COUNT(*) AS n FROM alerts WHERE status = 'open'")
        return int(row["n"]) if row else 0

    def latest_zone_business_date(self) -> date | None:
        row = self._one("SELECT MAX(business_date) AS d FROM realtime_zone_metrics")
        return row["d"] if row else None

    def zone_metrics(self, business_date: date) -> list[Row]:
        return self._all(
            """
            SELECT zone,
                   COUNT(*)             AS windows,
                   SUM(event_count)     AS event_count,
                   SUM(trips_completed) AS trips_completed,
                   SUM(total_earnings)  AS total_earnings,
                   ROUND(SUM(total_earnings) / NULLIF(SUM(trips_completed), 0), 2) AS avg_fare
            FROM realtime_zone_metrics
            WHERE business_date = %(business_date)s
            GROUP BY zone
            ORDER BY total_earnings DESC, zone
            """,
            {"business_date": business_date},
        )

    # ------------------------------------------------------------------ vehicles
    def latest_vehicle_event(self, vehicle_id: str) -> Row | None:
        return self._one(
            """
            SELECT event_id, trip_id, driver_id, status, latitude, longitude, zone,
                   speed, fare, event_timestamp, business_date, ingestion_ts
            FROM stream_events
            WHERE vehicle_id = %(vehicle_id)s
            ORDER BY event_timestamp DESC
            LIMIT 1
            """,
            {"vehicle_id": vehicle_id},
        )

    def vehicle_stream_stats(self, vehicle_id: str, business_date: date) -> Row:
        row = self._one(
            """
            SELECT COUNT(*)                         AS event_count,
                   COUNT(*) FILTER (WHERE fare > 0) AS trips_completed,
                   COALESCE(SUM(fare), 0)           AS earnings
            FROM stream_events
            WHERE vehicle_id = %(vehicle_id)s AND business_date = %(business_date)s
            """,
            {"vehicle_id": vehicle_id, "business_date": business_date},
        )
        assert row is not None
        return row

    def vehicle_daily_profitability(self, vehicle_id: str, limit: int = 7) -> list[Row]:
        return self._all(
            f"""
            SELECT {_PROFITABILITY_COLUMNS}
            FROM daily_vehicle_profitability
            WHERE vehicle_id = %(vehicle_id)s
            ORDER BY business_date DESC
            LIMIT %(limit)s
            """,
            {"vehicle_id": vehicle_id, "limit": limit},
        )

    def vehicle_open_alerts(self, vehicle_id: str) -> list[Row]:
        return self._all(
            f"""
            SELECT {_ALERT_COLUMNS}
            FROM alerts
            WHERE vehicle_id = %(vehicle_id)s AND status = 'open'
            ORDER BY created_at DESC, alert_id DESC
            """,
            {"vehicle_id": vehicle_id},
        )

    # ------------------------------------------------------------------ alerts
    def list_alerts(
        self,
        status: str | None = None,
        alert_type: str | None = None,
        vehicle_id: str | None = None,
        limit: int = 50,
    ) -> list[Row]:
        # NULL parameters disable their filter; the SQL text itself never changes.
        return self._all(
            f"""
            SELECT {_ALERT_COLUMNS}
            FROM alerts
            WHERE (%(status)s::text IS NULL OR status = %(status)s::text)
              AND (%(alert_type)s::text IS NULL OR alert_type = %(alert_type)s::text)
              AND (%(vehicle_id)s::text IS NULL OR vehicle_id = %(vehicle_id)s::text)
            ORDER BY created_at DESC, alert_id DESC
            LIMIT %(limit)s
            """,
            {"status": status, "alert_type": alert_type, "vehicle_id": vehicle_id, "limit": limit},
        )

    # ------------------------------------------------------------------ reports
    def daily_report_summary(self, business_date: date) -> Row | None:
        """Aggregates over one day's batch rows, or None when the day has no rows."""
        row = self._one(
            """
            SELECT COUNT(*)                  AS vehicle_count,
                   SUM(trips)                AS total_trips,
                   SUM(earnings)             AS total_earnings,
                   SUM(total_operating_cost) AS total_operating_cost,
                   SUM(estimated_profit)     AS total_estimated_profit,
                   AVG(utilization_rate)     AS avg_utilization_rate
            FROM daily_vehicle_profitability
            WHERE business_date = %(business_date)s
            """,
            {"business_date": business_date},
        )
        if row is None or row["vehicle_count"] == 0:
            return None
        return row

    def daily_report_status_counts(self, business_date: date) -> dict[str, int]:
        rows = self._all(
            """
            SELECT profitability_status, COUNT(*) AS n
            FROM daily_vehicle_profitability
            WHERE business_date = %(business_date)s
            GROUP BY profitability_status
            """,
            {"business_date": business_date},
        )
        return {r["profitability_status"]: int(r["n"]) for r in rows}

    def daily_report_rows(self, business_date: date) -> list[Row]:
        return self._all(
            f"""
            SELECT {_PROFITABILITY_COLUMNS}
            FROM daily_vehicle_profitability
            WHERE business_date = %(business_date)s
            ORDER BY estimated_profit ASC, vehicle_id
            """,
            {"business_date": business_date},
        )

    def latest_pipeline_run(self, business_date: date) -> Row | None:
        return self._one(
            """
            SELECT run_id, pipeline_name, business_date, status, started_at, finished_at,
                   rows_read, rows_written, rows_rejected, error_message, airflow_run_id
            FROM pipeline_runs
            WHERE business_date = %(business_date)s
            ORDER BY started_at DESC, run_id DESC
            LIMIT 1
            """,
            {"business_date": business_date},
        )


def get_repository() -> Iterator[FleetRepository]:
    """FastAPI dependency: one repository (and at most one connection) per request.

    Tests replace it through `app.dependency_overrides[get_repository]`.
    """
    repo = FleetRepository()
    try:
        yield repo
    finally:
        repo.close()
