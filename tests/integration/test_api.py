"""API -> PostgreSQL integration tests (real SQL, real schema).

Two isolation strategies, both following tests/conftest.py conventions
(vehicles V950-V959, business dates in 2099):

1. `seeded` inserts a full fixture across all serving tables inside ONE
   uncommitted transaction and points a real FleetRepository at that connection.
   Teardown rolls back, so nothing is ever visible to the live pipelines or to
   other sessions (a 2099 window would otherwise briefly become the "latest"
   window for the live API and for the idle-alert DAG), and cleanup happens even
   if the test process is killed.
2. `committed_report` commits a small batch-only fixture (pipeline_runs +
   daily_vehicle_profitability, positive profits so no alert rule reacts) and
   calls the API through the real `get_repository` dependency, i.e. its own
   connection. Teardown deletes exactly those rows in `finally`.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import psycopg
import pytest
from fastapi.testclient import TestClient

from fleet.api.main import app
from fleet.api.repository import FleetRepository, get_repository
from fleet.common.db import connect

pytestmark = pytest.mark.integration

D1 = date(2099, 12, 27)
D2 = date(2099, 12, 28)  # the "latest" day of the seeded fixture
D_COMMITTED = date(2099, 12, 20)
TEST_PIPELINE = "api_integration_test"


def _ts(day: date, hour: int, minute: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=UTC)


def _event_id(n: int) -> uuid.UUID:
    return uuid.uuid5(uuid.NAMESPACE_URL, f"fleet-api-integration-test/{n}")


def _insert_profit(cur, vehicle_id, day, trips, earnings, fuel, maint, util, status, run_id=None):
    # Fixture rows as the batch layer would store them (the API only reads these).
    earnings, fuel, maint = Decimal(earnings), Decimal(fuel), Decimal(maint)
    cur.execute(
        """
        INSERT INTO daily_vehicle_profitability (
            vehicle_id, business_date, trips, event_count, on_trip_event_count,
            stream_distance_km, distance_km, earnings, fuel_cost, maintenance_cost,
            total_operating_cost, estimated_profit, utilization_rate, service_flag,
            profitability_status, run_id)
        VALUES (%s, %s, %s, 300, %s, 100.00, 110.00, %s, %s, %s, %s, %s, %s, false, %s, %s)
        """,
        (
            vehicle_id,
            day,
            trips,
            int(util * 300),
            earnings,
            fuel,
            maint,
            fuel + maint,
            earnings - fuel - maint,
            util,
            status,
            run_id,
        ),
    )


def _insert_run(cur, pipeline, day, started, status="success") -> int:
    cur.execute(
        """
        INSERT INTO pipeline_runs (pipeline_name, business_date, status, started_at,
                                   finished_at, rows_read, rows_written, rows_rejected)
        VALUES (%s, %s, %s, %s, %s, 900, 3, 0)
        RETURNING run_id
        """,
        (pipeline, day, status, started, started + timedelta(seconds=30)),
    )
    return cur.fetchone()["run_id"]


def _seed(conn) -> dict:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS n FROM alerts WHERE status = 'open'")
        open_before = cur.fetchone()["n"]

        # --- speed layer: two fleet windows on D2 (11:00 is the latest)
        for start, rep, act, idle, ev, idle_ev, ratio, trips, earn, avg in [
            (_ts(D2, 10), 3, 3, 0, 100, 10, 0.1, 4, "1000.00", "250.00"),
            (_ts(D2, 11), 3, 2, 1, 60, 20, 0.3333, 3, "750.50", "250.17"),
        ]:
            cur.execute(
                """
                INSERT INTO realtime_vehicle_metrics (
                    window_start, window_end, business_date, vehicles_reporting,
                    active_vehicles, idle_vehicles, event_count, idle_event_count,
                    idle_ratio, trips_completed, total_earnings, avg_fare)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (start, start + timedelta(hours=1), D2, rep, act, idle, ev, idle_ev, ratio)
                + (trips, Decimal(earn), Decimal(avg)),
            )
        # --- zone windows on D2
        for start, zone, ev, trips, earn in [
            (_ts(D2, 10), "test_central", 50, 2, "600.00"),
            (_ts(D2, 11), "test_central", 30, 2, "400.00"),
            (_ts(D2, 11), "test_airport", 20, 3, "750.50"),
            (_ts(D2, 11), "test_north", 10, 0, "0.00"),
        ]:
            cur.execute(
                """
                INSERT INTO realtime_zone_metrics (
                    window_start, zone, window_end, business_date, vehicles_reporting,
                    active_vehicles, event_count, trips_completed, total_earnings, avg_fare)
                VALUES (%s, %s, %s, %s, 1, 1, %s, %s, %s, NULL)
                """,
                (start, zone, start + timedelta(hours=1), D2, ev, trips, Decimal(earn)),
            )
        # --- raw events for V950: one on D1, three on D2 (latest = 11:40, fare 300)
        for n, day, hh, mm, status, fare, zone in [
            (1, D1, 20, 0, "on_trip", "200.00", "test_central"),
            (2, D2, 11, 30, "idle", "0", "test_central"),
            (3, D2, 11, 35, "on_trip", "0", "test_airport"),
            (4, D2, 11, 40, "on_trip", "300.00", "test_airport"),
        ]:
            cur.execute(
                """
                INSERT INTO stream_events (
                    event_id, trip_id, driver_id, vehicle_id, latitude, longitude, speed,
                    status, fare, event_timestamp, business_date, zone, kafka_partition,
                    kafka_offset, ingestion_ts, processing_ts)
                VALUES (%s, %s, 'D950', 'V950', 12.95, 77.66, %s, %s, %s, %s, %s, %s,
                        0, %s, %s, %s)
                """,
                (
                    _event_id(n),
                    None if status == "idle" else f"T950-{day}",
                    0.0 if status == "idle" else 42.5,
                    status,
                    Decimal(fare),
                    _ts(day, hh, mm),
                    day,
                    zone,
                    n,
                    _ts(day, hh, mm) + timedelta(seconds=1),
                    _ts(day, hh, mm) + timedelta(seconds=5),
                ),
            )
        # --- batch layer: 8 days for V950 (API must return only the newest 7)
        for i in range(8):
            _insert_profit(
                cur, "V950", D2 - timedelta(days=i), 5, "1500.00", "600.00", "400.00", 0.4, "watch"
            )
        _insert_profit(cur, "V951", D2, 12, "3000.00", "900.00", "100.00", 0.6, "profitable")
        _insert_profit(cur, "V952", D2, 1, "300.00", "400.00", "100.00", 0.1, "unprofitable")
        _insert_run(cur, "expenses_load", D2, _ts(D2, 23, 50))
        latest_run = _insert_run(cur, "reconciliation", D2, _ts(D2, 23, 55))

        # --- alerts
        for vid, atype, status, created, key in [
            ("V950", "vehicle_idle", "open", _ts(D2, 11), "a"),
            ("V950", "low_profitability", "resolved", _ts(D2, 12), "b"),
            ("V952", "low_profitability", "open", _ts(D2, 13), "c"),
        ]:
            cur.execute(
                """
                INSERT INTO alerts (alert_type, severity, vehicle_id, business_date,
                                    created_at, message, status, dedup_key)
                VALUES (%s, 'warning', %s, %s, %s, %s, %s, %s)
                """,
                (atype, vid, D2, created, f"test {atype} {vid}", status, f"api-test-{key}"),
            )
    return {"open_alerts_before": open_before, "latest_run_id": latest_run}


@pytest.fixture
def seeded(pg_conn) -> Iterator[tuple[TestClient, dict]]:
    """TestClient whose repository reads the uncommitted fixture on `pg_conn`."""
    # one snapshot for the whole test: live pipelines committing meanwhile can't
    # change counts such as open alerts
    pg_conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
    try:
        info = _seed(pg_conn)
        app.dependency_overrides[get_repository] = lambda: FleetRepository(connection=pg_conn)
        with TestClient(app) as client:
            yield client, info
    finally:
        app.dependency_overrides.clear()
        pg_conn.rollback()  # nothing was ever committed


# ============================================================================ seeded tests
def test_summary_reads_latest_window_and_day_totals(seeded):
    client, info = seeded
    r = client.get("/api/v1/fleet/summary")
    assert r.status_code == 200
    body = r.json()
    assert body["data_available"] is True
    w = body["latest_window"]
    assert w["window_start"] == "2099-12-28T11:00:00Z"
    assert w["business_date"] == "2099-12-28"
    assert (w["vehicles_reporting"], w["active_vehicles"], w["idle_vehicles"]) == (3, 2, 1)
    assert w["idle_ratio"] == pytest.approx(0.3333)
    assert w["events_per_hour"] == 60.0
    assert w["trips_per_hour"] == 3.0
    assert w["total_earnings"] == 750.5
    assert w["avg_fare"] == 250.17
    assert body["business_day_totals"] == {
        "business_date": "2099-12-28",
        "windows": 2,
        "event_count": 160,
        "trips_completed": 7,
        "total_earnings": 1750.5,
    }
    assert body["last_ingestion_ts"] == "2099-12-28T11:40:01Z"
    assert body["last_event_timestamp"] == "2099-12-28T11:40:00Z"
    assert body["open_alerts"] == info["open_alerts_before"] + 2


def test_zones_default_to_latest_date(seeded):
    client, _ = seeded
    for params in ({}, {"business_date": "2099-12-28"}):
        r = client.get("/api/v1/fleet/zones", params=params)
        assert r.status_code == 200
        body = r.json()
        assert body["business_date"] == "2099-12-28"
        zones = {z["zone"]: z for z in body["zones"]}
        assert set(zones) == {"test_central", "test_airport", "test_north"}
        assert zones["test_central"] == {
            "zone": "test_central",
            "windows": 2,
            "event_count": 80,
            "trips_completed": 4,
            "total_earnings": 1000.0,
            "avg_fare": 250.0,
        }
        assert zones["test_airport"]["avg_fare"] == 250.17
        assert zones["test_north"]["avg_fare"] is None
        assert body["zones"][0]["zone"] == "test_central"  # highest earnings first


def test_zones_date_without_data_is_empty(seeded):
    client, _ = seeded
    r = client.get("/api/v1/fleet/zones", params={"business_date": "2099-01-01"})
    assert r.status_code == 200
    assert r.json()["zones"] == []


def test_vehicle_detail(seeded):
    client, _ = seeded
    r = client.get("/api/v1/vehicles/V950")
    assert r.status_code == 200
    body = r.json()
    ev = body["latest_event"]
    assert ev["event_id"] == str(_event_id(4))
    assert ev["status"] == "on_trip"
    assert ev["zone"] == "test_airport"
    assert ev["speed"] == 42.5
    assert (ev["latitude"], ev["longitude"]) == (12.95, 77.66)
    assert ev["event_timestamp"] == "2099-12-28T11:40:00Z"
    assert ev["ingestion_ts"] == "2099-12-28T11:40:01Z"
    assert body["stream_stats"] == {
        "business_date": "2099-12-28",
        "event_count": 3,
        "trips_completed": 1,
        "earnings": 300.0,
    }
    days = [row["business_date"] for row in body["daily_profitability"]]
    assert days == [(D2 - timedelta(days=i)).isoformat() for i in range(7)]
    assert body["daily_profitability"][0]["estimated_profit"] == 500.0
    assert [a["alert_type"] for a in body["open_alerts"]] == ["vehicle_idle"]


def test_vehicle_never_seen_is_404(seeded):
    client, _ = seeded
    assert client.get("/api/v1/vehicles/V959").status_code == 404


def test_alert_filters(seeded):
    client, _ = seeded
    r = client.get("/api/v1/alerts", params={"vehicle_id": "V950"})
    assert r.status_code == 200
    alerts = r.json()["alerts"]
    assert [a["alert_type"] for a in alerts] == ["low_profitability", "vehicle_idle"]  # newest 1st

    r = client.get("/api/v1/alerts", params={"vehicle_id": "V950", "status": "open"})
    assert [a["alert_type"] for a in r.json()["alerts"]] == ["vehicle_idle"]

    r = client.get(
        "/api/v1/alerts",
        params={"alert_type": "low_profitability", "status": "open", "vehicle_id": "V952"},
    )
    assert [a["vehicle_id"] for a in r.json()["alerts"]] == ["V952"]

    # global list: the 2099 alerts are the newest rows in the table
    r = client.get("/api/v1/alerts", params={"limit": 2})
    body = r.json()
    assert body["count"] == 2
    assert [a["vehicle_id"] for a in body["alerts"]] == ["V952", "V950"]


def test_daily_report(seeded):
    client, info = seeded
    r = client.get("/api/v1/reports/daily/2099-12-28")
    assert r.status_code == 200
    body = r.json()
    s = body["summary"]
    assert s["vehicle_count"] == 3
    assert s["total_trips"] == 5 + 12 + 1
    assert s["total_earnings"] == 1500 + 3000 + 300
    assert s["total_operating_cost"] == 1000 + 1000 + 500
    assert s["total_estimated_profit"] == 500 + 2000 - 200
    assert s["avg_utilization_rate"] == pytest.approx((0.4 + 0.6 + 0.1) / 3)
    assert s["status_counts"] == {"watch": 1, "profitable": 1, "unprofitable": 1}
    assert [v["vehicle_id"] for v in body["vehicles"]] == ["V952", "V950", "V951"]
    run = body["latest_pipeline_run"]
    assert run["run_id"] == info["latest_run_id"]
    assert run["pipeline_name"] == "reconciliation"


def test_daily_report_missing_date_is_404(seeded):
    client, _ = seeded
    assert client.get("/api/v1/reports/daily/2099-01-01").status_code == 404


# ============================================================================ committed path
@pytest.fixture
def committed_report(settings) -> Iterator[None]:
    """Committed batch-only rows; deleted in `finally` by their exact keys."""
    keys = [("V953", D_COMMITTED), ("V954", D_COMMITTED)]

    def cleanup() -> None:
        with connect(settings) as conn:
            for vid, day in keys:
                conn.execute(
                    "DELETE FROM daily_vehicle_profitability "
                    "WHERE vehicle_id = %s AND business_date = %s",
                    (vid, day),
                )
            conn.execute(
                "DELETE FROM pipeline_runs WHERE pipeline_name = %s AND business_date = %s",
                (TEST_PIPELINE, D_COMMITTED),
            )

    cleanup()  # leftovers of an interrupted earlier run
    try:
        with connect(settings) as conn, conn.cursor() as cur:
            run_id = _insert_run(cur, TEST_PIPELINE, D_COMMITTED, _ts(D_COMMITTED, 23))
            _insert_profit(
                cur,
                "V953",
                D_COMMITTED,
                8,
                "2000.00",
                "500.00",
                "100.00",
                0.5,
                "profitable",
                run_id,
            )
            _insert_profit(
                cur, "V954", D_COMMITTED, 4, "900.00", "400.00", "100.00", 0.3, "watch", run_id
            )
        yield
    finally:
        cleanup()


def test_real_connection_path(committed_report):
    """No dependency override: the API opens its own read-only connection."""
    app.dependency_overrides.clear()
    with TestClient(app) as client:
        health = client.get("/health")
        assert health.status_code == 200
        assert health.json()["database"] == "ok"

        r = client.get(f"/api/v1/reports/daily/{D_COMMITTED.isoformat()}")
        assert r.status_code == 200
        body = r.json()
        assert body["summary"]["vehicle_count"] == 2
        assert body["summary"]["total_estimated_profit"] == 1400 + 400
        assert [v["vehicle_id"] for v in body["vehicles"]] == ["V954", "V953"]
        assert body["latest_pipeline_run"]["pipeline_name"] == TEST_PIPELINE

        r = client.get("/api/v1/vehicles/V953")
        assert r.status_code == 200
        assert r.json()["latest_event"] is None
        assert r.json()["daily_profitability"][0]["estimated_profit"] == 1400.0

        assert client.get("/api/v1/vehicles/V958").status_code == 404
        assert client.get("/api/v1/vehicles/nope").status_code == 422
        # summary works against whatever live data exists (possibly none)
        assert client.get("/api/v1/fleet/summary").status_code == 200


def test_committed_rows_are_removed(settings):
    """Guard: no API test rows may survive in the shared database."""
    with connect(settings) as conn:
        n = conn.execute(
            "SELECT COUNT(*) AS n FROM daily_vehicle_profitability "
            "WHERE vehicle_id IN ('V950','V951','V952','V953','V954')"
        ).fetchone()["n"]
        m = conn.execute(
            "SELECT COUNT(*) AS n FROM pipeline_runs WHERE pipeline_name = %s", (TEST_PIPELINE,)
        ).fetchone()["n"]
        k = conn.execute("SELECT COUNT(*) AS n FROM alerts WHERE dedup_key LIKE 'api-test-%'")
        assert (n, m, k.fetchone()["n"]) == (0, 0, 0)
