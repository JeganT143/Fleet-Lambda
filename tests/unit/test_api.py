"""Unit tests for the FastAPI app, using a fake repository (no database)."""

from __future__ import annotations

import json
import logging
import uuid
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import psycopg
import pytest
from fastapi.testclient import TestClient

from fleet.api.main import app
from fleet.api.repository import get_repository

T0 = datetime(2026, 1, 3, 10, 0, tzinfo=UTC)
DAY = date(2026, 1, 3)


def _profit_row(vehicle_id: str, profit: str, status: str, **kw) -> dict:
    row = {
        "vehicle_id": vehicle_id,
        "business_date": DAY,
        "trips": 10,
        "event_count": 300,
        "on_trip_event_count": 120,
        "stream_distance_km": Decimal("150.50"),
        "distance_km": Decimal("155.00"),
        "earnings": Decimal("2000.00"),
        "fuel_cost": Decimal("700.00"),
        "maintenance_cost": Decimal("100.00"),
        "total_operating_cost": Decimal("800.00"),
        "estimated_profit": Decimal(profit),
        "utilization_rate": 0.4,
        "service_flag": False,
        "profitability_status": status,
        "computed_at": T0,
    }
    row.update(kw)
    return row


ALERT_ROW = {
    "alert_id": 7,
    "alert_type": "vehicle_idle",
    "severity": "warning",
    "vehicle_id": "V001",
    "business_date": DAY,
    "created_at": T0,
    "message": "V001 idle for 130 simulated minutes",
    "status": "open",
    "resolved_at": None,
}


class FakeRepository:
    """In-memory stand-in for FleetRepository; records the calls it receives."""

    def __init__(self, empty: bool = False) -> None:
        self.empty = empty
        self.calls: list[tuple] = []

    def ping(self):
        self.calls.append(("ping",))

    def latest_fleet_window(self):
        if self.empty:
            return None
        return {
            "window_start": T0,
            "window_end": T0 + timedelta(hours=1),
            "business_date": DAY,
            "vehicles_reporting": 20,
            "active_vehicles": 15,
            "idle_vehicles": 5,
            "event_count": 250,
            "idle_event_count": 75,
            "idle_ratio": 0.3,
            "trips_completed": 12,
            "total_earnings": Decimal("3456.78"),
            "avg_fare": Decimal("288.07"),
            "updated_at": T0,
        }

    def fleet_day_totals(self, business_date):
        self.calls.append(("fleet_day_totals", business_date))
        return {
            "windows": 3,
            "event_count": 700,
            "trips_completed": 30,
            "total_earnings": Decimal("9000.50"),
        }

    def last_ingestion(self):
        if self.empty:
            return {"last_ingestion_ts": None, "last_event_timestamp": None}
        return {"last_ingestion_ts": T0, "last_event_timestamp": T0 + timedelta(minutes=50)}

    def count_open_alerts(self):
        return 0 if self.empty else 4

    def latest_zone_business_date(self):
        return None if self.empty else DAY

    def zone_metrics(self, business_date):
        self.calls.append(("zone_metrics", business_date))
        return [
            {
                "zone": "Battaramulla",
                "windows": 2,
                "event_count": 400,
                "trips_completed": 10,
                "total_earnings": Decimal("3000.00"),
                "avg_fare": Decimal("300.00"),
            },
            {
                "zone": "Kelaniya",
                "windows": 2,
                "event_count": 100,
                "trips_completed": 0,
                "total_earnings": Decimal("0"),
                "avg_fare": None,
            },
        ]

    def latest_vehicle_event(self, vehicle_id):
        if vehicle_id not in ("V001", "V002"):
            return None
        if vehicle_id == "V002":  # only seen by the batch layer
            return None
        return {
            "event_id": uuid.UUID("00000000-0000-0000-0000-000000000001"),
            "trip_id": "T1",
            "driver_id": "D001",
            "status": "on_trip",
            "latitude": 6.914,
            "longitude": 79.877,
            "zone": "Battaramulla",
            "speed": 32.5,
            "fare": Decimal("0.00"),
            "event_timestamp": T0,
            "business_date": DAY,
            "ingestion_ts": T0,
        }

    def vehicle_stream_stats(self, vehicle_id, business_date):
        self.calls.append(("vehicle_stream_stats", vehicle_id, business_date))
        return {"event_count": 120, "trips_completed": 4, "earnings": Decimal("1200.00")}

    def vehicle_daily_profitability(self, vehicle_id, limit=7):
        self.calls.append(("vehicle_daily_profitability", vehicle_id, limit))
        if vehicle_id in ("V001", "V002"):
            return [_profit_row(vehicle_id, "1200.00", "profitable")]
        return []

    def vehicle_open_alerts(self, vehicle_id):
        return [ALERT_ROW] if vehicle_id == "V001" else []

    def list_alerts(self, status=None, alert_type=None, vehicle_id=None, limit=50):
        self.calls.append(("list_alerts", status, alert_type, vehicle_id, limit))
        return [ALERT_ROW]

    def daily_report_summary(self, business_date):
        if business_date != DAY:
            return None
        return {
            "vehicle_count": 2,
            "total_trips": 20,
            "total_earnings": Decimal("4000.00"),
            "total_operating_cost": Decimal("1600.00"),
            "total_estimated_profit": Decimal("2400.00"),
            "avg_utilization_rate": 0.4,
        }

    def daily_report_status_counts(self, business_date):
        return {"profitable": 1, "unprofitable": 1}

    def daily_report_rows(self, business_date):
        return [
            _profit_row("V003", "-100.00", "unprofitable"),
            _profit_row("V001", "2500.00", "profitable"),
        ]

    def latest_pipeline_run(self, business_date):
        return {
            "run_id": 11,
            "pipeline_name": "reconciliation",
            "business_date": business_date,
            "status": "success",
            "started_at": T0,
            "finished_at": T0 + timedelta(seconds=40),
            "rows_read": 6000,
            "rows_written": 20,
            "rows_rejected": 0,
            "error_message": None,
            "airflow_run_id": "scheduled__2026-01-03",
        }


class BrokenRepository(FakeRepository):
    """Every query fails as if PostgreSQL were down."""

    def __getattribute__(self, name):
        if name in ("empty", "calls") or name.startswith("__"):
            return object.__getattribute__(self, name)

        def fail(*args, **kwargs):
            raise psycopg.OperationalError("connection refused")

        return fail


@pytest.fixture
def fake_repo():
    return FakeRepository()


@pytest.fixture
def client(fake_repo):
    app.dependency_overrides[get_repository] = lambda: fake_repo
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


def _use(repo):
    app.dependency_overrides[get_repository] = lambda: repo


# ============================================================================ health
def test_health_ok(client):
    r = client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["database"] == "ok"


def test_health_database_down_returns_503(client):
    _use(BrokenRepository())
    r = client.get("/health")
    assert r.status_code == 503
    assert r.json()["database"] == "unavailable"
    assert r.json()["status"] == "degraded"


# ============================================================================ fleet summary
def test_fleet_summary_happy_path(client, fake_repo):
    r = client.get("/api/v1/fleet/summary")
    assert r.status_code == 200
    body = r.json()
    assert body["data_available"] is True
    w = body["latest_window"]
    assert w["vehicles_reporting"] == 20
    assert w["active_vehicles"] == 15
    assert w["idle_vehicles"] == 5
    assert w["idle_ratio"] == 0.3
    assert w["window_hours"] == 1.0
    assert w["events_per_hour"] == 250.0
    assert w["trips_per_hour"] == 12.0
    # NUMERIC -> JSON number, not string
    assert w["total_earnings"] == 3456.78
    assert isinstance(w["avg_fare"], float)
    totals = body["business_day_totals"]
    assert totals == {
        "business_date": "2026-01-03",
        "windows": 3,
        "event_count": 700,
        "trips_completed": 30,
        "total_earnings": 9000.5,
    }
    assert ("fleet_day_totals", DAY) in fake_repo.calls
    assert body["open_alerts"] == 4
    assert body["last_ingestion_ts"].startswith("2026-01-03T10:00:00")


def test_fleet_summary_no_data_is_200_with_nulls(client):
    _use(FakeRepository(empty=True))
    r = client.get("/api/v1/fleet/summary")
    assert r.status_code == 200
    body = r.json()
    assert body["data_available"] is False
    assert body["message"]
    assert body["latest_window"] is None
    assert body["business_day_totals"] is None
    assert body["last_ingestion_ts"] is None
    assert body["open_alerts"] == 0


def test_fleet_summary_database_down_returns_503(client):
    _use(BrokenRepository())
    r = client.get("/api/v1/fleet/summary")
    assert r.status_code == 503
    body = r.json()
    assert body["error"] == "database_unavailable"
    assert "connection refused" not in r.text  # no internals leaked
    assert body["request_id"] == r.headers["X-Request-ID"]


# ============================================================================ zones
def test_zones_default_latest_date(client, fake_repo):
    r = client.get("/api/v1/fleet/zones")
    assert r.status_code == 200
    body = r.json()
    assert body["business_date"] == "2026-01-03"
    assert body["zone_count"] == 2
    assert body["zones"][0] == {
        "zone": "Battaramulla",
        "windows": 2,
        "event_count": 400,
        "trips_completed": 10,
        "total_earnings": 3000.0,
        "avg_fare": 300.0,
    }
    assert body["zones"][1]["avg_fare"] is None
    assert ("zone_metrics", DAY) in fake_repo.calls


def test_zones_explicit_date(client, fake_repo):
    r = client.get("/api/v1/fleet/zones", params={"business_date": "2026-01-02"})
    assert r.status_code == 200
    assert r.json()["business_date"] == "2026-01-02"
    assert ("zone_metrics", date(2026, 1, 2)) in fake_repo.calls


def test_zones_no_data(client):
    _use(FakeRepository(empty=True))
    r = client.get("/api/v1/fleet/zones")
    assert r.status_code == 200
    assert r.json() == {"business_date": None, "zone_count": 0, "zones": []}


def test_zones_bad_date_is_422(client):
    r = client.get("/api/v1/fleet/zones", params={"business_date": "2026-13-45"})
    assert r.status_code == 422
    assert r.json()["error"] == "validation_error"


# ============================================================================ vehicles
def test_vehicle_happy_path(client, fake_repo):
    r = client.get("/api/v1/vehicles/V001")
    assert r.status_code == 200
    body = r.json()
    assert body["vehicle_id"] == "V001"
    ev = body["latest_event"]
    assert ev["status"] == "on_trip"
    assert ev["zone"] == "Battaramulla"
    assert ev["latitude"] == 6.914
    assert ev["event_id"] == "00000000-0000-0000-0000-000000000001"
    assert body["stream_stats"] == {
        "business_date": "2026-01-03",
        "event_count": 120,
        "trips_completed": 4,
        "earnings": 1200.0,
    }
    assert len(body["daily_profitability"]) == 1
    assert body["daily_profitability"][0]["estimated_profit"] == 1200.0
    assert body["open_alerts"][0]["alert_id"] == 7
    assert ("vehicle_daily_profitability", "V001", 7) in fake_repo.calls


def test_vehicle_seen_only_by_batch(client):
    r = client.get("/api/v1/vehicles/V002")
    assert r.status_code == 200
    body = r.json()
    assert body["latest_event"] is None
    assert body["stream_stats"] is None
    assert len(body["daily_profitability"]) == 1


def test_vehicle_unknown_is_404(client):
    r = client.get("/api/v1/vehicles/V999")
    assert r.status_code == 404
    body = r.json()
    assert body["error"] == "not_found"
    assert "V999" in body["message"]


@pytest.mark.parametrize("bad", ["v001", "V01", "V0001", "X001", "V00A"])
def test_vehicle_bad_id_is_422(client, fake_repo, bad):
    r = client.get(f"/api/v1/vehicles/{bad}")
    assert r.status_code == 422
    assert r.json()["error"] == "validation_error"
    assert fake_repo.calls == []  # never reached the database


# ============================================================================ alerts
def test_alerts_default(client, fake_repo):
    r = client.get("/api/v1/alerts")
    assert r.status_code == 200
    body = r.json()
    assert body["count"] == 1
    assert body["limit"] == 50
    assert body["alerts"][0]["alert_type"] == "vehicle_idle"
    assert ("list_alerts", None, None, None, 50) in fake_repo.calls


def test_alerts_filters_passed_to_repository(client, fake_repo):
    r = client.get(
        "/api/v1/alerts",
        params={"status": "open", "alert_type": "low_profitability", "vehicle_id": "V003"},
    )
    assert r.status_code == 200
    assert ("list_alerts", "open", "low_profitability", "V003", 50) in fake_repo.calls


@pytest.mark.parametrize(
    "params",
    [
        {"status": "closed"},
        {"alert_type": "fire"},
        {"vehicle_id": "bus-1"},
        {"limit": 0},
        {"limit": 501},
        {"limit": "ten"},
    ],
)
def test_alerts_invalid_params_are_422(client, fake_repo, params):
    r = client.get("/api/v1/alerts", params=params)
    assert r.status_code == 422
    assert r.json()["error"] == "validation_error"
    assert r.json()["details"]
    assert fake_repo.calls == []


@pytest.mark.parametrize("limit", [1, 500])
def test_alerts_limit_bounds_accepted(client, fake_repo, limit):
    r = client.get("/api/v1/alerts", params={"limit": limit})
    assert r.status_code == 200
    assert fake_repo.calls[-1][-1] == limit


# ============================================================================ reports
def test_daily_report_happy_path(client):
    r = client.get("/api/v1/reports/daily/2026-01-03")
    assert r.status_code == 200
    body = r.json()
    assert body["business_date"] == "2026-01-03"
    assert body["summary"] == {
        "vehicle_count": 2,
        "total_trips": 20,
        "total_earnings": 4000.0,
        "total_operating_cost": 1600.0,
        "total_estimated_profit": 2400.0,
        "avg_utilization_rate": 0.4,
        "status_counts": {"profitable": 1, "unprofitable": 1},
    }
    assert [v["vehicle_id"] for v in body["vehicles"]] == ["V003", "V001"]
    assert body["latest_pipeline_run"]["pipeline_name"] == "reconciliation"


def test_daily_report_missing_date_is_404(client):
    r = client.get("/api/v1/reports/daily/2026-01-04")
    assert r.status_code == 404
    assert r.json()["error"] == "not_found"


@pytest.mark.parametrize("bad", ["2026-02-30", "yesterday", "20260103"])
def test_daily_report_bad_date_is_422(client, bad):
    r = client.get(f"/api/v1/reports/daily/{bad}")
    assert r.status_code == 422


def test_daily_report_database_down_returns_503(client):
    _use(BrokenRepository())
    r = client.get("/api/v1/reports/daily/2026-01-03")
    assert r.status_code == 503
    assert r.json()["error"] == "database_unavailable"


# ============================================================================ errors, middleware
def test_unexpected_error_is_generic_500(client):
    class Exploding(FakeRepository):
        def list_alerts(self, **kwargs):
            raise RuntimeError("secret internal detail")

    _use(Exploding())
    r = client.get("/api/v1/alerts")
    assert r.status_code == 500
    body = r.json()
    assert body["error"] == "internal_error"
    assert "secret" not in r.text
    assert body["request_id"] == r.headers["X-Request-ID"]


def test_unknown_route_uses_error_model(client):
    r = client.get("/api/v1/nope")
    assert r.status_code == 404
    assert r.json()["error"] == "not_found"


class _Capture(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record):
        self.records.append(record)


@pytest.fixture
def api_log():
    handler = _Capture()
    logger = logging.getLogger("api")  # get_logger("api") disables propagation
    logger.addHandler(handler)
    yield handler
    logger.removeHandler(handler)


def test_request_id_header_generated(client):
    r = client.get("/health")
    rid = r.headers["X-Request-ID"]
    assert len(rid) == 32
    assert client.get("/health").headers["X-Request-ID"] != rid


def test_request_id_header_propagated(client):
    r = client.get("/health", headers={"X-Request-ID": "abc-123"})
    assert r.headers["X-Request-ID"] == "abc-123"


def test_request_id_header_rejects_unsafe_value(client):
    r = client.get("/health", headers={"X-Request-ID": "bad value\twith junk"})
    assert r.headers["X-Request-ID"] != "bad value\twith junk"


def test_request_is_logged_as_structured_json(client, api_log):
    r = client.get("/api/v1/alerts", params={"limit": 5}, headers={"X-Request-ID": "rid-1"})
    assert r.status_code == 200
    records = [rec for rec in api_log.records if rec.getMessage() == "request"]
    assert len(records) == 1
    fields = records[0].fields
    assert fields["method"] == "GET"
    assert fields["path"] == "/api/v1/alerts"
    assert fields["status_code"] == 200
    assert fields["request_id"] == "rid-1"
    assert isinstance(fields["duration_ms"], float) and fields["duration_ms"] >= 0
    # the project formatter turns it into one JSON line
    from fleet.common.logs import JsonFormatter

    line = json.loads(JsonFormatter().format(records[0]))
    assert line["component"] == "api"
    assert line["status_code"] == 200


def test_error_requests_are_logged_with_status(client, api_log):
    client.get("/api/v1/vehicles/bad")
    fields = [rec.fields for rec in api_log.records if rec.getMessage() == "request"]
    assert fields[-1]["status_code"] == 422


def test_unexpected_error_logs_traceback(client, api_log):
    class Exploding(FakeRepository):
        def count_open_alerts(self):
            raise RuntimeError("boom")

    _use(Exploding())
    client.get("/api/v1/fleet/summary")
    errors = [rec for rec in api_log.records if rec.getMessage() == "unhandled error"]
    assert errors and errors[0].exc_info is not None
    reqs = [rec.fields for rec in api_log.records if rec.getMessage() == "request"]
    assert reqs[-1]["status_code"] == 500


def test_openapi_has_all_endpoints_and_tags(client):
    spec = client.get("/openapi.json").json()
    paths = set(spec["paths"])
    assert {
        "/health",
        "/api/v1/fleet/summary",
        "/api/v1/fleet/zones",
        "/api/v1/vehicles/{vehicle_id}",
        "/api/v1/alerts",
        "/api/v1/reports/daily/{business_date}",
    } <= paths
    for path, ops in spec["paths"].items():
        assert ops["get"]["tags"], path
        assert ops["get"]["summary"], path
