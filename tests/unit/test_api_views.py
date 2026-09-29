"""Unit tests for the dashboard-oriented API endpoints (fake repository, no database)."""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from fleet.api.main import app
from fleet.api.repository import get_repository
from tests.unit.test_api import DAY, T0, BrokenRepository, FakeRepository


class ViewsFakeRepository(FakeRepository):
    """FakeRepository plus the queries behind the dashboard endpoints."""

    def fleet_windows(self, limit):
        self.calls.append(("fleet_windows", limit))
        latest = self.latest_fleet_window()
        if latest is None:
            return []
        older = dict(latest, window_start=T0 - timedelta(hours=1), window_end=T0)
        return [latest, older][:limit]  # newest first, like the SQL

    def vehicle_states(self):
        if self.empty:
            return []
        base = {
            "driver_id": "D001",
            "latitude": 6.91,
            "longitude": 79.915,
            "zone": "Battaramulla",
            "speed": 30.0,
            "event_timestamp": T0,
            "ingestion_ts": T0,
        }
        return [
            dict(base, vehicle_id="V001", status="on_trip"),
            dict(base, vehicle_id="V002", driver_id="D002", status="idle", speed=0.0),
            dict(base, vehicle_id="V003", driver_id="D003", status="idle", speed=0.0),
        ]

    def report_dates(self):
        if self.empty:
            return []
        return [
            {
                "business_date": DAY,
                "vehicle_count": 2,
                "total_estimated_profit": Decimal("2400.00"),
                "profitable": 1,
                "watch": 0,
                "unprofitable": 1,
            }
        ]

    def pipeline_runs(self, limit):
        self.calls.append(("pipeline_runs", limit))
        return [self.latest_pipeline_run(DAY)]

    def stream_quality_totals(self):
        if self.empty:
            return {"records_total": 0, "records_valid": 0, "records_rejected": 0}
        return {"records_total": 1000, "records_valid": 995, "records_rejected": 5}

    def rejected_by_reason(self):
        return {} if self.empty else {"malformed_json": 3, "invalid_speed": 2}

    def partition_stats(self):
        return [
            {"kafka_partition": 0, "event_count": 500, "vehicle_count": 5},
            {"kafka_partition": 1, "event_count": 500, "vehicle_count": 5},
        ]

    def recent_rejected(self, limit):
        return [
            {
                "rejected_id": 9,
                "reason": "malformed_json",
                "kafka_partition": 1,
                "kafka_offset": 42,
                "ingestion_ts": T0,
                "raw_value_preview": "{not json",
            }
        ]

    def recent_batch_quality(self, limit):
        return [
            {
                "pipeline_name": "expenses_validation",
                "batch_ref": "/data/landing/expenses/2026-01-03/vehicle_expenses.csv",
                "business_date": DAY,
                "records_total": 20,
                "records_valid": 20,
                "records_rejected": 0,
                "rule_failures": {},
                "recorded_at": T0,
            }
        ]


@pytest.fixture
def repo():
    return ViewsFakeRepository()


@pytest.fixture
def client(repo):
    app.dependency_overrides[get_repository] = lambda: repo
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


def test_fleet_windows_are_returned_oldest_first_with_rates(client, repo):
    body = client.get("/api/v1/fleet/windows", params={"limit": 2}).json()
    assert body["count"] == 2
    starts = [w["window_start"] for w in body["windows"]]
    assert starts == sorted(starts)
    assert body["windows"][-1]["events_per_hour"] == 250.0
    assert ("fleet_windows", 2) in repo.calls


@pytest.mark.parametrize("limit", [0, 501])
def test_fleet_windows_limit_bounds(client, limit):
    assert client.get("/api/v1/fleet/windows", params={"limit": limit}).status_code == 422


def test_vehicle_states_counts_statuses(client):
    body = client.get("/api/v1/vehicles").json()
    assert body["count"] == 3
    assert body["status_counts"] == {"on_trip": 1, "idle": 2}
    assert body["vehicles"][0]["vehicle_id"] == "V001"


def test_vehicle_list_route_does_not_shadow_vehicle_detail(client):
    assert client.get("/api/v1/vehicles/V001").status_code == 200


def test_report_dates_include_every_status(client):
    body = client.get("/api/v1/reports/daily").json()
    assert body["count"] == 1
    report = body["reports"][0]
    assert report["business_date"] == DAY.isoformat()
    assert report["total_estimated_profit"] == 2400.0
    assert report["status_counts"] == {"profitable": 1, "watch": 0, "unprofitable": 1}


def test_pipeline_runs(client, repo):
    body = client.get("/api/v1/pipeline/runs", params={"limit": 5}).json()
    assert body["count"] == 1
    assert body["runs"][0]["pipeline_name"] == "reconciliation"
    assert ("pipeline_runs", 5) in repo.calls


def test_data_quality_summary(client):
    body = client.get("/api/v1/data-quality").json()
    assert body["stream_records_total"] == 1000
    assert body["stream_records_rejected"] == 5
    assert body["stream_rejection_rate"] == 0.005
    assert body["rejected_by_reason"] == {"malformed_json": 3, "invalid_speed": 2}
    assert [p["kafka_partition"] for p in body["kafka_partitions"]] == [0, 1]
    assert body["recent_rejected"][0]["raw_value_preview"] == "{not json"
    assert body["recent_batch_checks"][0]["records_valid"] == 20


def test_data_quality_with_no_data_has_zero_rate(repo, client):
    repo.empty = True
    body = client.get("/api/v1/data-quality").json()
    assert body["stream_records_total"] == 0
    assert body["stream_rejection_rate"] == 0.0


def test_empty_views_return_empty_lists(repo, client):
    repo.empty = True
    assert client.get("/api/v1/fleet/windows").json() == {"count": 0, "windows": []}
    assert client.get("/api/v1/vehicles").json()["vehicles"] == []
    assert client.get("/api/v1/reports/daily").json() == {"count": 0, "reports": []}


@pytest.mark.parametrize(
    "path",
    [
        "/api/v1/fleet/windows",
        "/api/v1/vehicles",
        "/api/v1/reports/daily",
        "/api/v1/pipeline/runs",
        "/api/v1/data-quality",
    ],
)
def test_views_return_503_when_database_is_down(path):
    app.dependency_overrides[get_repository] = lambda: BrokenRepository()
    try:
        with TestClient(app) as c:
            response = c.get(path)
        assert response.status_code == 503
        assert response.json()["error"] == "database_unavailable"
    finally:
        app.dependency_overrides.clear()
