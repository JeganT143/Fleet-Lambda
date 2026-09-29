"""Unit tests for the Streamlit dashboard.

The page tests run the real app script with Streamlit's AppTest. The dashboard's API
client talks to the real FastAPI app (in-process TestClient) backed by the fake
repository, so these tests also prove the UI and the API agree on field names.
Airflow is replaced by a small fake.
"""

from __future__ import annotations

import base64
import json
from pathlib import Path

import httpx
import pytest

pytest.importorskip("streamlit")

import streamlit as st  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from streamlit.testing.v1 import AppTest  # noqa: E402

import fleet.dashboard.client as dash_client  # noqa: E402
from fleet.api.main import app as api_app  # noqa: E402
from fleet.api.repository import get_repository  # noqa: E402
from fleet.dashboard.client import AirflowClient, ApiClient, ApiError  # noqa: E402
from tests.unit.test_api_views import ViewsFakeRepository  # noqa: E402

APP_FILE = str(Path(__file__).resolve().parents[2] / "fleet" / "dashboard" / "app.py")
PAGES = ["Overview", "Live Fleet", "Vehicles", "Daily Report", "Alerts", "Pipeline & Quality"]


# ============================================================================ clients
def _api(handler) -> ApiClient:
    return ApiClient("http://api", transport=httpx.MockTransport(handler))


def test_api_client_returns_json_and_drops_empty_params():
    seen = {}

    def handler(request):
        seen["url"] = str(request.url)
        return httpx.Response(200, json={"ok": True})

    assert _api(handler).get("/x", params={"a": 1, "b": None, "c": ""}) == {"ok": True}
    assert seen["url"] == "http://api/x?a=1"


def test_api_client_404_allowed_returns_none():
    assert _api(lambda r: httpx.Response(404, json={})).get("/x", allow_404=True) is None


def test_api_client_error_uses_api_message():
    client = _api(lambda r: httpx.Response(503, json={"message": "Database is unavailable"}))
    with pytest.raises(ApiError, match="503.*Database is unavailable"):
        client.get("/x")


def test_api_client_unreachable_raises_api_error():
    def handler(request):
        raise httpx.ConnectError("refused")

    with pytest.raises(ApiError, match="unreachable"):
        _api(handler).get("/x")


def test_airflow_trigger_sends_conf_and_basic_auth():
    seen = {}

    def handler(request):
        seen["auth"] = request.headers["authorization"]
        seen["path"] = request.url.path
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={"dag_run_id": "r1", "state": "queued"})

    airflow = AirflowClient(
        "http://airflow", "admin", "secret", transport=httpx.MockTransport(handler)
    )
    airflow.trigger("fleet_daily_batch", {"business_date": "2026-01-03"}, "r1")
    assert seen["path"] == "/api/v1/dags/fleet_daily_batch/dagRuns"
    assert seen["body"] == {"dag_run_id": "r1", "conf": {"business_date": "2026-01-03"}}
    assert seen["auth"] == "Basic " + base64.b64encode(b"admin:secret").decode()


# ============================================================================ pages
class FakeAirflow:
    def __init__(self, down: bool = False) -> None:
        self.down = down
        self.triggered: list[tuple] = []

    def _check(self):
        if self.down:
            raise ApiError("Airflow unreachable (ConnectError)")

    def health(self):
        self._check()
        return {"scheduler": {"status": "healthy"}, "metadatabase": {"status": "healthy"}}

    def dag_runs(self, dag_id, limit=10):
        self._check()
        return [
            {
                "dag_run_id": "scheduled__1",
                "state": "success",
                "conf": {},
                "start_date": "2026-09-29T08:00:00+00:00",
            }
        ]

    def dag_run(self, dag_id, dag_run_id):
        self._check()
        return {"dag_run_id": dag_run_id, "state": "success"}

    def task_states(self, dag_id, dag_run_id):
        self._check()
        return [{"task_id": f"t{i}", "state": "success"} for i in range(6)]

    def trigger(self, dag_id, conf, dag_run_id):
        self._check()
        self.triggered.append((dag_id, conf, dag_run_id))
        return {"dag_run_id": dag_run_id, "state": "queued"}


class DownApi:
    def get(self, path, params=None, allow_404=False):
        raise ApiError("API unreachable (ConnectError)")


@pytest.fixture
def fake_services(monkeypatch):
    """Route the dashboard to the real API (fake repository) and a fake Airflow."""
    repo = ViewsFakeRepository()
    api_app.dependency_overrides[get_repository] = lambda: repo
    api = ApiClient("http://testserver")
    api._http = TestClient(api_app)
    airflow = FakeAirflow()
    monkeypatch.setattr(dash_client, "make_clients", lambda settings=None: (api, airflow))
    st.cache_resource.clear()
    yield repo, airflow
    api_app.dependency_overrides.clear()
    st.cache_resource.clear()


def _open(page: str) -> AppTest:
    at = AppTest.from_file(APP_FILE, default_timeout=60).run()
    if page != PAGES[0]:
        at.sidebar.radio[0].set_value(page).run()
    return at


@pytest.mark.parametrize("page", PAGES)
def test_every_page_renders_without_errors(fake_services, page):
    at = _open(page)
    assert not at.exception, at.exception
    assert not at.error, [e.value for e in at.error]
    assert at.title[0].value


def test_overview_shows_component_health(fake_services):
    at = _open("Overview")
    metrics = {m.label: m.value for m in at.metric}
    assert metrics["API"] == "OK"
    assert metrics["PostgreSQL"] == "OK"
    assert metrics["Airflow scheduler"] == "HEALTHY"
    assert metrics["Open alerts"] == "4"


def test_daily_report_shows_summary_from_api(fake_services):
    at = _open("Daily Report")
    metrics = {m.label: m.value for m in at.metric}
    assert metrics["Vehicles"] == "2"
    assert metrics["Estimated profit"] == "₹2,400"
    assert metrics["Profitable"] == "1"
    assert metrics["Unprofitable"] == "1"


def test_rerun_button_triggers_dag_and_confirms_idempotency(fake_services):
    _, airflow = fake_services
    at = _open("Pipeline & Quality")
    [button] = [b for b in at.button if b.label == "Re-run batch for this date"]
    button.click().run()
    assert not at.exception, at.exception
    [(dag_id, conf, run_id)] = airflow.triggered
    assert dag_id == "fleet_daily_batch"
    assert conf == {"business_date": "2026-01-03"}
    assert run_id.startswith("dashboard_rerun_2026-01-03_")
    assert any("idempotent" in s.value for s in at.success)


def test_airflow_down_shows_warning_not_crash(fake_services):
    _, airflow = fake_services
    airflow.down = True
    at = _open("Pipeline & Quality")
    assert not at.exception, at.exception
    assert any("Airflow REST API not available" in w.value for w in at.warning)


def test_api_down_shows_clear_error(monkeypatch):
    monkeypatch.setattr(
        dash_client, "make_clients", lambda settings=None: (DownApi(), FakeAirflow())
    )
    st.cache_resource.clear()
    try:
        at = AppTest.from_file(APP_FILE, default_timeout=60).run()
        assert not at.exception, at.exception
        assert any("API is not reachable" in e.value for e in at.error)
    finally:
        st.cache_resource.clear()
