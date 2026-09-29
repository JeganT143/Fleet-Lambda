"""HTTP clients used by the Streamlit dashboard.

The dashboard never touches PostgreSQL, Kafka or Spark directly:
  * business and pipeline data comes from the FastAPI serving layer (`ApiClient`);
  * orchestration (DAG run states, re-running a business date) goes through the
    Airflow REST API (`AirflowClient`), authenticated with the Airflow admin user.
"""

from __future__ import annotations

from typing import Any

import httpx

from fleet.common.config import Settings, load_settings

DAILY_BATCH_DAG = "fleet_daily_batch"
STREAM_ALERTS_DAG = "fleet_stream_alerts"


class ApiError(Exception):
    """A service could not be reached or returned an unexpected status."""


class ApiClient:
    def __init__(
        self, base_url: str, timeout: float = 5.0, transport: httpx.BaseTransport | None = None
    ) -> None:
        self._http = httpx.Client(base_url=base_url, timeout=timeout, transport=transport)

    def get(self, path: str, params: dict[str, Any] | None = None, allow_404: bool = False):
        """GET a JSON document. Returns None for 404 when allow_404 is set."""
        params = {k: v for k, v in (params or {}).items() if v not in (None, "")}
        try:
            response = self._http.get(path, params=params)
        except httpx.HTTPError as exc:
            raise ApiError(f"API unreachable ({type(exc).__name__})") from exc
        if response.status_code == 404 and allow_404:
            return None
        if response.status_code >= 400:
            try:
                message = response.json().get("message", response.text)
            except ValueError:
                message = response.text
            raise ApiError(f"API {response.status_code} on {path}: {message}")
        return response.json()


class AirflowClient:
    def __init__(
        self,
        base_url: str,
        user: str,
        password: str,
        timeout: float = 5.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._http = httpx.Client(
            base_url=base_url, auth=(user, password), timeout=timeout, transport=transport
        )

    def _request(self, method: str, path: str, **kwargs):
        try:
            response = self._http.request(method, path, **kwargs)
        except httpx.HTTPError as exc:
            raise ApiError(f"Airflow unreachable ({type(exc).__name__})") from exc
        if response.status_code >= 400:
            raise ApiError(f"Airflow {response.status_code} on {path}: {response.text[:200]}")
        return response.json()

    def health(self) -> dict[str, Any]:
        """Scheduler / metadatabase health (this endpoint needs no login)."""
        return self._request("GET", "/health")

    def dag_runs(self, dag_id: str, limit: int = 10) -> list[dict[str, Any]]:
        body = self._request(
            "GET",
            f"/api/v1/dags/{dag_id}/dagRuns",
            params={"order_by": "-execution_date", "limit": limit},
        )
        return body.get("dag_runs", [])

    def dag_run(self, dag_id: str, dag_run_id: str) -> dict[str, Any]:
        return self._request("GET", f"/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}")

    def task_states(self, dag_id: str, dag_run_id: str) -> list[dict[str, Any]]:
        body = self._request("GET", f"/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances")
        return body.get("task_instances", [])

    def trigger(self, dag_id: str, conf: dict[str, Any], dag_run_id: str) -> dict[str, Any]:
        return self._request(
            "POST", f"/api/v1/dags/{dag_id}/dagRuns", json={"dag_run_id": dag_run_id, "conf": conf}
        )


def make_clients(settings: Settings | None = None) -> tuple[ApiClient, AirflowClient]:
    settings = settings or load_settings()
    return (
        ApiClient(settings.api_base_url),
        AirflowClient(
            settings.airflow_base_url, settings.airflow_admin_user, settings.airflow_admin_password
        ),
    )
