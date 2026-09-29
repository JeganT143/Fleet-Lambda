"""FastAPI serving layer: read-only JSON API over the fleet PostgreSQL tables.

Run: uvicorn fleet.api.main:app --host 0.0.0.0 --port 8000   (compose service `api`)

The API presents what the streaming and batch layers have stored. It never runs
Spark or Airflow and never computes business values itself (profitability,
utilisation, statuses); it only reads them and adds simple SQL aggregates.
"""

from __future__ import annotations

import re
import time
import uuid
from datetime import UTC, date, datetime
from enum import Enum
from typing import Annotated, Any

import psycopg
from fastapi import Depends, FastAPI, HTTPException, Path, Query, Request, Response, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from fleet.api.repository import FleetRepository, get_repository
from fleet.api.schemas import (
    Alert,
    AlertListResponse,
    BatchQualityStat,
    DailyProfitabilityRow,
    DailyReportResponse,
    DailyReportSummary,
    DataQualityResponse,
    ErrorResponse,
    FleetDayTotals,
    FleetSummaryResponse,
    FleetWindow,
    FleetWindowHistoryResponse,
    FleetZonesResponse,
    HealthResponse,
    PartitionStats,
    PipelineRun,
    PipelineRunListResponse,
    RejectedEvent,
    ReportDate,
    ReportDateListResponse,
    VehicleDetailResponse,
    VehicleLatestEvent,
    VehicleState,
    VehicleStateListResponse,
    VehicleStreamStats,
    ZoneMetrics,
)
from fleet.common.contracts import (
    ALERT_LOW_PROFIT,
    ALERT_NO_DATA,
    ALERT_STATUSES,
    ALERT_VEHICLE_IDLE,
    PROFITABLE,
    UNPROFITABLE,
    VEHICLE_ID_PATTERN,
    WATCH,
)
from fleet.common.logs import get_logger

API_VERSION = "0.1.0"
REQUEST_ID_HEADER = "X-Request-ID"
_CLIENT_REQUEST_ID = re.compile(r"^[A-Za-z0-9._-]{1,64}$")

log = get_logger("api")

# Allowed filter values come from the shared contracts, not from this module.
AlertStatus = Enum("AlertStatus", {s: s for s in ALERT_STATUSES}, type=str)
AlertType = Enum(
    "AlertType",
    {t: t for t in (ALERT_NO_DATA, ALERT_VEHICLE_IDLE, ALERT_LOW_PROFIT)},
    type=str,
)

DB_UNAVAILABLE_ERRORS = (psycopg.OperationalError, psycopg.InterfaceError)

TAGS = [
    {"name": "health", "description": "Liveness and database connectivity."},
    {"name": "fleet", "description": "Real-time fleet metrics from the speed layer."},
    {"name": "vehicles", "description": "Per-vehicle view combining speed and batch layers."},
    {"name": "alerts", "description": "Threshold alerts raised by the pipelines."},
    {"name": "reports", "description": "Daily reconciliation (batch layer) reports."},
    {"name": "operations", "description": "Pipeline runs and data-quality statistics."},
]

ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    422: {"model": ErrorResponse, "description": "Invalid path or query parameter"},
    503: {"model": ErrorResponse, "description": "Database unavailable"},
}
NOT_FOUND = {404: {"model": ErrorResponse, "description": "Not found"}}

app = FastAPI(
    title="Fleet Operations API",
    version=API_VERSION,
    description="Serving layer of the ride-hailing Lambda pipeline (read-only).",
    openapi_tags=TAGS,
)

RepoDep = Annotated[FleetRepository, Depends(get_repository)]


# ============================================================================ middleware
def _request_id(request: Request) -> str:
    return getattr(request.state, "request_id", None) or "-"


def _error(
    request: Request,
    status_code: int,
    error: str,
    message: str,
    details: list[dict[str, Any]] | None = None,
) -> JSONResponse:
    body = ErrorResponse(
        error=error, message=message, request_id=_request_id(request), details=details
    )
    return JSONResponse(status_code=status_code, content=body.model_dump(mode="json"))


@app.middleware("http")
async def request_logging(request: Request, call_next):
    """One JSON log line per request, an X-Request-ID header, and the 500 fallback.

    Unexpected exceptions are turned into a generic 500 here (not in an
    `Exception` handler) so that the request is still logged with its status,
    duration and request id, and the traceback goes to the log, never the client.
    """
    incoming = request.headers.get(REQUEST_ID_HEADER, "")
    request_id = incoming if _CLIENT_REQUEST_ID.match(incoming) else uuid.uuid4().hex
    request.state.request_id = request_id
    started = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        log.exception(
            "unhandled error",
            extra={"fields": {"request_id": request_id, "path": request.url.path}},
        )
        response = _error(request, 500, "internal_error", "Internal server error")
    duration_ms = round((time.perf_counter() - started) * 1000, 2)
    response.headers[REQUEST_ID_HEADER] = request_id
    log.info(
        "request",
        extra={
            "fields": {
                "method": request.method,
                "path": request.url.path,
                "query": request.url.query or None,
                "status_code": response.status_code,
                "duration_ms": duration_ms,
                "request_id": request_id,
            }
        },
    )
    return response


# ============================================================================ exception handlers
@app.exception_handler(psycopg.OperationalError)
@app.exception_handler(psycopg.InterfaceError)
async def database_unavailable(request: Request, exc: psycopg.Error) -> JSONResponse:
    log.error(
        "database unavailable",
        extra={"fields": {"request_id": _request_id(request), "error": type(exc).__name__}},
    )
    return _error(request, 503, "database_unavailable", "Database is unavailable, retry later")


@app.exception_handler(StarletteHTTPException)
async def http_error(request: Request, exc: StarletteHTTPException) -> JSONResponse:
    codes = {404: "not_found", 405: "method_not_allowed"}
    return _error(request, exc.status_code, codes.get(exc.status_code, "http_error"), exc.detail)


@app.exception_handler(RequestValidationError)
async def validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
    details = [
        {"loc": list(e.get("loc", ())), "msg": str(e.get("msg")), "type": str(e.get("type"))}
        for e in exc.errors()
    ]
    return _error(request, 422, "validation_error", "Invalid request parameters", details)


# ============================================================================ helpers
def _per_hour(count: int, hours: float) -> float:
    return round(count / hours, 2) if hours > 0 else 0.0


def _fleet_window(row: dict[str, Any]) -> FleetWindow:
    hours = (row["window_end"] - row["window_start"]).total_seconds() / 3600
    return FleetWindow(
        **row,
        window_hours=hours,
        events_per_hour=_per_hour(row["event_count"], hours),
        trips_per_hour=_per_hour(row["trips_completed"], hours),
    )


# ============================================================================ endpoints
@app.get(
    "/health",
    tags=["health"],
    summary="Service and database health",
    response_model=HealthResponse,
    responses={503: {"model": HealthResponse, "description": "Database unreachable"}},
)
def health(response: Response, repo: RepoDep) -> HealthResponse:
    database = "ok"
    try:
        repo.ping()
    except psycopg.Error as exc:
        log.warning("health: database unavailable", extra={"fields": {"error": str(exc)}})
        database = "unavailable"
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return HealthResponse(
        status="ok" if database == "ok" else "degraded",
        database=database,
        version=API_VERSION,
        checked_at=datetime.now(UTC),
    )


@app.get(
    "/api/v1/fleet/summary",
    tags=["fleet"],
    summary="Latest real-time fleet window plus business-day totals",
    response_model=FleetSummaryResponse,
    responses={503: ERROR_RESPONSES[503]},
)
def fleet_summary(repo: RepoDep) -> FleetSummaryResponse:
    """Latest `realtime_vehicle_metrics` window, totals for its business date,
    last ingestion time and the number of open alerts.

    No data yet (pipeline just started) is a normal state of the system, not an
    error and not a missing resource, so it returns **200** with
    `data_available=false`, null window/totals and zero counts. Clients can poll one
    URL and render "waiting for data" instead of handling 404/500.
    """
    window = repo.latest_fleet_window()
    ingestion = repo.last_ingestion()
    open_alerts = repo.count_open_alerts()
    if window is None:
        return FleetSummaryResponse(
            data_available=False,
            message="No real-time metrics yet: the streaming job has not written a window.",
            latest_window=None,
            business_day_totals=None,
            last_ingestion_ts=ingestion["last_ingestion_ts"],
            last_event_timestamp=ingestion["last_event_timestamp"],
            open_alerts=open_alerts,
        )
    totals = repo.fleet_day_totals(window["business_date"])
    return FleetSummaryResponse(
        data_available=True,
        latest_window=_fleet_window(window),
        business_day_totals=FleetDayTotals(business_date=window["business_date"], **totals),
        last_ingestion_ts=ingestion["last_ingestion_ts"],
        last_event_timestamp=ingestion["last_event_timestamp"],
        open_alerts=open_alerts,
    )


@app.get(
    "/api/v1/fleet/windows",
    tags=["fleet"],
    summary="Recent real-time fleet windows, oldest first (for charts)",
    response_model=FleetWindowHistoryResponse,
    responses=ERROR_RESPONSES,
)
def fleet_windows(
    repo: RepoDep, limit: Annotated[int, Query(ge=1, le=500)] = 48
) -> FleetWindowHistoryResponse:
    rows = list(reversed(repo.fleet_windows(limit)))
    return FleetWindowHistoryResponse(count=len(rows), windows=[_fleet_window(r) for r in rows])


@app.get(
    "/api/v1/fleet/zones",
    tags=["fleet"],
    summary="Earnings, trips and events per zone for one business date",
    response_model=FleetZonesResponse,
    responses=ERROR_RESPONSES,
)
def fleet_zones(
    repo: RepoDep,
    business_date: Annotated[
        date | None,
        Query(description="YYYY-MM-DD; defaults to the latest date in realtime_zone_metrics"),
    ] = None,
) -> FleetZonesResponse:
    """Per-zone sums over all windows of the date. A date without data returns 200
    with an empty list (it is a filter result, not a missing resource)."""
    day = business_date or repo.latest_zone_business_date()
    zones = [ZoneMetrics(**r) for r in repo.zone_metrics(day)] if day else []
    return FleetZonesResponse(business_date=day, zone_count=len(zones), zones=zones)


@app.get(
    "/api/v1/vehicles",
    tags=["vehicles"],
    summary="Latest position and status of every vehicle",
    response_model=VehicleStateListResponse,
    responses={503: ERROR_RESPONSES[503]},
)
def vehicle_states(repo: RepoDep) -> VehicleStateListResponse:
    vehicles = [VehicleState(**r) for r in repo.vehicle_states()]
    counts: dict[str, int] = {}
    for v in vehicles:
        counts[v.status] = counts.get(v.status, 0) + 1
    return VehicleStateListResponse(count=len(vehicles), status_counts=counts, vehicles=vehicles)


@app.get(
    "/api/v1/vehicles/{vehicle_id}",
    tags=["vehicles"],
    summary="Latest position/status, stream stats, last 7 daily results and open alerts",
    response_model=VehicleDetailResponse,
    responses={**ERROR_RESPONSES, **NOT_FOUND},
)
def vehicle_detail(
    repo: RepoDep,
    vehicle_id: Annotated[
        str, Path(pattern=VEHICLE_ID_PATTERN, description="Vehicle id, e.g. V007")
    ],
) -> VehicleDetailResponse:
    latest = repo.latest_vehicle_event(vehicle_id)
    daily = repo.vehicle_daily_profitability(vehicle_id, limit=7)
    if latest is None and not daily:
        raise HTTPException(status_code=404, detail=f"Vehicle {vehicle_id} has never been seen")
    stats = None
    if latest is not None:
        day = latest["business_date"]
        stats = VehicleStreamStats(business_date=day, **repo.vehicle_stream_stats(vehicle_id, day))
    return VehicleDetailResponse(
        vehicle_id=vehicle_id,
        latest_event=VehicleLatestEvent(**latest) if latest else None,
        stream_stats=stats,
        daily_profitability=[DailyProfitabilityRow(**r) for r in daily],
        open_alerts=[Alert(**r) for r in repo.vehicle_open_alerts(vehicle_id)],
    )


@app.get(
    "/api/v1/alerts",
    tags=["alerts"],
    summary="List alerts, newest first",
    response_model=AlertListResponse,
    responses=ERROR_RESPONSES,
)
def list_alerts(
    repo: RepoDep,
    status_filter: Annotated[AlertStatus | None, Query(alias="status")] = None,
    alert_type: AlertType | None = None,
    vehicle_id: Annotated[str | None, Query(pattern=VEHICLE_ID_PATTERN)] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
) -> AlertListResponse:
    rows = repo.list_alerts(
        status=status_filter.value if status_filter else None,
        alert_type=alert_type.value if alert_type else None,
        vehicle_id=vehicle_id,
        limit=limit,
    )
    return AlertListResponse(count=len(rows), limit=limit, alerts=[Alert(**r) for r in rows])


@app.get(
    "/api/v1/reports/daily",
    tags=["reports"],
    summary="Business dates that have a daily report, newest first",
    response_model=ReportDateListResponse,
    responses={503: ERROR_RESPONSES[503]},
)
def report_dates(repo: RepoDep) -> ReportDateListResponse:
    reports = [
        ReportDate(
            business_date=r["business_date"],
            vehicle_count=r["vehicle_count"],
            total_estimated_profit=r["total_estimated_profit"],
            status_counts={s: int(r[s]) for s in (PROFITABLE, WATCH, UNPROFITABLE)},
        )
        for r in repo.report_dates()
    ]
    return ReportDateListResponse(count=len(reports), reports=reports)


@app.get(
    "/api/v1/reports/daily/{business_date}",
    tags=["reports"],
    summary="Daily profitability report for one business date",
    response_model=DailyReportResponse,
    responses={**ERROR_RESPONSES, **NOT_FOUND},
)
def daily_report(
    repo: RepoDep,
    business_date: Annotated[date, Path(description="YYYY-MM-DD")],
) -> DailyReportResponse:
    """Summary + per-vehicle rows (worst profit first) from daily_vehicle_profitability,
    and the latest pipeline_runs entry for the date. 404 until the batch has run."""
    summary = repo.daily_report_summary(business_date)
    if summary is None:
        raise HTTPException(
            status_code=404, detail=f"No daily report for {business_date.isoformat()}"
        )
    run = repo.latest_pipeline_run(business_date)
    return DailyReportResponse(
        business_date=business_date,
        summary=DailyReportSummary(
            **summary, status_counts=repo.daily_report_status_counts(business_date)
        ),
        vehicles=[DailyProfitabilityRow(**r) for r in repo.daily_report_rows(business_date)],
        latest_pipeline_run=PipelineRun(**run) if run else None,
    )


@app.get(
    "/api/v1/pipeline/runs",
    tags=["operations"],
    summary="Recent batch pipeline runs (pipeline_runs), newest first",
    response_model=PipelineRunListResponse,
    responses=ERROR_RESPONSES,
)
def pipeline_runs(
    repo: RepoDep, limit: Annotated[int, Query(ge=1, le=500)] = 30
) -> PipelineRunListResponse:
    runs = [PipelineRun(**r) for r in repo.pipeline_runs(limit)]
    return PipelineRunListResponse(count=len(runs), runs=runs)


@app.get(
    "/api/v1/data-quality",
    tags=["operations"],
    summary="Stream validation totals, quarantine reasons, Kafka partitions, batch checks",
    response_model=DataQualityResponse,
    responses={503: ERROR_RESPONSES[503]},
)
def data_quality(repo: RepoDep) -> DataQualityResponse:
    totals = repo.stream_quality_totals()
    total = int(totals["records_total"])
    rejected = int(totals["records_rejected"])
    return DataQualityResponse(
        stream_records_total=total,
        stream_records_valid=int(totals["records_valid"]),
        stream_records_rejected=rejected,
        stream_rejection_rate=round(rejected / total, 5) if total else 0.0,
        rejected_by_reason=repo.rejected_by_reason(),
        kafka_partitions=[PartitionStats(**r) for r in repo.partition_stats()],
        recent_rejected=[RejectedEvent(**r) for r in repo.recent_rejected(10)],
        recent_batch_checks=[BatchQualityStat(**r) for r in repo.recent_batch_quality(10)],
    )
