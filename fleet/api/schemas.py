"""Pydantic v2 response models for the fleet API.

Money columns are NUMERIC in PostgreSQL and arrive as `Decimal`; they are declared
as `float` here so they serialise as JSON numbers (Pydantic converts in lax mode).
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


class ErrorResponse(BaseModel):
    """Body of every non-2xx response."""

    error: str = Field(description="Machine-readable code, e.g. not_found, validation_error")
    message: str = Field(description="Human-readable explanation")
    request_id: str | None = Field(default=None, description="Same as the X-Request-ID header")
    details: list[dict[str, Any]] | None = Field(
        default=None, description="Per-field problems for validation errors"
    )


# ---------------------------------------------------------------------------- health
class HealthResponse(BaseModel):
    status: str = Field(description="ok | degraded")
    database: str = Field(description="ok | unavailable")
    service: str = "fleet-api"
    version: str
    checked_at: datetime


# ---------------------------------------------------------------------------- fleet
class FleetWindow(BaseModel):
    """The most recent event-time window from realtime_vehicle_metrics."""

    window_start: datetime
    window_end: datetime
    business_date: date
    window_hours: float = Field(description="Window length in simulated hours")
    vehicles_reporting: int
    active_vehicles: int
    idle_vehicles: int
    idle_ratio: float
    event_count: int
    trips_completed: int
    events_per_hour: float = Field(description="event_count / window_hours")
    trips_per_hour: float = Field(description="trips_completed / window_hours")
    total_earnings: float
    avg_fare: float | None
    updated_at: datetime


class FleetDayTotals(BaseModel):
    """Sums over all realtime windows of the latest window's business_date."""

    business_date: date
    windows: int
    event_count: int
    trips_completed: int
    total_earnings: float


class FleetSummaryResponse(BaseModel):
    data_available: bool = Field(description="False until the speed layer has written a window")
    message: str | None = None
    latest_window: FleetWindow | None
    business_day_totals: FleetDayTotals | None
    last_ingestion_ts: datetime | None = Field(
        description="Latest real Kafka ingestion time in stream_events"
    )
    last_event_timestamp: datetime | None = Field(
        description="Latest simulated event time in stream_events"
    )
    open_alerts: int


class ZoneMetrics(BaseModel):
    zone: str
    windows: int
    event_count: int
    trips_completed: int
    total_earnings: float
    avg_fare: float | None = Field(description="total_earnings / trips_completed")


class FleetZonesResponse(BaseModel):
    business_date: date | None
    zone_count: int
    zones: list[ZoneMetrics]


# ---------------------------------------------------------------------------- alerts
class Alert(BaseModel):
    alert_id: int
    alert_type: str
    severity: str
    vehicle_id: str | None
    business_date: date | None
    created_at: datetime
    message: str
    status: str
    resolved_at: datetime | None


class AlertListResponse(BaseModel):
    count: int
    limit: int
    alerts: list[Alert]


# ---------------------------------------------------------------------------- vehicles
class VehicleLatestEvent(BaseModel):
    event_id: UUID
    trip_id: str | None
    driver_id: str
    status: str
    latitude: float
    longitude: float
    zone: str
    speed: float
    fare: float
    event_timestamp: datetime
    business_date: date
    ingestion_ts: datetime


class VehicleStreamStats(BaseModel):
    """Speed-layer totals for the vehicle's latest business date."""

    business_date: date
    event_count: int
    trips_completed: int
    earnings: float


class DailyProfitabilityRow(BaseModel):
    vehicle_id: str
    business_date: date
    trips: int
    event_count: int
    on_trip_event_count: int
    stream_distance_km: float
    distance_km: float
    earnings: float
    fuel_cost: float
    maintenance_cost: float
    total_operating_cost: float
    estimated_profit: float
    utilization_rate: float
    service_flag: bool
    profitability_status: str
    computed_at: datetime


class VehicleDetailResponse(BaseModel):
    vehicle_id: str
    latest_event: VehicleLatestEvent | None
    stream_stats: VehicleStreamStats | None
    daily_profitability: list[DailyProfitabilityRow] = Field(
        description="Up to 7 most recent batch days, newest first"
    )
    open_alerts: list[Alert]


# ---------------------------------------------------------------------------- reports
class PipelineRun(BaseModel):
    run_id: int
    pipeline_name: str
    business_date: date | None
    status: str
    started_at: datetime
    finished_at: datetime | None
    rows_read: int | None
    rows_written: int | None
    rows_rejected: int | None
    error_message: str | None
    airflow_run_id: str | None


class DailyReportSummary(BaseModel):
    vehicle_count: int
    total_trips: int
    total_earnings: float
    total_operating_cost: float
    total_estimated_profit: float
    avg_utilization_rate: float
    status_counts: dict[str, int] = Field(description="Vehicles per profitability_status")


class DailyReportResponse(BaseModel):
    business_date: date
    summary: DailyReportSummary
    vehicles: list[DailyProfitabilityRow] = Field(
        description="Per-vehicle rows ordered by estimated_profit ascending (worst first)"
    )
    latest_pipeline_run: PipelineRun | None


# ---------------------------------------------------------------------------- dashboard views
class FleetWindowHistoryResponse(BaseModel):
    """Recent fleet windows, oldest first (for time-series charts)."""

    count: int
    windows: list[FleetWindow]


class VehicleState(BaseModel):
    """Latest known event of one vehicle (for the fleet map / vehicle picker)."""

    vehicle_id: str
    driver_id: str
    status: str
    latitude: float
    longitude: float
    zone: str
    speed: float
    event_timestamp: datetime
    ingestion_ts: datetime


class VehicleStateListResponse(BaseModel):
    count: int
    status_counts: dict[str, int] = Field(description="Vehicles per latest status")
    vehicles: list[VehicleState]


class ReportDate(BaseModel):
    business_date: date
    vehicle_count: int
    total_estimated_profit: float
    status_counts: dict[str, int]


class ReportDateListResponse(BaseModel):
    count: int
    reports: list[ReportDate] = Field(description="Newest business date first")


class PipelineRunListResponse(BaseModel):
    count: int
    runs: list[PipelineRun] = Field(description="Newest run first")


class RejectedEvent(BaseModel):
    rejected_id: int
    reason: str
    kafka_partition: int
    kafka_offset: int
    ingestion_ts: datetime
    raw_value_preview: str | None


class PartitionStats(BaseModel):
    kafka_partition: int
    event_count: int
    vehicle_count: int


class BatchQualityStat(BaseModel):
    pipeline_name: str
    batch_ref: str
    business_date: date | None
    records_total: int
    records_valid: int
    records_rejected: int
    rule_failures: dict[str, Any]
    recorded_at: datetime


class DataQualityResponse(BaseModel):
    stream_records_total: int
    stream_records_valid: int
    stream_records_rejected: int
    stream_rejection_rate: float = Field(description="rejected / total (0..1)")
    rejected_by_reason: dict[str, int]
    kafka_partitions: list[PartitionStats]
    recent_rejected: list[RejectedEvent]
    recent_batch_checks: list[BatchQualityStat]
