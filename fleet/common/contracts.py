"""Shared data contracts (AGENTS.md section 8).

This module is the single source of truth for field names, valid values and
validation bounds. Python validators, the Spark streaming job and the batch job
all import these constants instead of redefining them.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# 8.1 Streaming event contract (Kafka topic `vehicle.telemetry`, JSON value,
#     message key = vehicle_id)
# ---------------------------------------------------------------------------
EVENT_FIELDS: tuple[str, ...] = (
    "event_id",  # UUID string, unique per event
    "trip_id",  # string; null while the vehicle is idle
    "driver_id",  # e.g. "D007"
    "vehicle_id",  # e.g. "V007"
    "latitude",  # float, degrees
    "longitude",  # float, degrees
    "speed",  # float, km/h
    "status",  # one of VALID_STATUSES
    "fare",  # float, INR; > 0 only on the final on_trip event of a trip
    "timestamp",  # ISO-8601 UTC string, SIMULATED event time
)

# Fields that must be present and non-null. trip_id is nullable (idle vehicles).
EVENT_REQUIRED_FIELDS: tuple[str, ...] = tuple(f for f in EVENT_FIELDS if f != "trip_id")

STATUS_IDLE = "idle"
STATUS_ENROUTE = "enroute"
STATUS_ON_TRIP = "on_trip"
VALID_STATUSES: tuple[str, ...] = (STATUS_IDLE, STATUS_ENROUTE, STATUS_ON_TRIP)

# Fare semantics: a trip's full fare is reported exactly once, on the LAST
# on_trip event of that trip. All other events carry fare = 0.
#   earnings        = SUM(fare)
#   trips completed = COUNT(fare > 0)
#   average fare    = AVG(fare) WHERE fare > 0
# This keeps every metric a simple, streaming-friendly aggregation.

VEHICLE_ID_PATTERN = r"^V[0-9]{3}$"
DRIVER_ID_PATTERN = r"^D[0-9]{3}$"

LATITUDE_RANGE = (-90.0, 90.0)
LONGITUDE_RANGE = (-180.0, 180.0)
MAX_SPEED_KMH = 200.0
MAX_FARE = 10_000.0


def vehicle_id(n: int) -> str:
    """Canonical vehicle id for fleet member n (1-based): 7 -> 'V007'."""
    return f"V{n:03d}"


def driver_id(n: int) -> str:
    return f"D{n:03d}"


# ---------------------------------------------------------------------------
# 8.2 Batch expense contract (CSV in the landing directory)
#     <LANDING_DIR>/expenses/<business_date>/vehicle_expenses.csv
# ---------------------------------------------------------------------------
EXPENSE_FIELDS: tuple[str, ...] = (
    "vehicle_id",
    "fuel_cost",  # INR, >= 0
    "maintenance_cost",  # INR, >= 0
    "distance_covered",  # km (odometer), >= 0
    "service_flag",  # "true" / "false": vehicle was serviced that day
    "business_date",  # YYYY-MM-DD, must equal the directory date
)

EXPENSE_FILE_NAME = "vehicle_expenses.csv"
MAX_DAILY_COST = 100_000.0
MAX_DAILY_DISTANCE_KM = 2_000.0


def expense_file_path(landing_dir: str, business_date: str) -> str:
    return f"{landing_dir.rstrip('/')}/expenses/{business_date}/{EXPENSE_FILE_NAME}"


# ---------------------------------------------------------------------------
# Profitability statuses (daily_vehicle_profitability.profitability_status)
# ---------------------------------------------------------------------------
PROFITABLE = "profitable"
WATCH = "watch"
UNPROFITABLE = "unprofitable"

# ---------------------------------------------------------------------------
# Alerts
# ---------------------------------------------------------------------------
ALERT_NO_DATA = "no_stream_data"
ALERT_VEHICLE_IDLE = "vehicle_idle"
ALERT_LOW_PROFIT = "low_profitability"
ALERT_SEVERITIES: tuple[str, ...] = ("info", "warning", "critical")
ALERT_STATUSES: tuple[str, ...] = ("open", "resolved")
