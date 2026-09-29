"""Pure alert rules: thresholds, severities and de-duplication keys (no database).

Alert types (docs/architecture.md 5.6):

    no_stream_data     no event INGESTED for more than ALERT_NO_DATA_SECONDS (real time)
    vehicle_idle       a vehicle idle for at least ALERT_IDLE_MINUTES (SIMULATED time),
                       measured against the latest fleet event time
    low_profitability  a vehicle-day with estimated_profit < ALERT_MIN_DAILY_PROFIT

Severities (documented choice):

    no_stream_data     critical  - the whole speed layer is blind
    vehicle_idle       warning   - idle >= threshold
                       critical  - idle >= IDLE_CRITICAL_FACTOR x threshold (6 h by default)
    low_profitability  warning   - profit below the threshold
                       critical  - loss deeper than CRITICAL_LOSS_MARGIN below the threshold
                                   (INR 1,000 by default, e.g. a workshop day)

De-duplication: the alerts table has a UNIQUE dedup_key, and each key names ONE
occurrence of a condition, so re-evaluating it (every minute, Airflow retries, batch
re-runs) never creates a second row:

    no_stream_data:<last ingestion ts>          one alert per outage
    vehicle_idle:<vehicle>:<idle-since ts>      one alert per idle episode
    low_profitability:<vehicle>:<business date> one alert per vehicle-day
"""

from __future__ import annotations

from datetime import date, datetime

from fleet.common.contracts import ALERT_LOW_PROFIT, ALERT_NO_DATA, ALERT_VEHICLE_IDLE

WARNING = "warning"
CRITICAL = "critical"

IDLE_CRITICAL_FACTOR = 3.0
CRITICAL_LOSS_MARGIN = 1000.0


# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------
def no_data_breached(seconds_since_last_ingestion: float, threshold_seconds: float) -> bool:
    """True when the last ingestion is OLDER than the threshold."""
    return seconds_since_last_ingestion > threshold_seconds


def idle_breached(idle_minutes: float, threshold_minutes: float) -> bool:
    """True when the vehicle has been idle for AT LEAST the threshold."""
    return idle_minutes >= threshold_minutes


def low_profit_breached(estimated_profit: float, min_daily_profit: float) -> bool:
    """True when the profit is strictly BELOW the threshold."""
    return estimated_profit < min_daily_profit


# ---------------------------------------------------------------------------
# Severities
# ---------------------------------------------------------------------------
def no_data_severity() -> str:
    return CRITICAL


def idle_severity(idle_minutes: float, threshold_minutes: float) -> str:
    return CRITICAL if idle_minutes >= IDLE_CRITICAL_FACTOR * threshold_minutes else WARNING


def low_profit_severity(estimated_profit: float, min_daily_profit: float) -> str:
    return CRITICAL if estimated_profit < min_daily_profit - CRITICAL_LOSS_MARGIN else WARNING


# ---------------------------------------------------------------------------
# De-duplication keys
# ---------------------------------------------------------------------------
def _iso(ts: datetime) -> str:
    return ts.isoformat(timespec="milliseconds")


def no_data_key(last_ingestion: datetime) -> str:
    return f"{ALERT_NO_DATA}:{_iso(last_ingestion)}"


def idle_key(vehicle_id: str, idle_since: datetime) -> str:
    return f"{ALERT_VEHICLE_IDLE}:{vehicle_id}:{_iso(idle_since)}"


def low_profit_key(vehicle_id: str, business_date: date | str) -> str:
    bd = business_date.isoformat() if isinstance(business_date, date) else business_date
    return f"{ALERT_LOW_PROFIT}:{vehicle_id}:{bd}"
