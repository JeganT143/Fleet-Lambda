"""SQL-backed alert evaluation. Called by the Airflow DAGs:

    fleet_stream_alerts (every minute)  -> check_no_stream_data, check_idle_vehicles
    fleet_daily_batch   (per date)      -> raise_low_profitability_alerts

Every check (1) reads the current state, (2) applies the pure rules in rules.py,
(3) inserts alerts with ON CONFLICT (dedup_key) DO NOTHING, so a condition that is
still true on the next run is not raised again, and (4) resolves open alerts whose
condition is gone (status='resolved', resolved_at=now()).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

import psycopg

from fleet.alerts import rules
from fleet.common.config import Settings, load_settings
from fleet.common.contracts import (
    ALERT_LOW_PROFIT,
    ALERT_NO_DATA,
    ALERT_VEHICLE_IDLE,
    STATUS_IDLE,
)
from fleet.common.db import connect
from fleet.common.logs import get_logger

log = get_logger("alerts")


@dataclass(frozen=True)
class Alert:
    alert_type: str
    severity: str
    message: str
    dedup_key: str
    vehicle_id: str | None = None
    business_date: date | None = None


INSERT_SQL = """
INSERT INTO alerts (alert_type, severity, vehicle_id, business_date, message, dedup_key)
VALUES (%(alert_type)s, %(severity)s, %(vehicle_id)s, %(business_date)s, %(message)s,
        %(dedup_key)s)
ON CONFLICT (dedup_key) DO NOTHING
RETURNING alert_id
"""


def insert_alert(conn: psycopg.Connection, alert: Alert) -> bool:
    """Insert unless the same dedup_key exists. Returns True if a new row was created."""
    row = conn.execute(INSERT_SQL, alert.__dict__).fetchone()
    if row is not None:
        log.warning(
            "alert raised",
            extra={"fields": {**alert.__dict__, "business_date": str(alert.business_date)}},
        )
    return row is not None


def _resolve(conn: psycopg.Connection, where: str, params: dict) -> int:
    rows = conn.execute(
        "UPDATE alerts SET status = 'resolved', resolved_at = now()"
        f" WHERE status = 'open' AND {where} RETURNING dedup_key",
        params,
    ).fetchall()
    for r in rows:
        key = r["dedup_key"] if isinstance(r, dict) else r[0]
        log.info("alert resolved", extra={"fields": {"dedup_key": key}})
    return len(rows)


# ---------------------------------------------------------------------------
# no_stream_data (real time)
# ---------------------------------------------------------------------------
LAST_INGESTION_SQL = """
SELECT ingestion_ts, business_date FROM stream_events ORDER BY ingestion_ts DESC LIMIT 1
"""

# When data flows again, open outage alerts are resolved - but only those whose
# business_date lies within RESOLVE_DAYS simulated days before the current one. (The
# producer resumes simulated time where it stopped, so a real outage's alert is always in
# that window.) This keeps live data and test data (2099 dates) from resolving each
# other's alerts.
RESOLVE_DAYS = 7


def check_no_stream_data(
    settings: Settings | None = None,
    now: datetime | None = None,
    last_ingestion: tuple[datetime, date] | None = None,
) -> dict:
    """Raise / resolve the no_stream_data alert.

    now             real 'now' (default: PostgreSQL now())
    last_ingestion  (ingestion_ts, business_date) of the latest ingested event
                    (default: read from stream_events). Injectable so tests can check the
                    logic deterministically while the live stream keeps flowing.
    """
    settings = settings or load_settings()
    threshold = settings.alert_no_data_seconds
    with connect(settings) as conn:
        if last_ingestion is None:
            row = conn.execute(LAST_INGESTION_SQL).fetchone()
            last_ingestion = (row["ingestion_ts"], row["business_date"]) if row else None
        now = now or conn.execute("SELECT now() AS now").fetchone()["now"]
        if last_ingestion is None:
            log.info("no stream events stored yet; nothing to check")
            return {"raised": 0, "resolved": 0, "silence_seconds": None}
        last_ts, last_date = last_ingestion
        silence = (now - last_ts).total_seconds()
        raised = resolved = 0
        if rules.no_data_breached(silence, threshold):
            raised = int(
                insert_alert(
                    conn,
                    Alert(
                        alert_type=ALERT_NO_DATA,
                        severity=rules.no_data_severity(),
                        message=(
                            f"No stream events ingested for {silence:.0f} s (threshold"
                            f" {threshold} s); last event ingested at"
                            f" {last_ts.astimezone(UTC).isoformat()}"
                        ),
                        dedup_key=rules.no_data_key(last_ts),
                        business_date=last_date,
                    ),
                )
            )
        else:
            # data is flowing again -> close the outage alert(s)
            resolved = _resolve(
                conn,
                "alert_type = %(t)s AND business_date BETWEEN %(since)s AND %(d)s",
                {
                    "t": ALERT_NO_DATA,
                    "d": last_date,
                    "since": last_date - timedelta(days=RESOLVE_DAYS),
                },
            )
    result = {"silence_seconds": round(silence, 1), "threshold": threshold, "raised": raised}
    result["resolved"] = resolved
    log.info("no-data check done", extra={"fields": result})
    return result


# ---------------------------------------------------------------------------
# vehicle_idle (simulated time)
# ---------------------------------------------------------------------------
# "Now" in simulated time (fleet_ts) = event time of the most recently ingested event
# (the producer owns the clock; ADR-003), unless a fleet_ts is passed in.
# For every vehicle seen in the simulated day before fleet_ts, three LATERAL lookups each
# read one row through the (vehicle_id, event_timestamp) index:
#   latest      its latest event (only vehicles whose latest status is idle are idle)
#   moved       its last NON-idle event
#   idle_start  its first event after `moved` = the first idle event of the current idle
#               episode (or its very first event if it never moved). This is the moment
#               it became idle, and it is the same on every run -> stable dedup_key.
IDLE_SQL = """
WITH now_sim AS (
    SELECT COALESCE(
        %(fleet_ts)s::timestamptz,
        (SELECT event_timestamp FROM stream_events ORDER BY ingestion_ts DESC LIMIT 1)
    ) AS fleet_ts
),
vehicles AS (
    SELECT DISTINCT s.vehicle_id
    FROM stream_events s, now_sim n
    WHERE s.business_date BETWEEN ((n.fleet_ts AT TIME ZONE 'UTC') - interval '1 day')::date
                              AND (n.fleet_ts AT TIME ZONE 'UTC')::date
      AND s.event_timestamp BETWEEN n.fleet_ts - interval '1 day' AND n.fleet_ts
)
SELECT v.vehicle_id, n.fleet_ts, latest.status, latest.business_date,
       idle_start.event_timestamp AS idle_since
FROM vehicles v
CROSS JOIN now_sim n
CROSS JOIN LATERAL (
    SELECT status, business_date FROM stream_events s
    WHERE s.vehicle_id = v.vehicle_id AND s.event_timestamp <= n.fleet_ts
    ORDER BY s.event_timestamp DESC LIMIT 1
) latest
LEFT JOIN LATERAL (
    SELECT event_timestamp FROM stream_events s
    WHERE s.vehicle_id = v.vehicle_id AND s.status <> 'idle' AND s.event_timestamp <= n.fleet_ts
    ORDER BY s.event_timestamp DESC LIMIT 1
) moved ON TRUE
LEFT JOIN LATERAL (
    SELECT event_timestamp FROM stream_events s
    WHERE s.vehicle_id = v.vehicle_id AND s.event_timestamp <= n.fleet_ts
      AND s.event_timestamp > COALESCE(moved.event_timestamp, '-infinity'::timestamptz)
    ORDER BY s.event_timestamp ASC LIMIT 1
) idle_start ON TRUE
ORDER BY v.vehicle_id
"""


def check_idle_vehicles(settings: Settings | None = None, fleet_ts: datetime | None = None) -> dict:
    """Raise / resolve vehicle_idle alerts.

    fleet_ts  simulated 'now' (default: event time of the latest ingested event).
              Injectable so tests can evaluate their own 2099 vehicles only.
    """
    settings = settings or load_settings()
    threshold = settings.alert_idle_minutes
    raised = 0
    breached: list[str] = []
    with connect(settings) as conn:
        rows = conn.execute(IDLE_SQL, {"fleet_ts": fleet_ts}).fetchall()
        idle_rows = [r for r in rows if r["status"] == STATUS_IDLE]
        current_keys = []
        for r in idle_rows:
            idle_minutes = (r["fleet_ts"] - r["idle_since"]).total_seconds() / 60.0
            key = rules.idle_key(r["vehicle_id"], r["idle_since"])
            current_keys.append(key)
            if not rules.idle_breached(idle_minutes, threshold):
                continue
            breached.append(r["vehicle_id"])
            raised += insert_alert(
                conn,
                Alert(
                    alert_type=ALERT_VEHICLE_IDLE,
                    severity=rules.idle_severity(idle_minutes, threshold),
                    message=(
                        f"Vehicle {r['vehicle_id']} idle for {idle_minutes:.0f} simulated"
                        f" minutes (threshold {threshold}); idle since"
                        f" {r['idle_since'].astimezone(UTC).isoformat()}"
                    ),
                    dedup_key=key,
                    vehicle_id=r["vehicle_id"],
                    business_date=r["business_date"],
                ),
            )
        # An idle alert of an evaluated vehicle whose episode is over (it moved again) is
        # resolved. Vehicles not evaluated in this run are left alone.
        resolved = _resolve(
            conn,
            "alert_type = %(t)s AND vehicle_id = ANY(%(vehicles)s)"
            " AND NOT (dedup_key = ANY(%(keys)s))",
            {
                "t": ALERT_VEHICLE_IDLE,
                "vehicles": [r["vehicle_id"] for r in rows],
                "keys": current_keys,
            },
        )
    result = {
        "vehicles": len(rows),
        "idle_vehicles": len(idle_rows),
        "over_threshold": breached,
        "threshold_minutes": threshold,
        "raised": raised,
        "resolved": resolved,
    }
    log.info("idle check done", extra={"fields": result})
    return result


# ---------------------------------------------------------------------------
# low_profitability (per business date, after reconciliation)
# ---------------------------------------------------------------------------
def raise_low_profitability_alerts(
    business_date: date | str, settings: Settings | None = None
) -> dict:
    settings = settings or load_settings()
    bd = date.fromisoformat(business_date) if isinstance(business_date, str) else business_date
    threshold = settings.alert_min_daily_profit
    raised = 0
    with connect(settings) as conn:
        rows = conn.execute(
            "SELECT vehicle_id, estimated_profit, profitability_status"
            " FROM daily_vehicle_profitability WHERE business_date = %s ORDER BY vehicle_id",
            (bd,),
        ).fetchall()
        low_keys = []
        for r in rows:
            profit = float(r["estimated_profit"])
            if not rules.low_profit_breached(profit, threshold):
                continue
            key = rules.low_profit_key(r["vehicle_id"], bd)
            low_keys.append(key)
            raised += insert_alert(
                conn,
                Alert(
                    alert_type=ALERT_LOW_PROFIT,
                    severity=rules.low_profit_severity(profit, threshold),
                    message=(
                        f"Vehicle {r['vehicle_id']} estimated profit LKR {profit:,.2f} on"
                        f" {bd.isoformat()} is below LKR {threshold:,.2f}"
                        f" ({r['profitability_status']})"
                    ),
                    dedup_key=key,
                    vehicle_id=r["vehicle_id"],
                    business_date=bd,
                ),
            )
        # A re-run of the day may have corrected a vehicle's numbers.
        resolved = _resolve(
            conn,
            "alert_type = %(t)s AND business_date = %(d)s AND NOT (dedup_key = ANY(%(keys)s))",
            {"t": ALERT_LOW_PROFIT, "d": bd, "keys": low_keys},
        )
    result = {
        "business_date": bd.isoformat(),
        "vehicles": len(rows),
        "below_threshold": len(low_keys),
        "raised": raised,
        "resolved": resolved,
    }
    log.info("low-profitability check done", extra={"fields": result})
    return result
