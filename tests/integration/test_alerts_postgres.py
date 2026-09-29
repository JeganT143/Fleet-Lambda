"""Alerts in PostgreSQL: de-duplication and resolution (test vehicles V9xx, 2099 dates)."""

from __future__ import annotations

import dataclasses
import uuid
from datetime import UTC, date, datetime, timedelta

import psycopg
import pytest

from fleet.alerts import evaluator, rules

pytestmark = pytest.mark.integration

DAY = date(2099, 3, 20)
IDLE_SINCE = datetime(2099, 3, 20, 4, 30, tzinfo=UTC)


def cleanup(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "DELETE FROM alerts WHERE vehicle_id LIKE 'V9%%' AND business_date = %s", (DAY,)
        )


@pytest.fixture
def conn(settings):
    cleanup(settings.postgres_dsn)
    with psycopg.connect(settings.postgres_dsn, row_factory=psycopg.rows.dict_row) as c:
        yield c
    cleanup(settings.postgres_dsn)


def idle_alert(severity: str = "warning") -> evaluator.Alert:
    return evaluator.Alert(
        alert_type="vehicle_idle",
        severity=severity,
        message="test idle alert",
        dedup_key=rules.idle_key("V951", IDLE_SINCE),
        vehicle_id="V951",
        business_date=DAY,
    )


def test_same_dedup_key_is_stored_once(conn):
    assert evaluator.insert_alert(conn, idle_alert()) is True
    assert evaluator.insert_alert(conn, idle_alert("critical")) is False  # same episode
    other = evaluator.Alert(
        **{**idle_alert().__dict__, "dedup_key": rules.idle_key("V951", IDLE_SINCE.replace(hour=9))}
    )
    assert evaluator.insert_alert(conn, other) is True  # a new idle episode
    conn.commit()
    rows = conn.execute(
        "SELECT dedup_key, severity, status FROM alerts WHERE vehicle_id = 'V951'"
        " AND business_date = %s ORDER BY alert_id",
        (DAY,),
    ).fetchall()
    assert [r["severity"] for r in rows] == ["warning", "warning"]
    assert all(r["status"] == "open" for r in rows)
    assert rows[0]["dedup_key"] == "vehicle_idle:V951:2099-03-20T04:30:00.000+00:00"


def test_resolve_marks_status_and_time(conn):
    evaluator.insert_alert(conn, idle_alert())
    n = evaluator._resolve(conn, "dedup_key = %(k)s", {"k": idle_alert().dedup_key})
    conn.commit()
    assert n == 1
    row = conn.execute(
        "SELECT status, resolved_at FROM alerts WHERE dedup_key = %s", (idle_alert().dedup_key,)
    ).fetchone()
    assert row["status"] == "resolved" and row["resolved_at"] is not None
    # a resolved alert is not raised again for the same episode
    assert evaluator.insert_alert(conn, idle_alert()) is False


# ---------------------------------------------------------------------------
# check_idle_vehicles with a fixed simulated "now" (fleet_ts) on 2099-04-10.
# Test events have ingestion_ts in 2000, so the live check never uses them as "now",
# and their 2099 timestamps are outside the live check's one-day window.
# ---------------------------------------------------------------------------
IDLE_DAY = date(2099, 4, 10)
IDLE_EVENTS = [
    # V951: moved until 08:00, idle from 08:05 (the idle episode starts at 08:05)
    ("V951", "enroute", "07:55"),
    ("V951", "on_trip", "08:00"),
    ("V951", "idle", "08:05"),
    ("V951", "idle", "08:10"),
    ("V951", "idle", "10:10"),
    # V952: idle for only 35 minutes
    ("V952", "on_trip", "09:30"),
    ("V952", "idle", "09:35"),
    ("V952", "idle", "10:10"),
    # V953: never moved -> idle since its first event
    ("V953", "idle", "08:00"),
    ("V953", "idle", "10:10"),
]


def at(hhmm: str) -> datetime:
    h, m = hhmm.split(":")
    return datetime(2099, 4, 10, int(h), int(m), tzinfo=UTC)


def insert_events(conn, events) -> None:
    for vid, status, hhmm in events:
        conn.execute(
            "INSERT INTO stream_events (event_id, trip_id, driver_id, vehicle_id, latitude,"
            " longitude, speed, status, fare, event_timestamp, business_date, zone,"
            " kafka_partition, kafka_offset, ingestion_ts, processing_ts)"
            " VALUES (%s, NULL, %s, %s, 13.05, 80.225, %s, %s, 0, %s, %s, 'central',"
            " 9996, 0, '2000-01-01T00:00:00Z', '2000-01-01T00:00:00Z')",
            (
                uuid.uuid5(uuid.NAMESPACE_URL, f"idle-it-{vid}-{hhmm}"),
                "D" + vid[1:],
                vid,
                0 if status == "idle" else 30,
                status,
                at(hhmm),
                IDLE_DAY,
            ),
        )
    conn.commit()


def idle_cleanup(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "DELETE FROM stream_events WHERE vehicle_id LIKE 'V95%%' AND business_date = %s",
            (IDLE_DAY,),
        )
        conn.execute(
            "DELETE FROM alerts WHERE alert_type = 'vehicle_idle' AND vehicle_id LIKE 'V95%%'"
            " AND business_date = %s",
            (IDLE_DAY,),
        )


@pytest.fixture
def idle_db(settings):
    idle_cleanup(settings.postgres_dsn)
    s = dataclasses.replace(settings, alert_idle_minutes=120)
    with psycopg.connect(settings.postgres_dsn, row_factory=psycopg.rows.dict_row) as c:
        insert_events(c, IDLE_EVENTS)
        yield s, c
    idle_cleanup(settings.postgres_dsn)


def idle_alerts(conn) -> dict[str, dict]:
    conn.commit()  # end any open read transaction to see other connections' writes
    rows = conn.execute(
        "SELECT vehicle_id, severity, status, resolved_at, message, dedup_key FROM alerts"
        " WHERE alert_type = 'vehicle_idle' AND vehicle_id LIKE 'V95%%' AND business_date = %s",
        (IDLE_DAY,),
    ).fetchall()
    return {r["vehicle_id"]: r for r in rows}


def test_idle_starts_at_first_idle_event_not_last_moving_one(idle_db):
    s, conn = idle_db
    # 10:04: V951 idle since 08:05 = 119 min -> no alert yet (08:00 would give 124 min)
    result = evaluator.check_idle_vehicles(s, fleet_ts=at("10:04"))
    assert result["vehicles"] == 3
    assert "V951" not in result["over_threshold"]
    assert result["over_threshold"] == ["V953"]  # never moved: idle since 08:00 -> 124 min


def test_idle_alert_raised_deduplicated_and_resolved_when_vehicle_moves(idle_db):
    s, conn = idle_db
    first = evaluator.check_idle_vehicles(s, fleet_ts=at("10:10"))
    assert first["over_threshold"] == ["V951", "V953"] and first["raised"] == 2
    alerts = idle_alerts(conn)
    assert sorted(alerts) == ["V951", "V953"]  # V952: idle only 35 min
    v951 = alerts["V951"]
    assert v951["dedup_key"] == "vehicle_idle:V951:2099-04-10T08:05:00.000+00:00"
    assert v951["severity"] == "warning" and v951["status"] == "open"
    assert "idle for 125 simulated minutes" in v951["message"]
    assert "idle since 2099-04-10T08:05:00+00:00" in v951["message"]
    assert alerts["V953"]["dedup_key"] == "vehicle_idle:V953:2099-04-10T08:00:00.000+00:00"

    # same episode again: nothing new
    again = evaluator.check_idle_vehicles(s, fleet_ts=at("10:12"))
    assert again["raised"] == 0 and again["resolved"] == 0

    # V951 starts a trip -> its alert is resolved; V953 is still idle -> stays open
    insert_events(conn, [("V951", "enroute", "10:15")])
    moved = evaluator.check_idle_vehicles(s, fleet_ts=at("10:15"))
    assert moved["resolved"] == 1 and moved["raised"] == 0
    alerts = idle_alerts(conn)
    assert alerts["V951"]["status"] == "resolved" and alerts["V951"]["resolved_at"]
    assert alerts["V953"]["status"] == "open"


# ---------------------------------------------------------------------------
# check_no_stream_data with injected "now" and last ingestion. The last ingestion is a
# made-up (2000-01-01, business date 2099-04-20): no rows are inserted, and the live
# check (which resolves only alerts of its own recent business dates) never touches it.
# ---------------------------------------------------------------------------
NO_DATA_DAY = date(2099, 4, 20)
LAST = datetime(2000, 1, 1, 0, 0, 0, tzinfo=UTC)


@pytest.fixture
def no_data_settings(settings):
    def clean():
        with psycopg.connect(settings.postgres_dsn, autocommit=True) as c:
            c.execute(
                "DELETE FROM alerts WHERE alert_type = 'no_stream_data' AND business_date = %s",
                (NO_DATA_DAY,),
            )

    clean()
    yield dataclasses.replace(settings, alert_no_data_seconds=60)
    clean()


def test_no_stream_data_raised_once_then_resolved(no_data_settings):
    s = no_data_settings
    last = (LAST, NO_DATA_DAY)
    quiet = evaluator.check_no_stream_data(s, now=LAST + timedelta(seconds=60), last_ingestion=last)
    assert quiet["raised"] == 0  # exactly on the threshold: not yet "older than"

    silent = evaluator.check_no_stream_data(
        s, now=LAST + timedelta(seconds=75), last_ingestion=last
    )
    assert silent["raised"] == 1 and silent["silence_seconds"] == 75
    again = evaluator.check_no_stream_data(
        s, now=LAST + timedelta(seconds=135), last_ingestion=last
    )
    assert again["raised"] == 0  # same outage, same dedup_key

    with psycopg.connect(s.postgres_dsn, row_factory=psycopg.rows.dict_row) as c:
        rows = c.execute(
            "SELECT severity, status, dedup_key FROM alerts"
            " WHERE alert_type = 'no_stream_data' AND business_date = %s",
            (NO_DATA_DAY,),
        ).fetchall()
    assert rows == [
        {
            "severity": "critical",
            "status": "open",
            "dedup_key": "no_stream_data:2000-01-01T00:00:00.000+00:00",
        }
    ]

    # data arrives again: a new event ingested 5 s ago -> the outage alert is resolved
    fresh = (LAST + timedelta(seconds=200), NO_DATA_DAY)
    back = evaluator.check_no_stream_data(
        s, now=LAST + timedelta(seconds=205), last_ingestion=fresh
    )
    assert back["raised"] == 0 and back["resolved"] == 1
    with psycopg.connect(s.postgres_dsn) as c:
        status, resolved_at = c.execute(
            "SELECT status, resolved_at FROM alerts"
            " WHERE alert_type = 'no_stream_data' AND business_date = %s",
            (NO_DATA_DAY,),
        ).fetchone()
    assert status == "resolved" and resolved_at is not None
