"""Spark -> PostgreSQL: run the real foreachBatch writers on static DataFrames.

Test data conventions (tests/conftest.py): vehicles V9xx, business dates in 2099.
Rejected records use Kafka partition 9999 and data-quality rows the batch_ref prefix
'test_ingest:', so nothing here can touch live pipeline rows. Everything is cleaned up.
"""

from __future__ import annotations

import dataclasses
import json
import uuid
from datetime import date, datetime

import psycopg
import pytest

pytestmark = [pytest.mark.integration, pytest.mark.spark]

T = pytest.importorskip("fleet.streaming.transforms", reason="pyspark not installed")
from fleet.streaming import sinks  # noqa: E402

TEST_PARTITION = 9999
QUERY_NAME = "test_ingest"
DAY = "2099-01-01"


def ev(vehicle, status, hhmm, fare=0.0, speed=None, lat=6.91, lon=79.915):  # Battaramulla
    if speed is None:
        speed = 0.0 if status == "idle" else 30.0
    return json.dumps(
        {
            "event_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"it-{vehicle}-{status}-{hhmm}")),
            "trip_id": None if status == "idle" else f"T-{vehicle}",
            "driver_id": "D" + vehicle[1:],
            "vehicle_id": vehicle,
            "latitude": lat,
            "longitude": lon,
            "speed": speed,
            "status": status,
            "fare": fare,
            "timestamp": f"{DAY}T{hhmm}:00+05:30",  # Colombo local time
        }
    )


VALUES = [
    ev("V901", "idle", "08:05"),
    ev("V901", "enroute", "08:10"),
    ev("V901", "on_trip", "08:20", fare=320.5, lon=79.96),  # Malabe
    ev("V902", "idle", "08:30", lat=6.85, lon=79.87),  # Dehiwala
    ev("V902", "on_trip", "09:15", fare=180.0),
    ev("V903", "idle", "08:40", speed=-5.0),  # invalid: negative speed
    '{"vehicle_id": "V904", "spe',  # invalid: malformed JSON
]
VALID_COUNT = 5


def cleanup(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "DELETE FROM stream_events WHERE vehicle_id LIKE 'V9%%' "
            "AND business_date BETWEEN '2099-01-01' AND '2099-12-31'"
        )
        conn.execute("DELETE FROM rejected_events WHERE kafka_partition = %s", (TEST_PARTITION,))
        conn.execute("DELETE FROM data_quality_stats WHERE batch_ref LIKE %s", (f"{QUERY_NAME}:%",))
        for table in ("realtime_vehicle_metrics", "realtime_zone_metrics"):
            conn.execute(
                f"DELETE FROM {table} WHERE business_date BETWEEN '2099-01-01' AND '2099-12-31'"
            )


@pytest.fixture
def clean_db(settings):
    cleanup(settings.postgres_dsn)
    yield
    cleanup(settings.postgres_dsn)


@pytest.fixture
def kafka_batch(spark):
    ingest = datetime(2099, 1, 1, 12, 0, 0)
    rows = [(v.encode(), TEST_PARTITION, i, ingest) for i, v in enumerate(VALUES)]
    return spark.createDataFrame(
        rows, "value BINARY, partition INT, offset LONG, timestamp TIMESTAMP"
    )


def fetch(pg_conn, sql, params=()):
    return pg_conn.execute(sql, params).fetchall()


def test_ingest_writer_is_idempotent(settings, kafka_batch, clean_db, pg_conn):
    first = sinks.write_ingest_batch(kafka_batch, 1, settings, query_name=QUERY_NAME)
    again = sinks.write_ingest_batch(kafka_batch, 1, settings, query_name=QUERY_NAME)  # replay
    assert first["rows"] == again["rows"] == len(VALUES)
    assert first["rejected"] == 2

    events = fetch(
        pg_conn,
        "SELECT * FROM stream_events WHERE vehicle_id LIKE 'V9%%' AND business_date = %s "
        "ORDER BY kafka_offset",
        (date(2099, 1, 1),),
    )
    assert len(events) == VALID_COUNT  # no duplicates after the replay
    fare_event = events[2]
    assert fare_event["vehicle_id"] == "V901"
    assert float(fare_event["fare"]) == 320.5
    assert fare_event["zone"] == "Malabe"
    assert fare_event["status"] == "on_trip"
    assert fare_event["kafka_partition"] == TEST_PARTITION
    # 08:20 in Colombo is stored as the UTC instant 02:50; the business date is Sri Lankan
    assert fare_event["event_timestamp"].isoformat() == "2099-01-01T02:50:00+00:00"
    assert fare_event["business_date"] == date(2099, 1, 1)
    assert fare_event["ingestion_ts"].isoformat() == "2099-01-01T12:00:00+00:00"
    assert fare_event["processing_ts"] is not None
    assert events[0]["trip_id"] is None

    rejected = fetch(
        pg_conn,
        "SELECT kafka_offset, reason, raw_value FROM rejected_events "
        "WHERE kafka_partition = %s ORDER BY kafka_offset",
        (TEST_PARTITION,),
    )
    assert [(r["kafka_offset"], r["reason"]) for r in rejected] == [
        (5, "invalid_speed"),
        (6, "malformed_json"),
    ]
    assert rejected[1]["raw_value"] == VALUES[6]

    stats = fetch(
        pg_conn,
        "SELECT * FROM data_quality_stats WHERE batch_ref LIKE %s",
        (f"{QUERY_NAME}:%",),
    )
    assert len(stats) == 1  # replaced, not duplicated
    assert stats[0]["pipeline_name"] == "stream_ingest"
    assert stats[0]["batch_ref"] == f"{QUERY_NAME}:p{TEST_PARTITION}=0-6"
    assert (stats[0]["records_total"], stats[0]["records_valid"]) == (7, 5)
    assert stats[0]["records_rejected"] == 2
    assert stats[0]["rule_failures"] == {"invalid_speed": 1, "malformed_json": 1}
    assert stats[0]["business_date"] == date(2099, 1, 1)


def test_metric_writers_upsert(settings, kafka_batch, clean_db, pg_conn):
    valid, _ = T.split_valid_invalid(T.parse_kafka(kafka_batch))
    events = T.enrich(valid)
    fleet = T.fleet_window_metrics(events, "1 hour", "15 minutes")
    zones = T.zone_window_metrics(events, "1 hour", "15 minutes")
    for _ in range(2):  # the second run must update, not duplicate
        assert sinks.write_fleet_metrics_batch(fleet, 1, settings)["rows"] == 2
        assert sinks.write_zone_metrics_batch(zones, 1, settings)["rows"] == 4

    rows = fetch(
        pg_conn,
        "SELECT * FROM realtime_vehicle_metrics WHERE business_date = %s ORDER BY window_start",
        (date(2099, 1, 1),),
    )
    assert len(rows) == 2
    w8 = rows[0]
    # 08:00-09:00 in Colombo = 02:30-03:30 UTC
    assert w8["window_start"].isoformat() == "2099-01-01T02:30:00+00:00"
    assert w8["window_end"].isoformat() == "2099-01-01T03:30:00+00:00"
    assert (w8["vehicles_reporting"], w8["active_vehicles"], w8["idle_vehicles"]) == (2, 1, 1)
    assert (w8["event_count"], w8["idle_event_count"]) == (4, 2)
    assert w8["idle_ratio"] == pytest.approx(0.5)
    assert (w8["trips_completed"], float(w8["total_earnings"])) == (1, 320.5)
    assert float(w8["avg_fare"]) == 320.5
    w9 = rows[1]
    assert (w9["vehicles_reporting"], w9["event_count"], float(w9["total_earnings"])) == (
        1,
        1,
        180.0,
    )

    zone_rows = fetch(
        pg_conn,
        "SELECT to_char(window_start AT TIME ZONE 'Asia/Colombo', 'HH24:MI') AS w, zone, "
        "event_count, total_earnings, avg_fare FROM realtime_zone_metrics "
        "WHERE business_date = %s",
        (date(2099, 1, 1),),
    )
    got = {
        (r["w"], r["zone"]): (
            r["event_count"],
            float(r["total_earnings"]),
            None if r["avg_fare"] is None else float(r["avg_fare"]),
        )
        for r in zone_rows
    }
    assert got == {
        ("08:00", "Battaramulla"): (2, 0.0, None),
        ("08:00", "Malabe"): (1, 320.5, 320.5),
        ("08:00", "Dehiwala"): (1, 0.0, None),
        ("09:00", "Battaramulla"): (1, 180.0, 180.0),
    }


def test_database_error_fails_the_batch_loudly(settings, kafka_batch):
    """No silent data loss: a write error must propagate (the query then fails)."""
    unreachable = dataclasses.replace(settings, postgres_port=1)
    with pytest.raises(Exception, match=r"(?i)connection|refused|failed"):
        sinks.write_ingest_batch(kafka_batch, 1, unreachable, query_name=QUERY_NAME)
