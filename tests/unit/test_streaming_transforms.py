"""Speed-layer DataFrame transforms on small static DataFrames (and one tiny stream)."""

from __future__ import annotations

import json
import uuid
from datetime import date, datetime

import pytest

pytestmark = pytest.mark.spark

T = pytest.importorskip("fleet.streaming.transforms", reason="pyspark not installed")

WINDOW, WATERMARK = "1 hour", "15 minutes"
CENTRAL, EAST, SOUTH_WEST = (13.05, 80.225), (13.05, 80.29), (12.91, 80.16)


def ev(vehicle, status, hhmm, fare=0.0, where=CENTRAL, speed=None, day="2026-01-01"):
    """One contract event as JSON text."""
    if speed is None:
        speed = 0.0 if status == "idle" else 30.0
    return json.dumps(
        {
            "event_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{vehicle}{status}{hhmm}{day}")),
            "trip_id": None if status == "idle" else f"T-{vehicle}",
            "driver_id": "D" + vehicle[1:],
            "vehicle_id": vehicle,
            "latitude": where[0],
            "longitude": where[1],
            "speed": speed,
            "status": status,
            "fare": fare,
            "timestamp": f"{day}T{hhmm}Z" if len(hhmm) > 5 else f"{day}T{hhmm}:00.000Z",
        }
    )


# Window 08:00-09:00 and 09:00-10:00 on 2026-01-01, plus one invalid record
VALUES = [
    ev("V001", "idle", "08:05"),
    ev("V001", "enroute", "08:10"),
    ev("V001", "on_trip", "08:20"),
    ev("V001", "on_trip", "08:30", fare=300.0, where=EAST),
    ev("V002", "idle", "08:05", where=SOUTH_WEST),
    ev("V002", "idle", "08:40", where=SOUTH_WEST),
    ev("V003", "on_trip", "08:50", fare=200.0),
    ev("V004", "enroute", "08:15", speed=-1.0),  # invalid: negative speed
    ev("V001", "idle", "09:05", where=EAST),
    ev("V002", "enroute", "09:10", where=SOUTH_WEST),
    ev("V002", "on_trip", "09:59:59.999", fare=150.0, where=SOUTH_WEST),
]


def kafka_df(spark, values, partition=0):
    """A static DataFrame shaped like the Kafka source (value, partition, offset, timestamp)."""
    ingest = datetime(2026, 9, 29, 12, 0, 0)
    rows = [
        (v.encode() if v is not None else None, partition, i, ingest) for i, v in enumerate(values)
    ]
    return spark.createDataFrame(
        rows, "value BINARY, partition INT, offset LONG, timestamp TIMESTAMP"
    )


@pytest.fixture(scope="module")
def events(spark):
    valid, _ = T.split_valid_invalid(T.parse_kafka(kafka_df(spark, VALUES)))
    return T.enrich(valid)


def test_split_and_rejected_rows(spark):
    checked = T.parse_kafka(kafka_df(spark, [*VALUES, '{"broken'], partition=2))
    valid, invalid = T.split_valid_invalid(checked)
    assert valid.count() == 10
    rejected = {r.kafka_offset: r for r in T.rejected_event_rows(invalid).collect()}
    assert set(rejected) == {7, 11}
    assert rejected[7].reason == "invalid_speed"
    assert rejected[11].reason == "malformed_json"
    assert rejected[11].raw_value == '{"broken'
    assert rejected[11].kafka_partition == 2
    assert rejected[11].ingestion_ts == "2026-09-29T12:00:00.000000Z"


def test_enrich_adds_time_zone_and_kafka_columns(events):
    rows = {(r.vehicle_id, r.event_timestamp): r for r in events.collect()}
    first = rows[("V001", datetime(2026, 1, 1, 8, 5))]
    assert first.business_date == date(2026, 1, 1)
    assert first.zone == "central"
    assert first.kafka_partition == 0 and first.kafka_offset == 0
    assert first.ingestion_ts == datetime(2026, 9, 29, 12, 0, 0)
    assert first.processing_ts is not None
    assert first.trip_id is None and first.fare == 0.0 and first.speed == 0.0
    assert rows[("V001", datetime(2026, 1, 1, 8, 30))].zone == "east"
    assert rows[("V002", datetime(2026, 1, 1, 8, 5))].zone == "south-west"


def test_business_date_is_utc_date_of_event_time(spark):
    # 01:00 in India (+05:30) on Jan 2 is 19:30 UTC on Jan 1
    raw = json.loads(ev("V001", "idle", "08:00"))
    raw["timestamp"] = "2026-01-02T01:00:00+05:30"
    valid, _ = T.split_valid_invalid(T.parse_kafka(kafka_df(spark, [json.dumps(raw)])))
    row = T.enrich(valid).first()
    assert row.event_timestamp == datetime(2026, 1, 1, 19, 30)
    assert row.business_date == date(2026, 1, 1)


def test_stream_event_rows_are_db_ready(events):
    row = T.stream_event_rows(events).orderBy("kafka_offset").first()
    assert tuple(row.asDict()) == T.STREAM_EVENT_COLUMNS
    assert row.event_timestamp == "2026-01-01T08:05:00.000000Z"
    assert row.business_date == "2026-01-01"


def test_fleet_window_metrics_exact_numbers(events):
    rows = T.metric_rows(
        T.fleet_window_metrics(events, WINDOW, WATERMARK), T.FLEET_METRIC_COLUMNS
    ).orderBy("window_start")
    got = [r.asDict() for r in rows.collect()]
    assert got == [
        {
            "window_start": "2026-01-01T08:00:00.000000Z",
            "window_end": "2026-01-01T09:00:00.000000Z",
            "business_date": "2026-01-01",
            "vehicles_reporting": 3,  # V001, V002, V003 (V004's event was invalid)
            "active_vehicles": 2,  # V001, V003
            "idle_vehicles": 1,  # V002 never left idle
            "event_count": 7,
            "idle_event_count": 3,
            "idle_ratio": pytest.approx(3 / 7),
            "trips_completed": 2,
            "total_earnings": 500.0,
            "avg_fare": 250.0,
        },
        {
            "window_start": "2026-01-01T09:00:00.000000Z",
            "window_end": "2026-01-01T10:00:00.000000Z",
            "business_date": "2026-01-01",
            "vehicles_reporting": 2,
            "active_vehicles": 1,
            "idle_vehicles": 1,
            "event_count": 3,
            "idle_event_count": 1,
            "idle_ratio": pytest.approx(1 / 3),
            "trips_completed": 1,
            "total_earnings": 150.0,
            "avg_fare": 150.0,
        },
    ]


def test_zone_window_metrics_exact_numbers(events):
    rows = T.metric_rows(
        T.zone_window_metrics(events, WINDOW, WATERMARK), T.ZONE_METRIC_COLUMNS
    ).collect()
    got = {
        (r.window_start[11:16], r.zone): (
            r.vehicles_reporting,
            r.active_vehicles,
            r.event_count,
            r.trips_completed,
            r.total_earnings,
            r.avg_fare,
        )
        for r in rows
    }
    assert got == {
        ("08:00", "central"): (2, 2, 4, 1, 200.0, 200.0),
        ("08:00", "east"): (1, 1, 1, 1, 300.0, 300.0),
        ("08:00", "south-west"): (1, 0, 2, 0, 0.0, None),
        ("09:00", "east"): (1, 0, 1, 0, 0.0, None),
        ("09:00", "south-west"): (1, 1, 2, 1, 150.0, 150.0),
    }
    # zone earnings add up to the fleet earnings
    assert sum(v[4] for v in got.values()) == 650.0


def test_watermark_drops_events_that_are_too_late(spark, tmp_path):
    """A real (file-source) stream: a late event for a closed window is ignored."""
    from pyspark.sql import functions as F

    source_dir = tmp_path / "in"
    source_dir.mkdir()
    outputs: list[dict] = []

    def collect(batch_df, batch_id):
        for r in T.metric_rows(batch_df, T.FLEET_METRIC_COLUMNS).collect():
            outputs.append({"batch": batch_id, **r.asDict()})

    lines = spark.readStream.text(str(source_dir))
    kafka_like = lines.select(
        F.col("value").cast("binary").alias("value"),
        F.lit(0).alias("partition"),
        F.lit(0).cast("long").alias("offset"),
        F.current_timestamp().alias("timestamp"),
    )
    valid, _ = T.split_valid_invalid(T.parse_kafka(kafka_like))
    metrics = T.fleet_window_metrics(T.enrich(valid), WINDOW, WATERMARK)
    query = (
        metrics.writeStream.outputMode("update")
        .foreachBatch(collect)
        .option("checkpointLocation", str(tmp_path / "chk"))
        .start()
    )
    try:
        # batch 1: max event time 10:30 -> watermark becomes 10:15
        (source_dir / "1.txt").write_text(
            "\n".join([ev("V001", "idle", "08:05"), ev("V001", "idle", "10:30")]) + "\n"
        )
        query.processAllAvailable()
        # batch 2: 08:10 is older than the watermark (window 08:00-09:00 is closed) -> dropped;
        # 10:20 is late too but its window (10:00-11:00) is still open -> counted
        (source_dir / "2.txt").write_text(
            "\n".join([ev("V002", "idle", "08:10"), ev("V002", "idle", "10:20")]) + "\n"
        )
        query.processAllAvailable()
    finally:
        query.stop()

    def latest(window_start):
        rows = [o for o in outputs if o["window_start"].startswith(window_start)]
        return max(rows, key=lambda o: o["batch"])

    assert latest("2026-01-01T08:00")["event_count"] == 1  # late 08:10 event not counted
    assert latest("2026-01-01T10:00")["event_count"] == 2
    assert latest("2026-01-01T10:00")["vehicles_reporting"] == 2


def test_approx_distinct_is_exact_for_fleet_ids(spark):
    """HLL++ is exact while no two ids share a register; the fleet ids are fixed, so check.

    If the full set V001..V050 has an exact estimate, every subset does too.
    """
    import random

    from pyspark.sql import functions as F

    ids = [f"V{n:03d}" for n in range(1, 51)]
    rng = random.Random(3)
    groups = [ids, ids[:20]] + [rng.sample(ids, rng.randint(1, 50)) for _ in range(20)]
    rows = [(g, v) for g, members in enumerate(groups) for v in members]
    df = spark.createDataFrame(rows, "g INT, vehicle_id STRING")
    got = df.groupBy("g").agg(
        F.approx_count_distinct("vehicle_id", T.APPROX_DISTINCT_RSD).alias("n")
    )
    assert {r.g: r.n for r in got.collect()} == {g: len(m) for g, m in enumerate(groups)}
