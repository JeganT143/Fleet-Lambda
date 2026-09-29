"""Pure DataFrame transformations of the speed layer.

Every function takes and returns DataFrames and has no side effects, so the same code
runs on the streaming Kafka source (fleet/streaming/job.py) and on small static
DataFrames in unit tests.

Pipeline:

    Kafka rows --parse_kafka--> raw_value + parsed struct + reasons[]
               --split_valid_invalid--> valid / invalid
    valid      --enrich--> typed event + event_timestamp, business_date, zone,
                           ingestion_ts, processing_ts, kafka_partition/offset
    enriched   --fleet_window_metrics--> realtime_vehicle_metrics rows
               --zone_window_metrics---> realtime_zone_metrics rows

Timestamps: the job runs with spark.sql.session.timeZone=UTC and keeps every
timestamp in UTC. The business date is the Sri Lankan calendar date (Asia/Colombo,
UTC+05:30) of the simulated event time (AGENTS.md 8.3), and windows are aligned to
Sri Lankan clock hours (see local_date / window_start_time).
"""

from __future__ import annotations

import re
from datetime import datetime
from zoneinfo import ZoneInfo

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from fleet.common.contracts import BUSINESS_TIMEZONE, STATUS_ENROUTE, STATUS_IDLE, STATUS_ON_TRIP
from fleet.common.zones import (
    CITY_LAT_MAX,
    CITY_LAT_MIN,
    CITY_LON_MAX,
    CITY_LON_MIN,
    GRID_SIZE,
    OUTSIDE_ZONE,
    ZONE_NAMES,
)
from fleet.streaming.validation import spark_parse_json, spark_reasons_column

# Relative standard deviation for approx_count_distinct (see _common_aggregates).
# 0.02 -> HyperLogLog++ with 2^12 registers. For a fleet of tens of vehicles the
# estimate is in HLL's "linear counting" range: it is exact as long as no two vehicle
# ids hash to the same register. The ids are fixed (V001..), so that is checkable:
# tests/unit/test_streaming_transforms.py proves exactness for V001..V050.
# Trade-off (measured): lower precision is cheaper. The HLL buffer is ~410 longs per
# aggregate at 0.02 vs ~1,640 at 0.01, and a metrics micro-batch took ~2.1 s vs ~5.4 s
# locally; 0.02 is still exact for V001..V050, 0.05 is not (30 ids -> 31).
# NOTE: changing this value changes the streaming state schema, so the fleet_metrics
# and zone_metrics checkpoints must be deleted when it is changed.
APPROX_DISTINCT_RSD = 0.02

_UNIT_SECONDS = {"second": 1, "minute": 60, "hour": 3600, "day": 86400}


def local_date(ts: Column | str) -> Column:
    """Sri Lankan calendar date of a UTC timestamp column (the business date)."""
    return F.to_date(F.from_utc_timestamp(ts, BUSINESS_TIMEZONE))


def window_start_time(window_duration: str) -> str:
    """`startTime` for F.window so windows begin on Sri Lankan clock boundaries.

    Spark aligns windows to the UTC epoch. Colombo is UTC+05:30, so plain 1-hour
    windows would run 08:30-09:30 local time. Shifting them by (-offset mod length)
    makes them run 09:00-10:00 local: 30 minutes for hourly windows, 0 for 15 or 30
    minute windows, 18h30 for daily windows (= local midnight).
    """
    match = re.fullmatch(r"\s*(\d+)\s*(second|minute|hour|day)s?\s*", window_duration)
    if not match:
        raise ValueError(f"unsupported window duration: {window_duration!r}")
    length = int(match.group(1)) * _UNIT_SECONDS[match.group(2)]
    offset = ZoneInfo(BUSINESS_TIMEZONE).utcoffset(datetime(2026, 1, 1))
    assert offset is not None
    return f"{int(-offset.total_seconds()) % length} seconds"


# ISO-8601 with an explicit offset. Under a UTC session this renders "...Z", which
# psycopg passes to PostgreSQL's timestamptz unambiguously.
ISO_TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"


# ---------------------------------------------------------------------------
# Parse + validate
# ---------------------------------------------------------------------------
def parse_kafka(kafka_df: DataFrame) -> DataFrame:
    """Kafka source rows -> raw value, Kafka metadata, parsed struct and reason codes.

    ingestion_ts is the Kafka record timestamp (real time the producer/broker stamped it).
    """
    raw = kafka_df.select(
        F.col("value").cast("string").alias("raw_value"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("ingestion_ts"),
    )
    parsed = raw.withColumn("parsed", spark_parse_json(F.col("raw_value")))
    return parsed.withColumn("reasons", spark_reasons_column(F.col("parsed")))


def split_valid_invalid(checked: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split the output of parse_kafka into (valid, invalid) by the reasons array."""
    is_valid = F.size("reasons") == 0
    return checked.filter(is_valid), checked.filter(~is_valid)


# ---------------------------------------------------------------------------
# Enrichment
# ---------------------------------------------------------------------------
def _grid_index(value: Column, low: float, high: float) -> Column:
    """Spark twin of zones.grid_index: same arithmetic, same order of operations.

    Python computes int((value - low) / (high - low) * n); inside the box the value is
    >= low, so int() (truncation) equals floor(). The result is clamped to 0..n-1, so
    the upper edge belongs to the last cell.
    """
    idx = F.floor((value - F.lit(low)) / F.lit(high - low) * F.lit(GRID_SIZE)).cast("int")
    return F.least(F.greatest(idx, F.lit(0)), F.lit(GRID_SIZE - 1))


def zone_column(latitude: Column, longitude: Column) -> Column:
    """Zone name for coordinates; built only from fleet/common/zones.py constants."""
    inside = latitude.between(CITY_LAT_MIN, CITY_LAT_MAX) & longitude.between(
        CITY_LON_MIN, CITY_LON_MAX
    )
    row = _grid_index(latitude, CITY_LAT_MIN, CITY_LAT_MAX)
    col = _grid_index(longitude, CITY_LON_MIN, CITY_LON_MAX)
    # ZONE_NAMES flattened row by row: index = row * GRID_SIZE + col (element_at is 1-based)
    names = F.array(*[F.lit(name) for zone_row in ZONE_NAMES for name in zone_row])
    return F.when(inside, F.element_at(names, row * GRID_SIZE + col + 1)).otherwise(
        F.lit(OUTSIDE_ZONE)
    )


def enrich(valid: DataFrame) -> DataFrame:
    """Typed, enriched events (the stream_events columns, timestamps still as timestamps)."""
    p = F.col("parsed")
    typed = valid.select(
        p.event_id.alias("event_id"),
        p.trip_id.alias("trip_id"),
        p.driver_id.alias("driver_id"),
        p.vehicle_id.alias("vehicle_id"),
        p.latitude.cast("double").alias("latitude"),
        p.longitude.cast("double").alias("longitude"),
        p.speed.cast("double").alias("speed"),
        p.status.alias("status"),
        F.round(p.fare.cast("double"), 2).alias("fare"),
        p.timestamp.cast("timestamp").alias("event_timestamp"),  # simulated event time
        "kafka_partition",
        "kafka_offset",
        "ingestion_ts",  # real: Kafka record time
    )
    return typed.select(
        "*",
        local_date("event_timestamp").alias("business_date"),  # Sri Lankan date
        zone_column(F.col("latitude"), F.col("longitude")).alias("zone"),
        F.current_timestamp().alias("processing_ts"),  # real: Spark micro-batch time
    )


# ---------------------------------------------------------------------------
# Rows shaped for PostgreSQL (timestamps as ISO strings, dates as yyyy-MM-dd)
# ---------------------------------------------------------------------------
def _iso(column: str | Column) -> Column:
    return F.date_format(column, ISO_TS_FORMAT)


def _for_db(columns: tuple[str, ...], timestamp_columns: tuple[str, ...]) -> list[Column]:
    """Select `columns`, rendering timestamps as ISO strings and business_date as text."""
    selected = []
    for c in columns:
        if c in timestamp_columns:
            selected.append(_iso(c).alias(c))
        elif c == "business_date":
            selected.append(F.col(c).cast("string").alias(c))
        else:
            selected.append(F.col(c))
    return selected


STREAM_EVENT_COLUMNS: tuple[str, ...] = (
    "event_id",
    "trip_id",
    "driver_id",
    "vehicle_id",
    "latitude",
    "longitude",
    "speed",
    "status",
    "fare",
    "event_timestamp",
    "business_date",
    "zone",
    "kafka_partition",
    "kafka_offset",
    "ingestion_ts",
    "processing_ts",
)


def stream_event_rows(enriched: DataFrame) -> DataFrame:
    timestamps = ("event_timestamp", "ingestion_ts", "processing_ts")
    return enriched.select(*_for_db(STREAM_EVENT_COLUMNS, timestamps))


REJECTED_EVENT_COLUMNS: tuple[str, ...] = (
    "raw_value",
    "reason",
    "kafka_partition",
    "kafka_offset",
    "ingestion_ts",
    "processing_ts",
)


def rejected_event_rows(invalid: DataFrame) -> DataFrame:
    """Quarantine rows: the raw value is kept as-is, reasons joined with commas."""
    return invalid.select(
        "raw_value",
        F.concat_ws(",", "reasons").alias("reason"),
        "kafka_partition",
        "kafka_offset",
        _iso("ingestion_ts").alias("ingestion_ts"),
        _iso(F.current_timestamp()).alias("processing_ts"),
    )


# ---------------------------------------------------------------------------
# Event-time window aggregations (docs/architecture.md 5.4)
# ---------------------------------------------------------------------------
def _is_active() -> Column:
    return F.col("status").isin(STATUS_ENROUTE, STATUS_ON_TRIP)


def _common_aggregates() -> list[Column]:
    """Aggregates shared by the fleet and zone metrics.

    Fare semantics (contracts.py): a trip's fare appears once, on its last on_trip
    event, so earnings = SUM(fare), trips = COUNT(fare > 0), avg fare = AVG(fare | fare > 0).

    Distinct vehicles: Spark does not support exact COUNT(DISTINCT) in streaming
    aggregations, so we use approx_count_distinct (HyperLogLog++). With
    rsd=APPROX_DISTINCT_RSD (0.02) and a fleet of tens of vehicles the estimate
    equals the exact count in practice; the
    daily batch layer recomputes exact numbers from stream_events anyway.
    active_vehicles counts a vehicle if it had AT LEAST ONE enroute/on_trip event in the
    window (null for idle events, and nulls are not counted).
    """
    active_vehicle = F.when(_is_active(), F.col("vehicle_id"))
    return [
        F.approx_count_distinct("vehicle_id", APPROX_DISTINCT_RSD).alias("vehicles_reporting"),
        F.approx_count_distinct(active_vehicle, APPROX_DISTINCT_RSD).alias("active_vehicles"),
        F.count(F.lit(1)).alias("event_count"),
        F.sum(F.when(F.col("status") == STATUS_IDLE, 1).otherwise(0)).alias("idle_event_count"),
        F.sum(F.when(F.col("fare") > 0, 1).otherwise(0)).alias("trips_completed"),
        F.sum("fare").alias("total_earnings"),
        F.avg(F.when(F.col("fare") > 0, F.col("fare"))).alias("avg_fare"),
    ]


def _windowed(events: DataFrame, window_duration: str, watermark_delay: str, *keys: str):
    """Tumbling event-time windows with a watermark.

    The watermark tells Spark how late an event may arrive (in event time) and still be
    counted: a window's state is kept until max_event_time_seen - watermark_delay passes
    the window end, then it is finalised and dropped. Later events are ignored here
    (the batch layer still sees them in stream_events). On a static DataFrame the
    watermark is a no-op, which is what the unit tests use.
    """
    return events.withWatermark("event_timestamp", watermark_delay).groupBy(
        F.window(
            "event_timestamp", window_duration, startTime=window_start_time(window_duration)
        ).alias("window"),
        *keys,
    )


def _window_columns() -> list[Column]:
    return [
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        local_date("window.start").alias("business_date"),
    ]


def fleet_window_metrics(
    events: DataFrame, window_duration: str, watermark_delay: str
) -> DataFrame:
    """One row per window -> realtime_vehicle_metrics.

    idle_vehicles = vehicles_reporting - active_vehicles (vehicles that never left idle
    in the window). idle_ratio = idle events / all events: every vehicle reports at a
    fixed cadence, so the share of events equals the share of vehicle-time spent idle.
    """
    agg = _windowed(events, window_duration, watermark_delay).agg(*_common_aggregates())
    return agg.select(
        *_window_columns(),
        "vehicles_reporting",
        "active_vehicles",
        F.greatest(F.col("vehicles_reporting") - F.col("active_vehicles"), F.lit(0)).alias(
            "idle_vehicles"
        ),
        "event_count",
        "idle_event_count",
        (F.col("idle_event_count") / F.col("event_count")).alias("idle_ratio"),
        "trips_completed",
        F.round("total_earnings", 2).alias("total_earnings"),
        F.round("avg_fare", 2).alias("avg_fare"),
    )


def zone_window_metrics(events: DataFrame, window_duration: str, watermark_delay: str) -> DataFrame:
    """One row per (window, zone) -> realtime_zone_metrics.

    A vehicle that crosses zones inside a window is counted in each zone it reported
    from. Earnings are attributed to the zone of the fare event, i.e. the drop-off zone.
    """
    agg = _windowed(events, window_duration, watermark_delay, "zone").agg(*_common_aggregates())
    return agg.select(
        *_window_columns(),
        "zone",
        "vehicles_reporting",
        "active_vehicles",
        "event_count",
        "trips_completed",
        F.round("total_earnings", 2).alias("total_earnings"),
        F.round("avg_fare", 2).alias("avg_fare"),
    )


FLEET_METRIC_COLUMNS: tuple[str, ...] = (
    "window_start",
    "window_end",
    "business_date",
    "vehicles_reporting",
    "active_vehicles",
    "idle_vehicles",
    "event_count",
    "idle_event_count",
    "idle_ratio",
    "trips_completed",
    "total_earnings",
    "avg_fare",
)

ZONE_METRIC_COLUMNS: tuple[str, ...] = (
    "window_start",
    "zone",
    "window_end",
    "business_date",
    "vehicles_reporting",
    "active_vehicles",
    "event_count",
    "trips_completed",
    "total_earnings",
    "avg_fare",
)


def metric_rows(metrics: DataFrame, columns: tuple[str, ...]) -> DataFrame:
    """Metrics shaped for PostgreSQL: window timestamps as ISO strings, date as text."""
    return metrics.select(*_for_db(columns, ("window_start", "window_end")))
