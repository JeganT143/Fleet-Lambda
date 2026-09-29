"""Streaming validation: one case per rule in Python, and Spark must agree on every case."""

from __future__ import annotations

import json
import random

import pytest

from fleet.common.contracts import EVENT_REQUIRED_FIELDS, MAX_FARE, MAX_SPEED_KMH
from fleet.streaming import validation as V
from fleet.streaming.validation import validate_event, validate_raw

VALID_EVENT = {
    "event_id": "0b7c6f0e-8a52-4d7e-9c55-2f1f5d1c9e11",
    "trip_id": "5a3c2b1d-0e9f-4a8b-8c7d-6e5f4a3b2c1d",
    "driver_id": "D007",
    "vehicle_id": "V007",
    "latitude": 13.05,
    "longitude": 80.22,
    "speed": 32.5,
    "status": "on_trip",
    "fare": 245.5,
    "timestamp": "2026-01-01T08:24:00.000Z",
}


def event(**changes) -> str:
    return json.dumps({**VALID_EVENT, **changes})


def without(field: str) -> str:
    return json.dumps({k: v for k, v in VALID_EVENT.items() if k != field})


# (case id, raw Kafka value, expected reason codes)
CASES: list[tuple[str, str | None, list[str]]] = [
    ("valid", event(), []),
    ("valid_idle_null_trip", event(trip_id=None, status="idle", speed=0, fare=0), []),
    ("valid_missing_trip_id", without("trip_id"), []),
    ("valid_speed_at_max", event(speed=MAX_SPEED_KMH), []),
    ("valid_numbers_as_strings", event(speed="12.5", fare="100"), []),
    ("valid_offset_timestamp", event(timestamp="2026-01-01T13:54:00+05:30"), []),
    # malformed JSON
    ("malformed_truncated", event()[:40], [V.MALFORMED_JSON]),
    ("malformed_garbage", "not json at all", [V.MALFORMED_JSON]),
    ("malformed_array", "[1, 2, 3]", [V.MALFORMED_JSON]),
    ("malformed_scalar", "42", [V.MALFORMED_JSON]),
    ("malformed_json_null", "null", [V.MALFORMED_JSON]),
    ("malformed_kafka_null_value", None, [V.MALFORMED_JSON]),
    # required fields (one per field, plus null value and empty object)
    *[(f"missing_{f}", without(f), [V.missing(f)]) for f in EVENT_REQUIRED_FIELDS],
    ("null_vehicle_id", event(vehicle_id=None), [V.missing("vehicle_id")]),
    ("empty_object", "{}", [V.missing(f) for f in EVENT_REQUIRED_FIELDS]),
    # ids
    ("bad_event_id", event(event_id="evt-1"), [V.INVALID_EVENT_ID]),
    ("bad_vehicle_id", event(vehicle_id="X007"), [V.INVALID_VEHICLE_ID]),
    ("bad_vehicle_id_short", event(vehicle_id="V07"), [V.INVALID_VEHICLE_ID]),
    ("bad_vehicle_id_number", event(vehicle_id=7), [V.INVALID_VEHICLE_ID]),
    ("bad_driver_id", event(driver_id="DRV7"), [V.INVALID_DRIVER_ID]),
    # status
    ("unknown_status", event(status="teleporting"), [V.INVALID_STATUS]),
    ("status_wrong_case", event(status="IDLE"), [V.INVALID_STATUS]),
    # speed
    ("negative_speed", event(speed=-3.2), [V.INVALID_SPEED]),
    ("speed_too_high", event(speed=MAX_SPEED_KMH + 1), [V.INVALID_SPEED]),
    ("speed_not_a_number", event(speed="fast"), [V.INVALID_SPEED]),
    ("speed_boolean", event(speed=True), [V.INVALID_SPEED]),
    # fare
    ("negative_fare", event(fare=-10), [V.INVALID_FARE]),
    ("fare_too_high", event(fare=MAX_FARE + 1), [V.INVALID_FARE]),
    # coordinates
    ("latitude_out_of_range", event(latitude=95.0), [V.INVALID_COORDINATES]),
    ("longitude_out_of_range", event(longitude=-180.5), [V.INVALID_COORDINATES]),
    ("both_coordinates_bad", event(latitude=-91, longitude=181), [V.INVALID_COORDINATES]),
    ("latitude_not_a_number", event(latitude="north"), [V.INVALID_COORDINATES]),
    # timestamp
    ("timestamp_garbage", event(timestamp="yesterday"), [V.INVALID_TIMESTAMP]),
    ("timestamp_bad_month", event(timestamp="2026-13-01T00:00:00Z"), [V.INVALID_TIMESTAMP]),
    ("timestamp_date_only", event(timestamp="2026-01-01"), [V.INVALID_TIMESTAMP]),
    ("timestamp_epoch_number", event(timestamp=1767225600), [V.INVALID_TIMESTAMP]),
    # several problems at once: reported in rule order
    (
        "several",
        event(vehicle_id="bad", status="x", speed=-1, fare=-1),
        [V.INVALID_VEHICLE_ID, V.INVALID_STATUS, V.INVALID_SPEED, V.INVALID_FARE],
    ),
]


@pytest.mark.parametrize(("raw", "expected"), [c[1:] for c in CASES], ids=[c[0] for c in CASES])
def test_python_validation(raw, expected):
    assert validate_raw(raw) == expected


def test_validate_event_rejects_non_dict():
    assert validate_event(["not", "a", "dict"]) == [V.MALFORMED_JSON]


def test_all_reasons_lists_every_code_once():
    assert len(V.ALL_REASONS) == len(set(V.ALL_REASONS))
    produced = {r for _, raw, expected in CASES for r in expected}
    assert produced == set(V.ALL_REASONS)  # the fixtures exercise every rule


# ---------------------------------------------------------------------------
# Spark parity
# ---------------------------------------------------------------------------
def spark_reasons(spark, raws: list[str | None]) -> list[list[str]]:
    from pyspark.sql import functions as F

    df = spark.createDataFrame([(i, r) for i, r in enumerate(raws)], "i INT, value STRING")
    parsed = df.withColumn("parsed", V.spark_parse_json(F.col("value")))
    out = parsed.select("i", V.spark_reasons_column(F.col("parsed")).alias("reasons"))
    return [list(r.reasons) for r in out.orderBy("i").collect()]


@pytest.mark.spark
def test_spark_validation_matches_python_on_every_case(spark):
    raws = [raw for _, raw, _ in CASES]
    got = spark_reasons(spark, raws)
    for (case_id, raw, expected), spark_result in zip(CASES, got, strict=True):
        assert spark_result == validate_raw(raw) == expected, case_id


@pytest.mark.spark
def test_spark_validation_matches_python_on_random_mutations(spark):
    """Property-style check: random field mutations, both implementations must agree."""
    rng = random.Random(2099)
    bad_values = [None, -1, 0, 1e9, "abc", "", True, "V001", "D001", "idle", 13.0, "12.5"]
    raws = []
    for _ in range(300):
        candidate = dict(VALID_EVENT)
        for field in rng.sample(list(VALID_EVENT), rng.randint(1, 3)):
            if rng.random() < 0.2:
                candidate.pop(field)
            else:
                candidate[field] = rng.choice(bad_values)
        raws.append(json.dumps(candidate))
    got = spark_reasons(spark, raws)
    for raw, spark_result in zip(raws, got, strict=True):
        assert spark_result == validate_raw(raw), raw
