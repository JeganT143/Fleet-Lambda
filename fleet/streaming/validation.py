"""Streaming event validation: one rule set, two implementations.

(a) `validate_raw` / `validate_event`: pure Python, used by tests and tooling.
(b) `spark_reasons_column`: a Spark column expression used by the streaming job.

Both return the SAME reason codes, in the SAME order, built from the same
constants (fleet/common/contracts.py). A unit test runs both on identical
fixtures and compares the results.

Reason codes (an event is valid when the list is empty):

    malformed_json         value is not a JSON object (then no other rule is checked)
    missing_<field>        a required field is absent or null (contracts.EVENT_REQUIRED_FIELDS)
    invalid_event_id       event_id is not a UUID (stream_events.event_id is a UUID column)
    invalid_vehicle_id     vehicle_id does not match VEHICLE_ID_PATTERN
    invalid_driver_id      driver_id does not match DRIVER_ID_PATTERN
    invalid_status         status not in VALID_STATUSES
    invalid_speed          speed not a number in [0, MAX_SPEED_KMH]
    invalid_fare           fare not a number in [0, MAX_FARE]
    invalid_coordinates    latitude / longitude not numbers inside LATITUDE_RANGE / LONGITUDE_RANGE
    invalid_timestamp      timestamp not an ISO-8601 date-time that parses

A field that is missing only produces missing_<field>, never also invalid_<field>.

How Spark sees the JSON: every field is read as a STRING (a JSON number 12.5 becomes
"12.5", true becomes "true") and then cast. The Python side mirrors this: non-string
values are turned into their JSON text, and numbers are parsed with float().
"""

from __future__ import annotations

import json
import re
from datetime import datetime

from fleet.common.contracts import (
    DRIVER_ID_PATTERN,
    EVENT_REQUIRED_FIELDS,
    LATITUDE_RANGE,
    LONGITUDE_RANGE,
    MAX_FARE,
    MAX_SPEED_KMH,
    VALID_STATUSES,
    VEHICLE_ID_PATTERN,
)

MALFORMED_JSON = "malformed_json"
INVALID_EVENT_ID = "invalid_event_id"
INVALID_VEHICLE_ID = "invalid_vehicle_id"
INVALID_DRIVER_ID = "invalid_driver_id"
INVALID_STATUS = "invalid_status"
INVALID_SPEED = "invalid_speed"
INVALID_FARE = "invalid_fare"
INVALID_COORDINATES = "invalid_coordinates"
INVALID_TIMESTAMP = "invalid_timestamp"

UUID_PATTERN = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
# ISO-8601 date-time, optional fraction and UTC offset ("2026-01-01T08:24:00.000Z")
TIMESTAMP_PATTERN = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?(Z|[+-]\d{2}:\d{2})?$"


def missing(field: str) -> str:
    return f"missing_{field}"


# All reason codes in the order they are reported (used for data-quality counters)
ALL_REASONS: tuple[str, ...] = (
    (MALFORMED_JSON,)
    + tuple(missing(f) for f in EVENT_REQUIRED_FIELDS)
    + (
        INVALID_EVENT_ID,
        INVALID_VEHICLE_ID,
        INVALID_DRIVER_ID,
        INVALID_STATUS,
        INVALID_SPEED,
        INVALID_FARE,
        INVALID_COORDINATES,
        INVALID_TIMESTAMP,
    )
)


# ---------------------------------------------------------------------------
# (a) pure Python
# ---------------------------------------------------------------------------
def _text(value) -> str | None:
    """A field as Spark reads it with a STRING schema: strings as-is, others as JSON text."""
    if value is None:
        return None
    return value if isinstance(value, str) else json.dumps(value)


def _number(value) -> float | None:
    """Parse like Spark's CAST(string AS DOUBLE); None when it is not a number."""
    text = _text(value)
    if text is None:
        return None
    try:
        return float(text.strip())
    except ValueError:
        return None


def _in_range(value, low: float, high: float) -> bool:
    number = _number(value)
    # NaN fails both comparisons, so it is rejected too
    return number is not None and low <= number <= high


def _matches(value, pattern: str) -> bool:
    return re.search(pattern, _text(value)) is not None


def _parses_as_timestamp(value) -> bool:
    text = _text(value)
    if not re.search(TIMESTAMP_PATTERN, text):
        return False
    try:
        datetime.fromisoformat(text)
    except ValueError:
        return False
    return True


def validate_event(event) -> list[str]:
    """Reason codes for a decoded event (a dict); empty list = valid."""
    if not isinstance(event, dict):
        return [MALFORMED_JSON]

    reasons = [missing(f) for f in EVENT_REQUIRED_FIELDS if event.get(f) is None]

    def present(field: str) -> bool:
        return event.get(field) is not None

    if present("event_id") and not _matches(event["event_id"], UUID_PATTERN):
        reasons.append(INVALID_EVENT_ID)
    if present("vehicle_id") and not _matches(event["vehicle_id"], VEHICLE_ID_PATTERN):
        reasons.append(INVALID_VEHICLE_ID)
    if present("driver_id") and not _matches(event["driver_id"], DRIVER_ID_PATTERN):
        reasons.append(INVALID_DRIVER_ID)
    if present("status") and _text(event["status"]) not in VALID_STATUSES:
        reasons.append(INVALID_STATUS)
    if present("speed") and not _in_range(event["speed"], 0.0, MAX_SPEED_KMH):
        reasons.append(INVALID_SPEED)
    if present("fare") and not _in_range(event["fare"], 0.0, MAX_FARE):
        reasons.append(INVALID_FARE)
    lat_bad = present("latitude") and not _in_range(event["latitude"], *LATITUDE_RANGE)
    lon_bad = present("longitude") and not _in_range(event["longitude"], *LONGITUDE_RANGE)
    if lat_bad or lon_bad:
        reasons.append(INVALID_COORDINATES)
    if present("timestamp") and not _parses_as_timestamp(event["timestamp"]):
        reasons.append(INVALID_TIMESTAMP)
    return reasons


def validate_raw(raw: str | bytes | None) -> list[str]:
    """Reason codes for a raw Kafka value (JSON text); empty list = valid."""
    if raw is None:
        return [MALFORMED_JSON]
    try:
        event = json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        return [MALFORMED_JSON]
    return validate_event(event)


# ---------------------------------------------------------------------------
# (b) Spark column expression (pyspark imported lazily: the producer image path
#     and pure-Python tests do not need Spark)
# ---------------------------------------------------------------------------
CORRUPT_RECORD_COLUMN = "_corrupt_record"


def spark_raw_schema():
    """Every contract field as STRING, plus Spark's corrupt-record column."""
    from pyspark.sql.types import StringType, StructField, StructType

    from fleet.common.contracts import EVENT_FIELDS

    fields = [StructField(f, StringType()) for f in EVENT_FIELDS]
    fields.append(StructField(CORRUPT_RECORD_COLUMN, StringType()))
    return StructType(fields)


def spark_parse_json(value_col):
    """from_json with the raw schema. Malformed input -> struct with _corrupt_record set,
    null input -> null struct (both are treated as malformed_json)."""
    from pyspark.sql import functions as F

    return F.from_json(
        value_col, spark_raw_schema(), {"columnNameOfCorruptRecord": CORRUPT_RECORD_COLUMN}
    )


def spark_reasons_column(parsed_col):
    """array<string> of reason codes for a parsed struct column (see spark_parse_json)."""
    from pyspark.sql import functions as F

    def field(name: str):
        return parsed_col.getField(name)

    def present(name: str):
        return field(name).isNotNull()

    def in_range(name: str, low: float, high: float):
        # CAST fails -> null -> the whole condition is not TRUE -> invalid.
        # Spark treats NaN as larger than any number, so NaN fails "<= high".
        number = field(name).cast("double")
        return F.coalesce((number >= F.lit(low)) & (number <= F.lit(high)), F.lit(False))

    def not_matching(name: str, pattern: str):
        return present(name) & ~field(name).rlike(pattern)

    def out_of_range(name: str, low: float, high: float):
        return present(name) & ~in_range(name, low, high)

    timestamp_ok = (
        field("timestamp").rlike(TIMESTAMP_PATTERN)
        & field("timestamp").cast("timestamp").isNotNull()
    )

    rules = [(~present(f), missing(f)) for f in EVENT_REQUIRED_FIELDS] + [
        (not_matching("event_id", UUID_PATTERN), INVALID_EVENT_ID),
        (not_matching("vehicle_id", VEHICLE_ID_PATTERN), INVALID_VEHICLE_ID),
        (not_matching("driver_id", DRIVER_ID_PATTERN), INVALID_DRIVER_ID),
        (present("status") & ~field("status").isin(*VALID_STATUSES), INVALID_STATUS),
        (out_of_range("speed", 0.0, MAX_SPEED_KMH), INVALID_SPEED),
        (out_of_range("fare", 0.0, MAX_FARE), INVALID_FARE),
        (
            out_of_range("latitude", *LATITUDE_RANGE) | out_of_range("longitude", *LONGITUDE_RANGE),
            INVALID_COORDINATES,
        ),
        (present("timestamp") & ~timestamp_ok, INVALID_TIMESTAMP),
    ]
    # One slot per rule: the code when the rule fails, else null; nulls are then removed.
    checks = F.array(*[F.when(condition, F.lit(code)) for condition, code in rules])
    field_reasons = F.filter(checks, lambda code: code.isNotNull())

    malformed = parsed_col.isNull() | field(CORRUPT_RECORD_COLUMN).isNotNull()
    return F.when(malformed, F.array(F.lit(MALFORMED_JSON))).otherwise(field_reasons)
