"""Batch transforms on hand-made static DataFrames with exactly known results."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest

pytestmark = pytest.mark.spark

T = pytest.importorskip("fleet.batch.transforms", reason="pyspark not installed")

DAY = date(2099, 3, 1)
EVENT_SCHEMA = (
    "vehicle_id string, status string, speed double, fare decimal(10,2), event_timestamp timestamp"
)
EXPENSE_SCHEMA = (
    "vehicle_id string, business_date date, fuel_cost decimal(10,2),"
    " maintenance_cost decimal(10,2), distance_covered decimal(10,2), service_flag boolean"
)


def ts(hhmm: str) -> datetime:
    h, m = hhmm.split(":")
    return datetime(2099, 3, 1, int(h), int(m))


def ev(vehicle, status, hhmm, speed=0.0, fare="0"):
    return (vehicle, status, float(speed), Decimal(fare), ts(hhmm))


# Deliberately NOT in time order: the window must sort by event time.
EVENTS = [
    # V901: 6 events, 4 on_trip, 2 trips, earnings 450.00
    ev("V901", "on_trip", "09:04", 45, "199.50"),  # 4 min after 09:00 -> 45 * 4/60 = 3.0 km
    ev("V901", "idle", "08:00"),  # first event: no previous -> 0 km
    ev("V901", "enroute", "08:05", 30),  # 30 km/h * 5 min = 2.5 km
    ev("V901", "on_trip", "08:10", 60),  # 60 * 5/60 = 5.0 km
    ev("V901", "on_trip", "08:15", 48, "250.50"),  # 48 * 5/60 = 4.0 km
    ev("V901", "on_trip", "09:00", 40),  # 45 min gap > 30 -> ignored (would be 30 km)
    # V902: idle all day
    ev("V902", "idle", "08:00"),
    ev("V902", "idle", "08:05"),
    # V904: events but NO expense row
    ev("V904", "on_trip", "10:00", 30, "1000.00"),
    # V905: profit exactly on PROFITABLE_MIN
    ev("V905", "on_trip", "11:00", 30, "1000.00"),
]

EXPENSES = [
    ("V901", DAY, Decimal("100.00"), Decimal("50.00"), Decimal("20.00"), False),
    ("V902", DAY, Decimal("20.00"), Decimal("30.00"), Decimal("5.00"), False),
    ("V903", DAY, Decimal("10.00"), Decimal("5.00"), Decimal("0.00"), True),  # no events
    ("V905", DAY, Decimal("150.00"), Decimal("50.00"), Decimal("7.50"), False),
]


@pytest.fixture
def events(spark):
    return spark.createDataFrame(EVENTS, EVENT_SCHEMA)


@pytest.fixture
def expenses(spark):
    return spark.createDataFrame(EXPENSES, EXPENSE_SCHEMA)


def test_vehicle_day_aggregates(events):
    rows = {r.vehicle_id: r for r in T.vehicle_day_aggregates(events, 30).collect()}
    v = rows["V901"]
    assert (v.event_count, v.on_trip_event_count, v.trips) == (6, 4, 2)
    assert v.earnings == Decimal("450.00")
    assert v.stream_distance_km == pytest.approx(2.5 + 5.0 + 4.0 + 3.0)
    v2 = rows["V902"]
    assert (v2.event_count, v2.on_trip_event_count, v2.trips, v2.earnings) == (2, 0, 0, 0)
    assert v2.stream_distance_km == 0.0


def test_gap_threshold_is_configurable(events):
    rows = {r.vehicle_id: r for r in T.vehicle_day_aggregates(events, 60).collect()}
    # with a 60 min limit the 45 min gap counts: + 40 km/h * 0.75 h = 30 km
    assert rows["V901"].stream_distance_km == pytest.approx(14.5 + 30.0)


def test_reconcile_exact_values(events, expenses):
    agg = T.vehicle_day_aggregates(events, 30)
    result, missing = T.reconcile(agg, expenses, DAY, profitable_min=800.0, watch_min=0.0)
    assert tuple(result.columns) == T.PROFITABILITY_COLUMNS
    rows = {r.vehicle_id: r.asDict() for r in result.collect()}
    assert sorted(rows) == ["V901", "V902", "V903", "V905"]

    assert rows["V901"] == {
        "vehicle_id": "V901",
        "business_date": DAY,
        "trips": 2,
        "event_count": 6,
        "on_trip_event_count": 4,
        "stream_distance_km": Decimal("14.50"),
        "distance_km": Decimal("20.00"),
        "earnings": Decimal("450.00"),
        "fuel_cost": Decimal("100.00"),
        "maintenance_cost": Decimal("50.00"),
        "total_operating_cost": Decimal("150.00"),
        "estimated_profit": Decimal("300.00"),
        "utilization_rate": pytest.approx(4 / 6),
        "service_flag": False,
        "profitability_status": "watch",
    }
    v2 = rows["V902"]
    assert (v2["estimated_profit"], v2["profitability_status"]) == (
        Decimal("-50.00"),
        "unprofitable",
    )
    assert v2["utilization_rate"] == 0.0

    # expense row but no events: zero trips / earnings / utilisation, costs kept
    v3 = rows["V903"]
    assert (v3["trips"], v3["event_count"], v3["earnings"], v3["utilization_rate"]) == (
        0,
        0,
        Decimal("0.00"),
        0.0,
    )
    assert v3["estimated_profit"] == Decimal("-15.00")
    assert v3["service_flag"] is True
    assert v3["profitability_status"] == "unprofitable"

    # exactly on the threshold -> profitable
    v5 = rows["V905"]
    assert v5["estimated_profit"] == Decimal("800.00")
    assert v5["profitability_status"] == "profitable"
    assert v5["utilization_rate"] == 1.0

    # events but no expense row: reported separately, not in the result
    assert [(r.vehicle_id, r.event_count) for r in missing.collect()] == [("V904", 1)]


def test_clean_expenses_trims_casts_and_rejects(spark):
    raw = spark.createDataFrame(
        [
            (" v001 ", " 1650.20", "230", "250.5 ", " TRUE ", "2099-03-01"),
            ("V002", "abc", "230", "250", "false", "2099-03-01"),  # bad number
            ("V003", "10", "-1", "250", "false", "2099-03-01"),  # negative
            ("V004", "10", "1", "250", "maybe", "2099-03-01"),  # bad flag
            ("V005", "10", "1", "250", "false", "2099-03-02"),  # other date
            ("V006", "10", None, "250", "false", "2099-03-01"),  # missing
        ],
        T.EXPENSE_CSV_SCHEMA,
    )
    clean, rejected = T.clean_expenses(raw, DAY, "/landing/x.csv")
    rows = clean.collect()
    assert len(rows) == 1 and rejected.count() == 5
    r = rows[0].asDict()
    assert r == {
        "vehicle_id": "V001",
        "business_date": DAY,
        "fuel_cost": Decimal("1650.20"),
        "maintenance_cost": Decimal("230.00"),
        "distance_covered": Decimal("250.50"),
        "service_flag": True,
        "source_file": "/landing/x.csv",
    }
