"""Unit tests for the telemetry simulator and the invalid-record injector (no Kafka)."""

from __future__ import annotations

import json
import random
import statistics
from collections import defaultdict
from datetime import date, datetime

import pytest

from fleet.common.contracts import (
    EVENT_FIELDS,
    STATUS_ENROUTE,
    STATUS_IDLE,
    STATUS_ON_TRIP,
)
from fleet.common.fleet_profiles import LOW_UTILIZATION, NORMAL, profile_for
from fleet.common.simclock import SimClock, business_date_of, local_time
from fleet.common.zones import CITY_LAT_MAX, CITY_LAT_MIN, CITY_LON_MAX, CITY_LON_MIN
from fleet.producer import simulator as sim_mod
from fleet.producer.faults import EXPECTED_REASON, FAULT_KINDS, make_invalid_record
from fleet.producer.simulator import TelemetrySimulator, compute_fare
from fleet.streaming.validation import validate_event, validate_raw

TICK_SIM_SECONDS = 288.0  # STREAM_INTERVAL_SECONDS=1 x speed-up 288
TICKS_PER_DAY = 300


class ManualTime:
    """Fake real-time source: advances only when the test says so."""

    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


def run_ticks(ticks: int, fleet_size: int = 20, seed: int = 42) -> list[dict]:
    real = ManualTime()
    clock = SimClock(date(2026, 1, 1), 300.0, real_now=real)
    sim = TelemetrySimulator(fleet_size, clock, TICK_SIM_SECONDS, seed=seed)
    events = []
    for _ in range(ticks):
        events.extend(sim.tick())
        real.t += 1.0  # one real second per tick
    return events


@pytest.fixture(scope="module")
def day_events() -> list[dict]:
    return run_ticks(TICKS_PER_DAY)


def by_vehicle(events: list[dict]) -> dict[str, list[dict]]:
    grouped = defaultdict(list)
    for e in events:
        grouped[e["vehicle_id"]].append(e)
    return grouped


def test_one_event_per_vehicle_per_tick_with_contract_fields():
    events = run_ticks(1, fleet_size=5)
    assert [e["vehicle_id"] for e in events] == ["V001", "V002", "V003", "V004", "V005"]
    for e in events:
        assert tuple(e) == EVENT_FIELDS
        assert e["driver_id"] == "D" + e["vehicle_id"][1:]  # fixed driver per vehicle
        assert validate_event(e) == []
        json.dumps(e)  # serialisable


def test_every_generated_event_is_valid(day_events):
    assert len(day_events) == 20 * TICKS_PER_DAY
    assert all(validate_event(e) == [] for e in day_events)
    assert len({e["event_id"] for e in day_events}) == len(day_events)  # unique ids


def test_timestamps_are_simulated_time(day_events):
    first = [e for e in day_events if e["vehicle_id"] == "V001"]
    # the day starts at 00:00 in Colombo (UTC+05:30) = 18:30 UTC the evening before
    assert first[0]["timestamp"] == "2025-12-31T18:30:00.000Z"
    assert first[1]["timestamp"] == "2025-12-31T18:34:48.000Z"  # +288 simulated seconds
    assert first[-1]["timestamp"].startswith("2026-01-01T18:25:12")  # 23:55:12 in Colombo
    # one full Sri Lankan business day, no more, no less
    assert {business_date_of(datetime.fromisoformat(e["timestamp"])) for e in first} == {
        date(2026, 1, 1)
    }


ALLOWED_TRANSITIONS = {
    (STATUS_IDLE, STATUS_IDLE),
    (STATUS_IDLE, STATUS_ENROUTE),
    (STATUS_ENROUTE, STATUS_ENROUTE),
    (STATUS_ENROUTE, STATUS_ON_TRIP),
    (STATUS_ON_TRIP, STATUS_ON_TRIP),
    (STATUS_ON_TRIP, STATUS_IDLE),
}


def test_lifecycle_order_and_phase_lengths(day_events):
    for vid, events in by_vehicle(day_events).items():
        statuses = [e["status"] for e in events]
        assert statuses[0] == STATUS_IDLE, vid
        for a, b in zip(statuses, statuses[1:], strict=False):
            assert (a, b) in ALLOWED_TRANSITIONS, (vid, a, b)
        # phase lengths per trip
        trips = defaultdict(list)
        for e in events:
            if e["trip_id"]:
                trips[e["trip_id"]].append(e["status"])
        unfinished_trip = events[-1]["trip_id"]  # still running when the run ends
        for trip_id, trip_statuses in trips.items():
            n_enroute = trip_statuses.count(STATUS_ENROUTE)
            n_on_trip = trip_statuses.count(STATUS_ON_TRIP)
            if trip_id != unfinished_trip:
                assert 1 <= n_enroute <= 3
                assert 2 <= n_on_trip <= 6
            assert trip_statuses == sorted(trip_statuses)  # "enroute" < "on_trip"


def test_trip_id_rules(day_events):
    seen_trips = set()
    for events in by_vehicle(day_events).values():
        current = None
        for e in events:
            if e["status"] == STATUS_IDLE:
                assert e["trip_id"] is None
                current = None
            else:
                assert e["trip_id"] is not None
                if current is None:  # a new trip starts with enroute and a new id
                    assert e["status"] == STATUS_ENROUTE
                    assert e["trip_id"] not in seen_trips
                    current = e["trip_id"]
                    seen_trips.add(current)
                assert e["trip_id"] == current  # same id on enroute and on_trip events
    assert len(seen_trips) > 100


def test_fare_only_on_last_on_trip_event(day_events):
    for events in by_vehicle(day_events).values():
        for i, e in enumerate(events):
            nxt = events[i + 1] if i + 1 < len(events) else None
            is_last_on_trip = (
                e["status"] == STATUS_ON_TRIP and nxt is not None and nxt["status"] == STATUS_IDLE
            )
            if is_last_on_trip:
                assert e["fare"] > 0
            elif nxt is not None:  # the final event of the run may be a trip's last one
                assert e["fare"] == 0
    fares = [e["fare"] for e in day_events if e["fare"] > 0]
    trips = {e["trip_id"] for e in day_events if e["fare"] > 0}
    assert len(fares) == len(trips)  # exactly one fare per trip
    # LKR: minimum fare 320; a typical trip costs a few hundred to ~1,500 rupees
    assert min(fares) >= sim_mod.FARE_MINIMUM and max(fares) <= 1920
    assert 640 <= statistics.mean(fares) <= 1024


def test_same_seed_is_deterministic_and_other_seed_differs():
    assert run_ticks(30, seed=7) == run_ticks(30, seed=7)
    assert run_ticks(30, seed=7) != run_ticks(30, seed=8)


def test_id_seed_changes_ids_but_not_behaviour():
    def run(id_seed):
        real = ManualTime()
        clock = SimClock(date(2026, 1, 1), 300.0, real_now=real)
        sim = TelemetrySimulator(5, clock, TICK_SIM_SECONDS, seed=1, id_seed=id_seed)
        out = []
        for _ in range(40):
            out.extend(sim.tick())
            real.t += 1.0
        return out

    a, b = run(100), run(200)
    strip = [{k: v for k, v in e.items() if k not in ("event_id", "trip_id")} for e in a]
    assert strip == [{k: v for k, v in e.items() if k not in ("event_id", "trip_id")} for e in b]
    assert {e["event_id"] for e in a}.isdisjoint({e["event_id"] for e in b})


def test_coordinates_stay_in_city_bbox(day_events):
    for e in day_events:
        assert CITY_LAT_MIN <= e["latitude"] <= CITY_LAT_MAX
        assert CITY_LON_MIN <= e["longitude"] <= CITY_LON_MAX


def test_speeds_per_status(day_events):
    for e in day_events:
        if e["status"] == STATUS_IDLE:
            assert e["speed"] == 0
        elif e["status"] == STATUS_ENROUTE:
            assert 15 <= e["speed"] <= 45
        else:
            assert 15 <= e["speed"] <= 60


def test_positions_move_by_speed_times_tick_duration(day_events):
    step_limit_km = {}
    for events in by_vehicle(day_events).values():
        for prev, cur in zip(events, events[1:], strict=False):
            d_lat = (cur["latitude"] - prev["latitude"]) * sim_mod.KM_PER_DEG_LAT
            d_lon = (cur["longitude"] - prev["longitude"]) * sim_mod.KM_PER_DEG_LON
            moved = (d_lat**2 + d_lon**2) ** 0.5
            max_km = cur["speed"] * TICK_SIM_SECONDS / 3600
            assert moved <= max_km + 0.01  # never faster than its reported speed
            if cur["status"] == STATUS_IDLE:
                assert moved < 1e-9  # parked
            step_limit_km[cur["status"]] = max(step_limit_km.get(cur["status"], 0), moved)
    assert step_limit_km[STATUS_ON_TRIP] > 1.0  # vehicles really drive


def test_demand_is_lower_at_night(day_events):
    def trips_in_hours(hours):
        return sum(
            1
            for e in day_events
            if e["status"] == STATUS_ENROUTE
            and local_time(datetime.fromisoformat(e["timestamp"])).hour in hours  # Colombo
        )

    night = trips_in_hours(range(0, 5))
    evening_rush = trips_in_hours(range(17, 22))
    assert evening_rush > 3 * night


def test_daily_earnings_by_profile():
    """Average over several seeded days (single days are noisy)."""
    earnings = defaultdict(list)
    for seed in range(5):
        per_vehicle = defaultdict(float)
        for e in run_ticks(TICKS_PER_DAY, seed=seed):
            per_vehicle[e["vehicle_id"]] += e["fare"]
        for vid in (f"V{n:03d}" for n in range(1, 21)):
            earnings[profile_for(vid).name].append(per_vehicle[vid])
    # LKR per vehicle per day
    assert 9_600 <= statistics.mean(earnings[NORMAL]) <= 16_000
    assert 2_880 <= statistics.mean(earnings[LOW_UTILIZATION]) <= 5_760


def test_compute_fare():
    assert compute_fare(10.0, 20.0) == 160 + 48 * 10 + 6 * 20  # LKR 760
    assert compute_fare(0.1, 1.0) == sim_mod.FARE_MINIMUM


def test_request_probability_scales_with_hour_and_profile():
    assert sim_mod.request_probability(3, 1.0) < sim_mod.request_probability(18, 1.0)
    assert sim_mod.request_probability(18, 0.3) == pytest.approx(
        0.3 * sim_mod.request_probability(18, 1.0)
    )
    assert all(0 <= sim_mod.request_probability(h, 1.0) <= 1 for h in range(24))


# ---------------------------------------------------------------------------
# Invalid-record injection (quarantine demo)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("seed", range(12))
def test_injected_records_fail_validation_with_expected_reason(seed):
    template = run_ticks(1, fleet_size=1)[0]
    kind, value = make_invalid_record(random.Random(seed), template)
    assert kind in FAULT_KINDS
    reasons = validate_raw(value)
    assert EXPECTED_REASON[kind] in reasons
    assert validate_raw(json.dumps(template)) == []  # the template itself is untouched
