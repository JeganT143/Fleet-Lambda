"""Deterministic vehicle telemetry simulator (pure Python, no Kafka).

Every call to `TelemetrySimulator.tick()` advances the fleet by one tick and returns
one event per vehicle, shaped exactly like the streaming contract
(`fleet.common.contracts.EVENT_FIELDS`).

Trip lifecycle of a vehicle (each arrow is one tick):

    idle ... idle -> enroute x (1-3) -> on_trip x (2-6) -> idle ...
                     \\____________ same trip_id ____________/

- A trip starts when an idle vehicle receives a ride request. The chance per tick is
  BASE_REQUEST_PROBABILITY x hourly demand (lower at night, peaks in rush hours)
  x the vehicle's profile demand_factor (fleet_profiles.py).
- `enroute` = driving to the pickup point, `on_trip` = passenger on board.
- The trip's full fare is reported ONCE, on the last on_trip event; every other
  event has fare 0 (contracts.py). trip_id is None while idle.
- After a trip ends the vehicle reports at least one idle event before the next trip,
  so the lifecycle order is always idle -> enroute -> on_trip -> idle.
- Positions move by speed x simulated tick duration toward a target point inside the
  city bounding box (zones.py). The box is convex and targets are inside it, so the
  vehicle never leaves the box.

Determinism: all behaviour comes from a `random.Random(seed)`. Event and trip ids
come from a second RNG (`id_seed`, defaults to `seed`) so tests are fully
reproducible while the real producer can use fresh ids on every run.
"""

from __future__ import annotations

import math
import random
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from fleet.common.contracts import (
    STATUS_ENROUTE,
    STATUS_IDLE,
    STATUS_ON_TRIP,
    driver_id,
    vehicle_id,
)
from fleet.common.fleet_profiles import profile_for
from fleet.common.simclock import to_iso
from fleet.common.zones import CITY_LAT_MAX, CITY_LAT_MIN, CITY_LON_MAX, CITY_LON_MIN

# ---------------------------------------------------------------------------
# Tuning constants (tuned so one simulated day gives ~INR 3000-5000 per normal
# vehicle and ~900-1800 per low-utilisation vehicle; see tests / report)
# ---------------------------------------------------------------------------
# Ride-request chance per idle tick at hourly demand 1.0 and demand_factor 1.0
BASE_REQUEST_PROBABILITY = 0.10

# Relative ride demand per simulated hour of day (index = hour, UTC)
HOURLY_DEMAND: tuple[float, ...] = (
    0.15, 0.10, 0.10, 0.10, 0.15, 0.30,  # 00-05 night
    0.60, 0.90, 1.40, 1.40, 1.00, 0.90,  # 06-11 morning rush 08-09
    1.00, 1.00, 0.90, 0.90, 1.00, 1.50,  # 12-17
    1.60, 1.50, 1.10, 0.90, 0.60, 0.35,  # 18-23 evening rush 17-19
)  # fmt: skip
RUSH_HOURS = frozenset({8, 9, 17, 18, 19})
RUSH_SPEED_SHARE = 0.6  # in rush hours only the lower 60% of the speed range is used

# Ticks spent in each phase of a trip (inclusive ranges)
ENROUTE_TICKS = (1, 3)
ON_TRIP_TICKS = (2, 6)

# Speed ranges in km/h (idle vehicles are parked: speed 0)
ENROUTE_SPEED_KMH = (15.0, 45.0)
ON_TRIP_SPEED_KMH = (15.0, 60.0)

# Fare = base + per-km + per-minute (INR), with a minimum fare
FARE_BASE = 50.0
FARE_PER_KM = 15.0
FARE_PER_MINUTE = 2.0
FARE_MINIMUM = 100.0

# Pickups are near the vehicle: offset of up to this many degrees (~3 km)
PICKUP_RADIUS_DEG = 0.03

# Flat-earth conversion, good enough inside one city (Chennai ~13 deg N)
KM_PER_DEG_LAT = 111.0
KM_PER_DEG_LON = 111.0 * math.cos(math.radians((CITY_LAT_MIN + CITY_LAT_MAX) / 2))


class Clock(Protocol):
    """Anything with now() -> aware datetime (fleet.common.simclock.SimClock)."""

    def now(self) -> datetime: ...


def compute_fare(distance_km: float, duration_minutes: float) -> float:
    """Trip fare in INR: base + per-km + per-minute, never below the minimum fare."""
    fare = FARE_BASE + FARE_PER_KM * distance_km + FARE_PER_MINUTE * duration_minutes
    return round(max(fare, FARE_MINIMUM), 2)


def request_probability(hour: int, demand_factor: float) -> float:
    """Chance that an idle vehicle gets a ride request during one tick."""
    return min(1.0, BASE_REQUEST_PROBABILITY * HOURLY_DEMAND[hour] * demand_factor)


@dataclass
class VehicleState:
    vehicle_id: str
    driver_id: str
    demand_factor: float
    lat: float
    lon: float
    status: str = STATUS_IDLE
    last_emitted_status: str | None = None  # None before the first event
    trip_id: str | None = None
    ticks_left: int = 0  # ticks remaining in the current enroute / on_trip phase
    on_trip_ticks: int = 0  # planned length of the on_trip phase
    trip_km: float = 0.0  # distance driven with the passenger on board
    target_lat: float = 0.0
    target_lon: float = 0.0


class TelemetrySimulator:
    def __init__(
        self,
        fleet_size: int,
        clock: Clock,
        tick_sim_seconds: float,
        seed: int,
        id_seed: int | None = None,
    ) -> None:
        """
        fleet_size        vehicles V001..V<fleet_size>, driver D00n drives V00n
        clock             provides the simulated timestamp of each tick
        tick_sim_seconds  simulated seconds between ticks
                          (STREAM_INTERVAL_SECONDS x sim speed-up = 288 s by default)
        seed              seeds all behaviour (positions, demand, speeds, durations)
        id_seed           seeds event/trip ids; None -> same as seed
        """
        self.clock = clock
        self.tick_sim_seconds = tick_sim_seconds
        self.rng = random.Random(seed)
        self.id_rng = random.Random(seed if id_seed is None else id_seed)
        self.vehicles: list[VehicleState] = []
        for n in range(1, fleet_size + 1):
            vid = vehicle_id(n)
            self.vehicles.append(
                VehicleState(
                    vehicle_id=vid,
                    driver_id=driver_id(n),
                    demand_factor=profile_for(vid).demand_factor,
                    lat=self.rng.uniform(CITY_LAT_MIN, CITY_LAT_MAX),
                    lon=self.rng.uniform(CITY_LON_MIN, CITY_LON_MAX),
                )
            )

    # ------------------------------------------------------------------ public
    def tick(self) -> list[dict]:
        """Advance every vehicle by one tick and return one event per vehicle."""
        now = self.clock.now()
        timestamp = to_iso(now)
        return [self._step(v, now.hour, timestamp) for v in self.vehicles]

    # ------------------------------------------------------------------ internals
    def _new_id(self) -> str:
        return str(uuid.UUID(int=self.id_rng.getrandbits(128), version=4))

    def _speed(self, speed_range: tuple[float, float], hour: int) -> float:
        low, high = speed_range
        if hour in RUSH_HOURS:  # congestion: only the slow part of the range
            high = low + (high - low) * RUSH_SPEED_SHARE
        return self.rng.uniform(low, high)

    def _random_point(self) -> tuple[float, float]:
        return (
            self.rng.uniform(CITY_LAT_MIN, CITY_LAT_MAX),
            self.rng.uniform(CITY_LON_MIN, CITY_LON_MAX),
        )

    def _nearby_point(self, lat: float, lon: float) -> tuple[float, float]:
        r = PICKUP_RADIUS_DEG
        new_lat = min(max(lat + self.rng.uniform(-r, r), CITY_LAT_MIN), CITY_LAT_MAX)
        new_lon = min(max(lon + self.rng.uniform(-r, r), CITY_LON_MIN), CITY_LON_MAX)
        return new_lat, new_lon

    def _move(self, v: VehicleState, speed_kmh: float) -> float:
        """Drive toward the target for one tick; returns km driven (speed x tick time)."""
        step_km = speed_kmh * self.tick_sim_seconds / 3600.0
        d_lat_km = (v.target_lat - v.lat) * KM_PER_DEG_LAT
        d_lon_km = (v.target_lon - v.lon) * KM_PER_DEG_LON
        dist_km = math.hypot(d_lat_km, d_lon_km)
        if dist_km <= step_km:
            # Target reached within this tick: stop there and pick a new point to
            # keep driving toward (the passenger's route is not a straight line).
            v.lat, v.lon = v.target_lat, v.target_lon
            v.target_lat, v.target_lon = self._random_point()
        else:
            share = step_km / dist_km
            v.lat += (v.target_lat - v.lat) * share
            v.lon += (v.target_lon - v.lon) * share
        # Distance is speed x time, the same estimate the batch layer uses.
        return step_km

    def _step(self, v: VehicleState, hour: int, timestamp: str) -> dict:
        speed = 0.0
        fare = 0.0

        # 1) State transition at the start of the tick
        if v.status == STATUS_IDLE and v.last_emitted_status == STATUS_IDLE:
            # Only a vehicle that already reported idle can get a new ride request.
            if self.rng.random() < request_probability(hour, v.demand_factor):
                v.status = STATUS_ENROUTE
                v.trip_id = self._new_id()
                v.ticks_left = self.rng.randint(*ENROUTE_TICKS)
                v.target_lat, v.target_lon = self._nearby_point(v.lat, v.lon)
        elif v.status == STATUS_ENROUTE and v.ticks_left == 0:
            # Arrived at the pickup: passenger on board, drive to a drop-off point.
            v.status = STATUS_ON_TRIP
            v.on_trip_ticks = self.rng.randint(*ON_TRIP_TICKS)
            v.ticks_left = v.on_trip_ticks
            v.trip_km = 0.0
            v.target_lat, v.target_lon = self._random_point()

        # 2) Movement during the tick
        if v.status == STATUS_ENROUTE:
            speed = self._speed(ENROUTE_SPEED_KMH, hour)
            self._move(v, speed)
            v.ticks_left -= 1
        elif v.status == STATUS_ON_TRIP:
            speed = self._speed(ON_TRIP_SPEED_KMH, hour)
            v.trip_km += self._move(v, speed)
            v.ticks_left -= 1
            if v.ticks_left == 0:
                # Last on_trip event: report the whole trip's fare exactly once.
                minutes = v.on_trip_ticks * self.tick_sim_seconds / 60.0
                fare = compute_fare(v.trip_km, minutes)

        event = {
            "event_id": self._new_id(),
            "trip_id": v.trip_id,
            "driver_id": v.driver_id,
            "vehicle_id": v.vehicle_id,
            "latitude": round(v.lat, 6),
            "longitude": round(v.lon, 6),
            "speed": round(speed, 1),
            "status": v.status,
            "fare": fare,
            "timestamp": timestamp,
        }
        v.last_emitted_status = v.status

        # 3) Trip finished -> vehicle is idle from the next tick on
        if v.status == STATUS_ON_TRIP and v.ticks_left == 0:
            v.status = STATUS_IDLE
            v.trip_id = None
        return event
