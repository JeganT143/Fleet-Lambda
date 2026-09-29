"""Deterministic vehicle profiles shared by the telemetry producer and the expense generator.

Both sources are independent systems, but they describe the same physical fleet, so a
"low utilisation" vehicle must get little demand in the stream AND low distance in the
expense file. The profile is a pure function of the vehicle number, so no coordination
or shared state is needed.

With FLEET_SIZE=20: V003/V013 low utilisation, V006/V016 high cost,
V009/V019 maintenance heavy, the rest normal.
"""

from __future__ import annotations

from dataclasses import dataclass

NORMAL = "normal"
LOW_UTILIZATION = "low_utilization"
HIGH_COST = "high_cost"
MAINTENANCE_HEAVY = "maintenance_heavy"

FUEL_PRICE_PER_LITRE = 102.0  # INR


@dataclass(frozen=True)
class VehicleProfile:
    name: str
    demand_factor: float  # multiplies the probability an idle vehicle gets a ride request
    fuel_km_per_litre: float  # fuel efficiency; lower = more expensive to run
    maintenance_factor: float  # multiplies routine maintenance cost
    service_probability: float  # daily chance of a (costly) service visit


_PROFILES = {
    NORMAL: VehicleProfile(NORMAL, 1.0, 14.0, 1.0, 0.05),
    LOW_UTILIZATION: VehicleProfile(LOW_UTILIZATION, 0.3, 14.0, 1.0, 0.05),
    HIGH_COST: VehicleProfile(HIGH_COST, 1.0, 6.5, 1.8, 0.10),
    MAINTENANCE_HEAVY: VehicleProfile(MAINTENANCE_HEAVY, 0.9, 12.0, 3.0, 0.60),
}


def _vehicle_number(vehicle_id: str) -> int:
    return int(vehicle_id.lstrip("V"))


def profile_for(vehicle_id: str) -> VehicleProfile:
    last_digit = _vehicle_number(vehicle_id) % 10
    if last_digit == 3:
        return _PROFILES[LOW_UTILIZATION]
    if last_digit == 6:
        return _PROFILES[HIGH_COST]
    if last_digit == 9:
        return _PROFILES[MAINTENANCE_HEAVY]
    return _PROFILES[NORMAL]
