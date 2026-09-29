"""Daily expense generator: determinism, profiles, contract fields, file layout."""

from __future__ import annotations

import csv
import dataclasses
from datetime import date
from pathlib import Path
from statistics import mean

import pytest

from fleet.batch import generator as G
from fleet.batch.validation import validate_expense_file
from fleet.common.contracts import EXPENSE_FIELDS
from fleet.common.fleet_profiles import FUEL_PRICE_PER_LITRE, profile_for

DAY = date(2026, 1, 1)
DAYS = [date(2026, 1, d) for d in range(1, 29)]  # four weeks for averages


def test_same_seed_date_vehicle_gives_identical_rows():
    assert G.generate_expenses(DAY, 20, seed=42) == G.generate_expenses(DAY, 20, seed=42)
    # a vehicle's row does not depend on the fleet size (per-vehicle RNG)
    assert G.generate_expenses(DAY, 5, seed=42) == G.generate_expenses(DAY, 20, seed=42)[:5]


def test_different_date_or_seed_gives_different_rows():
    base = G.generate_expenses(DAY, 20, seed=42)
    assert G.generate_expenses(date(2026, 1, 2), 20, seed=42) != base
    assert G.generate_expenses(DAY, 20, seed=7) != base


def test_rows_have_all_contract_fields_and_valid_values():
    rows = G.generate_expenses(DAY, 20, seed=42)
    assert [r["vehicle_id"] for r in rows] == [f"V{n:03d}" for n in range(1, 21)]
    for r in rows:
        assert tuple(r) == EXPENSE_FIELDS
        assert r["business_date"] == "2026-01-01"
        assert r["service_flag"] in ("true", "false")
        for f in ("fuel_cost", "maintenance_cost", "distance_covered"):
            assert float(r[f]) >= 0


def _avg(vid: str, field: str) -> float:
    return mean(float(G.expense_row(d, vid, 42)[field]) for d in DAYS)


def test_profiles_are_reflected_in_costs():
    normal, low, high_cost, heavy = "V001", "V003", "V006", "V009"
    assert profile_for(low).name == "low_utilization"
    # low-utilisation vehicles drive much less
    assert _avg(low, "distance_covered") < 0.5 * _avg(normal, "distance_covered")
    # high-cost vehicles burn much more fuel per km
    per_km = lambda v: _avg(v, "fuel_cost") / _avg(v, "distance_covered")  # noqa: E731
    assert per_km(high_cost) > 1.8 * per_km(normal)
    # maintenance-heavy vehicles cost the most to maintain and are serviced often
    assert _avg(heavy, "maintenance_cost") > 3 * _avg(normal, "maintenance_cost")
    services = sum(G.expense_row(d, heavy, 42)["service_flag"] == "true" for d in DAYS)
    assert services >= len(DAYS) // 3


def test_fuel_cost_follows_distance_and_efficiency():
    for vid in ("V001", "V006"):
        p = profile_for(vid)
        for d in DAYS[:5]:
            r = G.expense_row(d, vid, 42)
            expected = float(r["distance_covered"]) / p.fuel_km_per_litre * FUEL_PRICE_PER_LITRE
            assert (
                G.FUEL_NOISE[0] - 0.01 <= float(r["fuel_cost"]) / expected <= G.FUEL_NOISE[1] + 0.01
            )


def test_service_day_adds_a_large_maintenance_cost():
    rows = [G.expense_row(d, "V009", 42) for d in DAYS]
    serviced = [float(r["maintenance_cost"]) for r in rows if r["service_flag"] == "true"]
    routine = [float(r["maintenance_cost"]) for r in rows if r["service_flag"] == "false"]
    assert serviced and routine
    assert min(serviced) > max(routine)


@pytest.fixture
def settings_tmp(settings, tmp_path):
    return dataclasses.replace(settings, landing_dir=str(tmp_path), fleet_size=20)


def test_generate_file_writes_valid_csv_at_contract_path(settings_tmp, tmp_path):
    path = G.generate_file(settings_tmp, DAY)
    assert path == Path(tmp_path) / "expenses" / "2026-01-01" / "vehicle_expenses.csv"
    with path.open() as fh:
        rows = list(csv.DictReader(fh))
    assert len(rows) == 20 and tuple(rows[0]) == EXPENSE_FIELDS
    assert validate_expense_file(path, "2026-01-01").is_valid
    # no temp files left behind (atomic write)
    assert [p.name for p in path.parent.iterdir()] == ["vehicle_expenses.csv"]
    # regenerating gives the identical file
    first = path.read_bytes()
    G.generate_file(settings_tmp, DAY)
    assert path.read_bytes() == first


def test_generate_file_does_not_overwrite_when_asked(settings_tmp):
    path = G.generate_file(settings_tmp, DAY)
    path.write_text("kept")
    assert G.generate_file(settings_tmp, DAY, overwrite=False) is None
    assert path.read_text() == "kept"


def test_corrupt_file_fails_validation(settings_tmp):
    path = G.generate_file(settings_tmp, date(2099, 1, 5), corrupt=True)
    result = validate_expense_file(path, "2099-01-05")
    assert not result.is_valid
    assert {
        "negative_value",
        "invalid_vehicle_id",
        "missing_value",
        "invalid_service_flag",
        "invalid_business_date",
        "invalid_number",
        "duplicate_vehicle_id",
    } <= set(result.rule_failures)


def test_cli_single_date(settings_tmp, tmp_path, monkeypatch):
    monkeypatch.setenv("LANDING_DIR", str(tmp_path))
    assert G.main(["--business-date", "2026-01-03"]) == 0
    assert (tmp_path / "expenses" / "2026-01-03" / "vehicle_expenses.csv").is_file()
