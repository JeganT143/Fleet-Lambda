"""Expense-file validation: one test per rule, plus the happy path."""

from __future__ import annotations

import pytest

from fleet.batch.validation import validate_expense_file, validate_rows

DAY = "2026-01-01"
HEADER = "vehicle_id,fuel_cost,maintenance_cost,distance_covered,service_flag,business_date"
GOOD = [
    "V001,1650.20,230.00,250.5,false,2026-01-01",
    "V002,1720.00,4100.10,140.0,true,2026-01-01",
]


def check(*rows: str, header: str = HEADER, day: str = DAY):
    return validate_rows([header, *rows], day, "test.csv")


def test_valid_file_passes():
    r = check(*GOOD)
    assert r.is_valid and r.records_total == 2 and r.records_rejected == 0


def test_whitespace_and_case_are_tolerated():
    r = check(" V001 , 1650.20 ,230, 250.5 , TRUE ,2026-01-01")
    assert r.is_valid


@pytest.mark.parametrize(
    ("row", "rule"),
    [
        ("V001,,230,250,false,2026-01-01", "missing_value"),
        ("X01,1650,230,250,false,2026-01-01", "invalid_vehicle_id"),
        ("V0001,1650,230,250,false,2026-01-01", "invalid_vehicle_id"),
        ("V001,abc,230,250,false,2026-01-01", "invalid_number"),
        ("V001,nan,230,250,false,2026-01-01", "invalid_number"),
        ("V001,1650,-1,250,false,2026-01-01", "negative_value"),
        ("V001,1650,230,-0.5,false,2026-01-01", "negative_value"),
        ("V001,300001,230,250,false,2026-01-01", "value_above_max"),
        ("V001,1650,230,2000.01,false,2026-01-01", "value_above_max"),
        ("V001,1650,230,250,yes,2026-01-01", "invalid_service_flag"),
        ("V001,1650,230,250,false,2026-02-30", "invalid_business_date"),
        ("V001,1650,230,250,false,01/01/2026", "invalid_business_date"),
        ("V001,1650,230,250,false,2026-01-02", "business_date_mismatch"),
        ("V001,1650,230,250,false", "wrong_column_count"),
    ],
)
def test_each_rule(row, rule):
    r = check(GOOD[1], row)
    assert not r.is_valid
    assert r.rule_failures == {rule: 1}
    assert r.records_total == 2 and r.records_rejected == 1
    assert rule in r.summary()


def test_duplicate_vehicle_id():
    r = check(GOOD[0], GOOD[1], GOOD[0])
    assert r.rule_failures == {"duplicate_vehicle_id": 1}
    assert r.records_rejected == 1


def test_header_mismatch_stops_validation():
    r = check(*GOOD, header="vehicle,fuel,maintenance,distance,service,date")
    assert r.rule_failures == {"header_mismatch": 1}


def test_empty_file():
    assert check().rule_failures == {"empty_file": 1}
    assert validate_rows([], DAY).rule_failures == {"header_mismatch": 1}


def test_counts_several_failures_in_one_row():
    r = check("BAD,-5,x,250,maybe,2026-01-01")
    assert r.rule_failures == {
        "invalid_vehicle_id": 1,
        "negative_value": 1,
        "invalid_number": 1,
        "invalid_service_flag": 1,
    }
    assert r.records_rejected == 1


def test_validate_file_from_disk(tmp_path):
    path = tmp_path / "vehicle_expenses.csv"
    path.write_text("\n".join([HEADER, *GOOD]) + "\n")
    assert validate_expense_file(path, DAY).is_valid
    assert not validate_expense_file(path, "2026-01-02").is_valid
