"""Pure-Python validation of a daily expense file (contract: contracts.EXPENSE_FIELDS).

The whole file is accepted or rejected: expenses are money, so a partially loaded day
would give a wrong profitability report. `validate_expense_file` never touches the
database; `fleet.batch.runs.validate_and_record` wraps it, records the statistics and
raises `ExpenseValidationError` for an invalid file.

Rules (the rule name is the key in data_quality_stats.rule_failures):

    header_mismatch         header row differs from EXPENSE_FIELDS (nothing else checked)
    wrong_column_count      a row has more / fewer values than the header
    missing_value           a required value is empty
    invalid_vehicle_id      vehicle_id does not match VEHICLE_ID_PATTERN
    invalid_number          a cost / distance is not a number
    negative_value          a cost / distance is < 0
    value_above_max         cost > MAX_DAILY_COST or distance > MAX_DAILY_DISTANCE_KM
    invalid_service_flag    service_flag is not "true" / "false"
    invalid_business_date   business_date is not a YYYY-MM-DD date
    business_date_mismatch  business_date differs from the directory date
    duplicate_vehicle_id    the same vehicle appears more than once
    empty_file              no data rows at all
"""

from __future__ import annotations

import csv
import math
import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

from fleet.common.contracts import (
    EXPENSE_FIELDS,
    MAX_DAILY_COST,
    MAX_DAILY_DISTANCE_KM,
    VEHICLE_ID_PATTERN,
)

COST_FIELDS = ("fuel_cost", "maintenance_cost")
DISTANCE_FIELD = "distance_covered"
SERVICE_FLAG_VALUES = ("true", "false")
MAX_EXAMPLES = 10  # how many example problems are kept for the error message

_VEHICLE_RE = re.compile(VEHICLE_ID_PATTERN)


@dataclass
class ValidationResult:
    file_path: str
    business_date: str
    records_total: int = 0
    records_rejected: int = 0
    rule_failures: Counter = field(default_factory=Counter)
    examples: list[str] = field(default_factory=list)

    @property
    def records_valid(self) -> int:
        return self.records_total - self.records_rejected

    @property
    def is_valid(self) -> bool:
        return not self.rule_failures

    def fail(self, rule: str, detail: str) -> None:
        self.rule_failures[rule] += 1
        if len(self.examples) < MAX_EXAMPLES:
            self.examples.append(f"{rule}: {detail}")

    def summary(self) -> str:
        rules = ", ".join(f"{r}={n}" for r, n in sorted(self.rule_failures.items()))
        return (
            f"expense file {self.file_path} is INVALID: {self.records_rejected} of "
            f"{self.records_total} rows rejected; rule failures: {rules}; "
            f"examples: {'; '.join(self.examples)}"
        )


class ExpenseValidationError(Exception):
    """Raised for an invalid expense file; carries the ValidationResult."""

    def __init__(self, result: ValidationResult) -> None:
        super().__init__(result.summary())
        self.result = result


def _check_row(row: dict[str, str], line: int, expected_date: str, result: ValidationResult) -> int:
    """Check one data row; returns how many rules it broke."""
    before = sum(result.rule_failures.values())
    values = {k: (v or "").strip() for k, v in row.items()}

    for name in EXPENSE_FIELDS:
        if values.get(name, "") == "":
            result.fail("missing_value", f"line {line}: {name} is empty")

    vid = values.get("vehicle_id", "")
    if vid and not _VEHICLE_RE.match(vid):
        result.fail("invalid_vehicle_id", f"line {line}: {vid!r}")

    for name in (*COST_FIELDS, DISTANCE_FIELD):
        raw = values.get(name, "")
        if not raw:
            continue  # already counted as missing_value
        try:
            number = float(raw)
        except ValueError:
            result.fail("invalid_number", f"line {line}: {name}={raw!r}")
            continue
        if not math.isfinite(number):
            result.fail("invalid_number", f"line {line}: {name}={raw!r}")
        elif number < 0:
            result.fail("negative_value", f"line {line}: {name}={raw}")
        elif number > (MAX_DAILY_DISTANCE_KM if name == DISTANCE_FIELD else MAX_DAILY_COST):
            result.fail("value_above_max", f"line {line}: {name}={raw}")

    flag = values.get("service_flag", "")
    if flag and flag.lower() not in SERVICE_FLAG_VALUES:
        result.fail("invalid_service_flag", f"line {line}: {flag!r}")

    raw_date = values.get("business_date", "")
    if raw_date:
        try:
            parsed = date.fromisoformat(raw_date)
        except ValueError:
            result.fail("invalid_business_date", f"line {line}: {raw_date!r}")
        else:
            if parsed.isoformat() != expected_date:
                result.fail("business_date_mismatch", f"line {line}: {raw_date} != {expected_date}")
    return sum(result.rule_failures.values()) - before


def validate_rows(lines: list[str], business_date: str, file_path: str = "") -> ValidationResult:
    """Validate CSV text lines (header first) for `business_date` (the directory date)."""
    result = ValidationResult(file_path=file_path, business_date=business_date)
    reader = csv.reader(lines)
    header = next(reader, None)
    if header is None or tuple(h.strip() for h in header) != EXPENSE_FIELDS:
        result.fail("header_mismatch", f"expected {list(EXPENSE_FIELDS)}, got {header}")
        return result

    seen: Counter = Counter()
    for line_no, values in enumerate(reader, start=2):
        if not values or all(not v.strip() for v in values):
            continue  # ignore blank lines
        result.records_total += 1
        broken = 0
        if len(values) != len(EXPENSE_FIELDS):
            result.fail("wrong_column_count", f"line {line_no}: {len(values)} values")
            broken = 1
        else:
            row = dict(zip(EXPENSE_FIELDS, values, strict=True))
            broken = _check_row(row, line_no, business_date, result)
            vid = row["vehicle_id"].strip()
            if vid:
                seen[vid] += 1
                if seen[vid] > 1:
                    result.fail("duplicate_vehicle_id", f"line {line_no}: {vid}")
                    broken += 1
        if broken:
            result.records_rejected += 1

    if result.records_total == 0:
        result.fail("empty_file", "no data rows")
    return result


def validate_expense_file(file_path: str | Path, business_date: str) -> ValidationResult:
    """Validate the expense file of one business date. Never raises for bad content."""
    path = Path(file_path)
    with path.open(newline="", encoding="utf-8") as fh:
        lines = fh.read().splitlines()
    return validate_rows(lines, business_date, str(path))
