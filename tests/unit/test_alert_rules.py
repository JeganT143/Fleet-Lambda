"""Alert thresholds, severities and de-duplication keys (pure functions)."""

from __future__ import annotations

from datetime import UTC, date, datetime

from fleet.alerts import rules


def test_no_data_threshold_is_strictly_older_than():
    assert not rules.no_data_breached(59.9, 60)
    assert not rules.no_data_breached(60, 60)
    assert rules.no_data_breached(60.1, 60)
    assert rules.no_data_severity() == "critical"


def test_idle_threshold_is_at_least():
    assert not rules.idle_breached(119.9, 120)
    assert rules.idle_breached(120, 120)
    assert rules.idle_severity(120, 120) == "warning"
    assert rules.idle_severity(359.9, 120) == "warning"
    assert rules.idle_severity(360, 120) == "critical"


def test_low_profit_threshold_is_strictly_below():
    assert rules.low_profit_breached(-0.01, 0)
    assert not rules.low_profit_breached(0, 0)
    assert not rules.low_profit_breached(500, 0)
    assert rules.low_profit_breached(499.99, 500)


def test_low_profit_severity():
    assert rules.low_profit_severity(-1, 0) == "warning"
    assert rules.low_profit_severity(-1000, 0) == "warning"
    assert rules.low_profit_severity(-1000.01, 0) == "critical"


def test_dedup_keys_are_stable_per_occurrence():
    ts = datetime(2026, 1, 2, 3, 4, 5, 678000, tzinfo=UTC)
    assert rules.no_data_key(ts) == "no_stream_data:2026-01-02T03:04:05.678+00:00"
    assert rules.idle_key("V003", ts) == "vehicle_idle:V003:2026-01-02T03:04:05.678+00:00"
    assert rules.low_profit_key("V006", date(2026, 1, 2)) == "low_profitability:V006:2026-01-02"
    assert rules.low_profit_key("V006", "2026-01-02") == rules.low_profit_key(
        "V006", date(2026, 1, 2)
    )
