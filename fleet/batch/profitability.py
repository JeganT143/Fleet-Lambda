"""Business rules of the daily report (docs/architecture.md 5.5), defined ONCE.

    total_operating_cost = fuel_cost + maintenance_cost
    estimated_profit     = earnings - fuel_cost - maintenance_cost
    utilization_rate     = on_trip_events / all_events          (0 when there are no events)
    profitability_status = profitable   if estimated_profit >= PROFITABLE_MIN_PROFIT
                           watch        if estimated_profit >= WATCH_MIN_PROFIT
                           unprofitable otherwise

The plain-Python functions are the reference implementation. The Spark reconciliation
builds its column expressions with `spark_status_column` / `spark_utilization_column`,
which read the same threshold arguments and use the same comparison operators, and
tests/unit/test_batch_profitability.py checks that both agree (including values exactly
on a threshold).

pyspark is imported lazily inside the Spark helpers, so this module stays importable
(and testable) without Spark.
"""

from __future__ import annotations

from decimal import Decimal

from fleet.common.contracts import PROFITABLE, UNPROFITABLE, WATCH

Number = float | int | Decimal


def estimated_profit(earnings: Number, fuel_cost: Number, maintenance_cost: Number) -> Number:
    return earnings - fuel_cost - maintenance_cost


def utilization_rate(on_trip_events: int, all_events: int) -> float:
    """Share of the vehicle-day spent on paid trips (events arrive at a fixed cadence)."""
    if all_events <= 0:
        return 0.0
    return on_trip_events / all_events


def classify_profit(profit: Number, profitable_min: float, watch_min: float) -> str:
    """Profitability status of one vehicle-day. Thresholds are inclusive lower bounds."""
    if profitable_min < watch_min:
        raise ValueError("PROFITABLE_MIN_PROFIT must be >= WATCH_MIN_PROFIT")
    if profit >= profitable_min:
        return PROFITABLE
    if profit >= watch_min:
        return WATCH
    return UNPROFITABLE


# ---------------------------------------------------------------------------
# Spark versions of the same rules
# ---------------------------------------------------------------------------
def spark_status_column(profit_col, profitable_min: float, watch_min: float):
    """Column expression equal to classify_profit(profit, profitable_min, watch_min)."""
    from pyspark.sql import functions as F

    if profitable_min < watch_min:
        raise ValueError("PROFITABLE_MIN_PROFIT must be >= WATCH_MIN_PROFIT")
    return (
        F.when(profit_col >= F.lit(profitable_min), F.lit(PROFITABLE))
        .when(profit_col >= F.lit(watch_min), F.lit(WATCH))
        .otherwise(F.lit(UNPROFITABLE))
    )


def spark_utilization_column(on_trip_col, events_col):
    """Column expression equal to utilization_rate(on_trip, events)."""
    from pyspark.sql import functions as F

    return F.when(events_col > 0, on_trip_col.cast("double") / events_col).otherwise(F.lit(0.0))
