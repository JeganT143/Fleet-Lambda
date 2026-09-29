"""Profitability rules: Python reference vs the Spark column expressions."""

from __future__ import annotations

from decimal import Decimal

import pytest

from fleet.batch import profitability as P

PROFITABLE_MIN, WATCH_MIN = 800.0, 0.0
EDGE_VALUES = ["-5000.00", "-0.01", "0.00", "0.01", "799.99", "800.00", "800.01", "12345.67"]


@pytest.mark.parametrize(
    ("profit", "status"),
    [
        (Decimal("-0.01"), "unprofitable"),
        (Decimal("0.00"), "watch"),  # exactly on WATCH_MIN -> watch
        (Decimal("799.99"), "watch"),
        (Decimal("800.00"), "profitable"),  # exactly on PROFITABLE_MIN -> profitable
        (1e6, "profitable"),
        (-1, "unprofitable"),
    ],
)
def test_classify_profit(profit, status):
    assert P.classify_profit(profit, PROFITABLE_MIN, WATCH_MIN) == status


def test_thresholds_must_be_ordered():
    with pytest.raises(ValueError):
        P.classify_profit(0, 0, 100)


def test_profit_and_utilization_formulas():
    assert P.estimated_profit(Decimal("450.00"), Decimal("100.00"), Decimal("50.00")) == Decimal(
        "300.00"
    )
    assert P.utilization_rate(4, 6) == pytest.approx(2 / 3)
    assert P.utilization_rate(0, 0) == 0.0


@pytest.mark.spark
@pytest.mark.parametrize(("profitable_min", "watch_min"), [(800.0, 0.0), (500.0, 500.0)])
def test_spark_status_agrees_with_python(spark, profitable_min, watch_min):
    pytest.importorskip("pyspark")
    from pyspark.sql import functions as F

    rows = [(Decimal(v),) for v in EDGE_VALUES] + [(Decimal("500.00"),), (Decimal("499.99"),)]
    df = spark.createDataFrame(rows, "profit decimal(12,2)")
    got = df.select(
        "profit",
        P.spark_status_column(F.col("profit"), profitable_min, watch_min).alias("s"),
    ).collect()
    assert len(got) == len(rows)
    for r in got:
        assert r.s == P.classify_profit(r.profit, profitable_min, watch_min), r.profit


@pytest.mark.spark
def test_spark_utilization_agrees_with_python(spark):
    from pyspark.sql import functions as F

    rows = [(0, 0), (0, 10), (4, 6), (300, 300)]
    df = spark.createDataFrame(rows, "on_trip int, events int")
    got = df.select(
        "on_trip",
        "events",
        P.spark_utilization_column(F.col("on_trip"), F.col("events")).alias("u"),
    ).collect()
    for r in got:
        assert r.u == pytest.approx(P.utilization_rate(r.on_trip, r.events))
