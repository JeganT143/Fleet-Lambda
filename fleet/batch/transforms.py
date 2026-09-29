"""Pure DataFrame transformations of the batch layer (no I/O, unit-tested on static data).

    raw CSV strings --clean_expenses--> typed vehicle_expenses rows + rejected rows
    stream events   --vehicle_day_aggregates--> events, on_trip events, trips, earnings,
                                                stream_distance_km per vehicle
    aggregates + expenses --reconcile--> daily_vehicle_profitability rows
                                         + vehicles that have events but no expense row

Money stays DECIMAL end to end (no float rounding in profit); only utilization_rate and
the distance estimate are doubles.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from fleet.batch.profitability import spark_status_column, spark_utilization_column
from fleet.common.contracts import (
    EXPENSE_FIELDS,
    MAX_DAILY_COST,
    MAX_DAILY_DISTANCE_KM,
    STATUS_ON_TRIP,
    VEHICLE_ID_PATTERN,
)

# Every column is read as a STRING on purpose: with an explicit all-string schema no
# value is silently turned into NULL by the CSV parser; the casts below decide, and a
# failed cast is counted as a rejected row instead of disappearing.
EXPENSE_CSV_SCHEMA = StructType([StructField(name, StringType(), True) for name in EXPENSE_FIELDS])

MONEY = "decimal(10,2)"
BIG_MONEY = "decimal(12,2)"

# Column order of the tables (without run_id / timestamps, which PostgreSQL fills)
VEHICLE_EXPENSE_COLUMNS: tuple[str, ...] = (
    "vehicle_id",
    "business_date",
    "fuel_cost",
    "maintenance_cost",
    "distance_covered",
    "service_flag",
    "source_file",
)
PROFITABILITY_COLUMNS: tuple[str, ...] = (
    "vehicle_id",
    "business_date",
    "trips",
    "event_count",
    "on_trip_event_count",
    "stream_distance_km",
    "distance_km",
    "earnings",
    "fuel_cost",
    "maintenance_cost",
    "total_operating_cost",
    "estimated_profit",
    "utilization_rate",
    "service_flag",
    "profitability_status",
)


# ---------------------------------------------------------------------------
# (a) expenses: clean / cast / trim
# ---------------------------------------------------------------------------
def clean_expenses(
    raw: DataFrame, business_date: date, source_file: str
) -> tuple[DataFrame, DataFrame]:
    """Trim + cast the all-string CSV rows. Returns (clean rows, rejected rows).

    A row is rejected when any value is missing or does not cast, a number is out of
    range, or its business_date is not the date being loaded. (The Airflow validation
    task rejects such files before Spark runs; this is the second line of defence.)
    """
    t = {c: F.trim(F.col(c)) for c in EXPENSE_FIELDS}
    flag = F.lower(t["service_flag"])
    typed = raw.select(
        F.upper(t["vehicle_id"]).alias("vehicle_id"),
        F.to_date(t["business_date"], "yyyy-MM-dd").alias("business_date"),
        t["fuel_cost"].cast(MONEY).alias("fuel_cost"),
        t["maintenance_cost"].cast(MONEY).alias("maintenance_cost"),
        t["distance_covered"].cast(MONEY).alias("distance_covered"),
        F.when(flag == "true", True).when(flag == "false", False).alias("service_flag"),
        F.lit(source_file).alias("source_file"),
    )
    cost_ok = lambda c: F.col(c).between(0, MAX_DAILY_COST)  # noqa: E731
    ok = (
        F.col("vehicle_id").rlike(VEHICLE_ID_PATTERN)
        & (F.col("business_date") == F.lit(business_date))
        & cost_ok("fuel_cost")
        & cost_ok("maintenance_cost")
        & F.col("distance_covered").between(0, MAX_DAILY_DISTANCE_KM)
        & F.col("service_flag").isNotNull()
    )
    # a NULL anywhere makes `ok` NULL -> coalesce to False (= rejected)
    checked = typed.withColumn("_ok", F.coalesce(ok, F.lit(False)))
    clean = checked.filter("_ok").drop("_ok").select(*VEHICLE_EXPENSE_COLUMNS)
    rejected = checked.filter(~F.col("_ok")).drop("_ok")
    return clean, rejected


# ---------------------------------------------------------------------------
# (b) stream history: one row per vehicle-day
# ---------------------------------------------------------------------------
def vehicle_day_aggregates(events: DataFrame, max_gap_minutes: float) -> DataFrame:
    """Aggregate one business date of stream_events per vehicle.

    Input columns: vehicle_id, status, speed (km/h), fare, event_timestamp.

        event_count          all events of the vehicle
        on_trip_event_count  events with status on_trip
        trips                events with fare > 0 (fare is reported once per trip)
        earnings             SUM(fare)
        stream_distance_km   SUM(speed x gap to the vehicle's previous event)

    Distance: the producer reports, on each event, the speed driven since the previous
    event, so speed_i x (t_i - t_(i-1)) is the km of that interval. A LAG window per
    vehicle (ordered by event time) gives t_(i-1). The first event of the day has no
    previous event (contributes 0), and gaps longer than `max_gap_minutes` (simulated;
    lost events or a producer outage) are ignored instead of being counted as hours of
    driving at the last speed.
    """
    by_vehicle = Window.partitionBy("vehicle_id").orderBy("event_timestamp")
    gap_hours = (
        F.col("event_timestamp").cast("double")
        - F.lag("event_timestamp").over(by_vehicle).cast("double")
    ) / 3600.0
    with_gap = events.withColumn("_gap_h", gap_hours)
    usable_gap = F.col("_gap_h").isNotNull() & (F.col("_gap_h") <= max_gap_minutes / 60.0)
    fare = F.col("fare").cast(BIG_MONEY)
    return with_gap.groupBy("vehicle_id").agg(
        F.count(F.lit(1)).cast("int").alias("event_count"),
        F.sum(F.when(F.col("status") == STATUS_ON_TRIP, 1).otherwise(0))
        .cast("int")
        .alias("on_trip_event_count"),
        F.sum(F.when(fare > 0, 1).otherwise(0)).cast("int").alias("trips"),
        F.coalesce(F.sum(fare), F.lit(0)).cast(BIG_MONEY).alias("earnings"),
        F.coalesce(
            F.sum(F.when(usable_gap, F.col("speed").cast("double") * F.col("_gap_h"))),
            F.lit(0.0),
        ).alias("stream_distance_km"),
    )


# ---------------------------------------------------------------------------
# (c) reconciliation
# ---------------------------------------------------------------------------
def reconcile(
    aggregates: DataFrame,
    expenses: DataFrame,
    business_date: date,
    profitable_min: float,
    watch_min: float,
) -> tuple[DataFrame, DataFrame]:
    """Join per-vehicle stream aggregates with the day's expenses.

    Returns (profitability rows in PROFITABILITY_COLUMNS order, missing-expense vehicles).

    Handling of mismatches (a LEFT join from expenses):
    - expense row but no events  -> reported with 0 trips / earnings / utilisation;
      its costs are real, so it shows up as a loss (a car that cost money but never drove).
    - events but no expense row  -> NOT reported: without costs its profit is unknown,
      and inventing 0 costs would show a fake profit. These vehicles are returned
      separately so the job can log a warning and count them in data_quality_stats.
    """
    exp = expenses.select(
        "vehicle_id", "fuel_cost", "maintenance_cost", "distance_covered", "service_flag"
    )
    joined = exp.join(aggregates, on="vehicle_id", how="left")

    zero = F.lit(0)
    events = F.coalesce(F.col("event_count"), zero)
    on_trip = F.coalesce(F.col("on_trip_event_count"), zero)
    earnings = F.coalesce(F.col("earnings"), F.lit(0).cast(BIG_MONEY)).cast(BIG_MONEY)
    total_cost = (F.col("fuel_cost") + F.col("maintenance_cost")).cast(BIG_MONEY)
    # same formula as profitability.estimated_profit: earnings - fuel - maintenance
    profit = (earnings - F.col("fuel_cost") - F.col("maintenance_cost")).cast(BIG_MONEY)

    result = joined.select(
        F.col("vehicle_id"),
        F.lit(business_date).alias("business_date"),
        F.coalesce(F.col("trips"), zero).cast("int").alias("trips"),
        events.cast("int").alias("event_count"),
        on_trip.cast("int").alias("on_trip_event_count"),
        F.round(F.coalesce(F.col("stream_distance_km"), F.lit(0.0)), 2)
        .cast(MONEY)
        .alias("stream_distance_km"),
        F.col("distance_covered").cast(MONEY).alias("distance_km"),
        earnings.alias("earnings"),
        F.col("fuel_cost").cast(MONEY).alias("fuel_cost"),
        F.col("maintenance_cost").cast(MONEY).alias("maintenance_cost"),
        total_cost.alias("total_operating_cost"),
        profit.alias("estimated_profit"),
        spark_utilization_column(on_trip, events).alias("utilization_rate"),
        F.col("service_flag"),
        spark_status_column(profit, profitable_min, watch_min).alias("profitability_status"),
    ).select(*PROFITABILITY_COLUMNS)

    missing = aggregates.join(exp.select("vehicle_id"), on="vehicle_id", how="left_anti").select(
        "vehicle_id", "event_count", "trips", "earnings"
    )
    return result, missing
