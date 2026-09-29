"""The Spark zone expression must agree with the reference zones.zone_for everywhere."""

from __future__ import annotations

import random

import pytest

from fleet.common.zones import (
    ALL_ZONES,
    CITY_LAT_MAX,
    CITY_LAT_MIN,
    CITY_LON_MAX,
    CITY_LON_MIN,
    GRID_SIZE,
    OUTSIDE_ZONE,
    zone_for,
)

pytestmark = pytest.mark.spark


def _cell_edges(low: float, high: float) -> list[float]:
    step = (high - low) / GRID_SIZE
    return [low + i * step for i in range(GRID_SIZE + 1)]


def _points() -> list[tuple[float, float]]:
    lat_edges = _cell_edges(CITY_LAT_MIN, CITY_LAT_MAX)
    lon_edges = _cell_edges(CITY_LON_MIN, CITY_LON_MAX)
    eps = 1e-9
    lats = [x + d for x in lat_edges for d in (-eps, 0.0, eps)]
    lons = [x + d for x in lon_edges for d in (-eps, 0.0, eps)]
    # every combination of edge / just-inside / just-outside values
    points = [(lat, lon) for lat in lats for lon in lons]
    # the literal decimal edges (as a producer would send them) and far-away points
    points += [(6.84, 79.86), (6.98, 79.97), (6.9, 79.9), (6.93, 79.933), (0.0, 0.0)]
    points += [(-90.0, -180.0), (90.0, 180.0), (6.93, 79.0), (8.0, 79.9)]
    rng = random.Random(7)
    points += [
        (rng.uniform(CITY_LAT_MIN - 0.05, CITY_LAT_MAX + 0.05),
         rng.uniform(CITY_LON_MIN - 0.05, CITY_LON_MAX + 0.05))
        for _ in range(2000)
    ]  # fmt: skip
    return points


def test_spark_zone_expression_matches_zone_for(spark):
    from pyspark.sql import functions as F

    from fleet.streaming.transforms import zone_column

    points = _points()
    df = spark.createDataFrame(
        [(i, lat, lon) for i, (lat, lon) in enumerate(points)], "i INT, lat DOUBLE, lon DOUBLE"
    )
    rows = df.select("i", zone_column(F.col("lat"), F.col("lon")).alias("zone")).collect()
    got = {r.i: r.zone for r in rows}
    mismatches = [(p, got[i], zone_for(*p)) for i, p in enumerate(points) if got[i] != zone_for(*p)]
    assert mismatches == []
    # sanity: the fixture really covers all nine cells and the outside zone
    assert set(got.values()) == set(ALL_ZONES)


def test_known_zones():
    # reference values the Spark test relies on
    assert zone_for(CITY_LAT_MIN, CITY_LON_MIN) == "Dehiwala"  # south-west corner
    assert zone_for(CITY_LAT_MAX, CITY_LON_MAX) == "Mulleriyawa"  # upper edge -> last cell
    # real neighbourhood coordinates land in their own labelled cell
    assert zone_for(6.8990, 79.9180) == "Battaramulla"
    assert zone_for(6.9140, 79.8770) == "Borella"
    assert zone_for(6.9553, 79.9219) == "Kelaniya"
    assert zone_for(6.9060, 79.9580) == "Malabe"
    assert zone_for(6.8480, 79.9270) == "Maharagama"
    assert zone_for(6.8510, 79.8660) == "Dehiwala"
    # Colombo Fort (on the coast, west of the grid) is outside
    assert zone_for(6.9344, 79.8428) == OUTSIDE_ZONE
    assert zone_for(CITY_LAT_MIN - 0.001, 79.9) == OUTSIDE_ZONE
