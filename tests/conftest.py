"""Shared pytest fixtures.

- `settings`: configuration from environment (.env inside the runner container)
- `spark`:    one local SparkSession per test session (Spark tests run inside the
              fleet-runtime container: `make test-unit`)
- `pg_conn`:  PostgreSQL connection for integration tests. Integration tests FAIL,
              not skip, when the database is unreachable; start it with `make up`.

Test data conventions (keep integration tests from touching live pipeline data):
- vehicle ids V900-V999 are reserved for tests
- business dates in the year 2099 are reserved for tests
- Kafka topics used by tests start with "test."
Every integration test must delete the rows / topics it creates.
"""

from __future__ import annotations

import pytest

from fleet.common.config import Settings, load_settings

TEST_VEHICLE_PREFIX = "V9"
TEST_YEAR = 2099


@pytest.fixture(scope="session")
def settings() -> Settings:
    return load_settings()


@pytest.fixture(scope="session")
def spark():
    pyspark_sql = pytest.importorskip("pyspark.sql")
    session = (
        pyspark_sql.SparkSession.builder.master("local[1]")
        .appName("fleet-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("WARN")
    yield session
    session.stop()


@pytest.fixture
def pg_conn(settings):
    from fleet.common.db import connect

    with connect(settings) as conn:
        yield conn
