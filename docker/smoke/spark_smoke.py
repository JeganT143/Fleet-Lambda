"""Infrastructure smoke test: proves the baked-in Spark jars work.

(a) Kafka connector: batch-reads a throwaway topic (created and filled with the
    kafka CLI by `make smoke`) and checks the expected number of messages.
(b) JDBC driver: writes a tiny DataFrame into a throwaway table in the fleet DB,
    reads it back through JDBC, then drops the table.

Runs inside fleet-runtime (`make smoke`) and inside the airflow container
(`make smoke-airflow`, script piped over stdin). Settings come from env vars
via fleet.common.config. Exit code 0 = pass.
"""

from __future__ import annotations

import argparse
import sys
import uuid

import psycopg
from pyspark.sql import SparkSession

from fleet.common.config import load_settings


def check_kafka(spark: SparkSession, bootstrap: str, topic: str, expected: int) -> None:
    df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
        .selectExpr("CAST(value AS STRING) AS value", "partition", "offset")
    )
    rows = df.collect()
    for row in rows:
        print(f"  kafka {topic}[{row.partition}]@{row.offset}: {row.value}")
    if len(rows) != expected:
        raise AssertionError(f"expected {expected} Kafka messages, got {len(rows)}")
    print(f"KAFKA OK: read {len(rows)} messages from {topic} via spark-sql-kafka")


def check_jdbc(spark: SparkSession) -> None:
    s = load_settings()
    table = f"infra_smoke_{uuid.uuid4().hex[:8]}"
    props = {
        "user": s.postgres_user,
        "password": s.postgres_password,
        "driver": "org.postgresql.Driver",
    }
    df = spark.createDataFrame([(1, "alpha"), (2, "beta"), (3, "gamma")], "id INT, name STRING")
    try:
        df.write.jdbc(s.postgres_jdbc_url, table, mode="errorifexists", properties=props)
        back = spark.read.jdbc(s.postgres_jdbc_url, table, properties=props)
        got = sorted((r.id, r.name) for r in back.collect())
        if got != [(1, "alpha"), (2, "beta"), (3, "gamma")]:
            raise AssertionError(f"JDBC round trip mismatch: {got}")
        print(f"JDBC OK: wrote and read back {len(got)} rows in {s.postgres_db}.{table}")
    finally:
        with psycopg.connect(s.postgres_dsn, autocommit=True) as conn:
            conn.execute(f'DROP TABLE IF EXISTS "{table}"')
        print(f"  dropped {table}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", help="throwaway Kafka topic to read (omit to skip Kafka)")
    parser.add_argument("--expected", type=int, default=3, help="messages expected in --topic")
    parser.add_argument("--skip-jdbc", action="store_true")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.master("local[1]")
        .appName("infra-smoke")
        .config("spark.driver.memory", "512m")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print(
        f"spark {spark.version}, java {spark.sparkContext._jvm.System.getProperty('java.version')}"
    )
    try:
        if args.topic:
            check_kafka(spark, load_settings().kafka_bootstrap_servers, args.topic, args.expected)
        if not args.skip_jdbc:
            check_jdbc(spark)
    finally:
        spark.stop()
    print("SMOKE PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
