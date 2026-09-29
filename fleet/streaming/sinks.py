"""foreachBatch writers: Spark micro-batches -> PostgreSQL (psycopg 3).

Idempotency (a micro-batch may be re-run after a crash, because Spark commits the
checkpoint only after foreachBatch returns):

    stream_events             INSERT ... ON CONFLICT (event_id) DO NOTHING
    rejected_events           INSERT ... ON CONFLICT (kafka_partition, kafka_offset) DO NOTHING
    realtime_*_metrics        upsert on the primary key (latest aggregate wins)
    data_quality_stats        delete + insert for the same batch_ref, in one transaction

Errors are NOT caught: a failed write fails the micro-batch, the query and the job.
Docker restarts the container and Spark resumes from the last committed checkpoint,
so no data is lost and nothing is written twice.
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterable
from functools import partial

import psycopg
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from fleet.common.config import Settings, load_settings
from fleet.common.logs import get_logger
from fleet.streaming import transforms as T

log = get_logger("spark-streaming")

PIPELINE_NAME = "stream_ingest"
INSERT_CHUNK_ROWS = 500

INSERT_STREAM_EVENT_SQL = f"""
INSERT INTO stream_events ({", ".join(T.STREAM_EVENT_COLUMNS)})
VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s,
        %s::timestamptz, %s::date, %s, %s, %s, %s::timestamptz, %s::timestamptz)
ON CONFLICT (event_id) DO NOTHING
"""

INSERT_REJECTED_SQL = f"""
INSERT INTO rejected_events ({", ".join(T.REJECTED_EVENT_COLUMNS)})
VALUES (%s, %s, %s, %s, %s::timestamptz, %s::timestamptz)
ON CONFLICT (kafka_partition, kafka_offset) DO NOTHING
"""


def _upsert_sql(table: str, columns: tuple[str, ...], key: tuple[str, ...]) -> str:
    casts = {
        "window_start": "::timestamptz",
        "window_end": "::timestamptz",
        "business_date": "::date",
    }
    values = ", ".join(f"%s{casts.get(c, '')}" for c in columns)
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns if c not in key)
    return (
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({values}) "
        f"ON CONFLICT ({', '.join(key)}) DO UPDATE SET {updates}, updated_at = now()"
    )


UPSERT_FLEET_METRICS_SQL = _upsert_sql(
    "realtime_vehicle_metrics", T.FLEET_METRIC_COLUMNS, ("window_start",)
)
UPSERT_ZONE_METRICS_SQL = _upsert_sql(
    "realtime_zone_metrics", T.ZONE_METRIC_COLUMNS, ("window_start", "zone")
)


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------
def _insert_rows(dsn: str, sql: str, columns: tuple[str, ...], rows: Iterable) -> None:
    """Insert rows in chunks inside one transaction (commit on success, rollback on error)."""
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        chunk = []
        for row in rows:
            chunk.append(tuple(row[c] for c in columns))
            if len(chunk) >= INSERT_CHUNK_ROWS:
                cur.executemany(sql, chunk)
                chunk = []
        if chunk:
            cur.executemany(sql, chunk)


def _write_partitions(df: DataFrame, dsn: str, sql: str, columns: tuple[str, ...]) -> None:
    """Each Spark partition opens its own connection on the executor (scales with partitions)."""
    df.foreachPartition(partial(_insert_rows, dsn, sql, columns))


def _batch_summary(checked: DataFrame) -> list:
    """One small Spark job: per Kafka partition -> rows, rejected rows, offset range and
    the latest business date of the valid events."""
    is_invalid = F.size("reasons") > 0
    event_date = F.to_date(F.col("parsed.timestamp").cast("timestamp"))
    return (
        checked.groupBy("kafka_partition")
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum(is_invalid.cast("int")).alias("rejected"),
            F.min("kafka_offset").alias("lo"),
            F.max("kafka_offset").alias("hi"),
            F.max(F.when(~is_invalid, event_date)).alias("business_date"),
        )
        .orderBy("kafka_partition")
        .collect()
    )


def _offset_ref(summary: list, query_name: str) -> str:
    """Identify a micro-batch by its Kafka offset ranges, e.g. 'ingest:p0=100-199,p1=...'.

    A re-run of the same micro-batch covers the same offsets and gets the same ref, so
    its data_quality_stats row is replaced instead of duplicated.
    """
    return f"{query_name}:" + ",".join(f"p{r.kafka_partition}={r.lo}-{r.hi}" for r in summary)


# ---------------------------------------------------------------------------
# Query 1: ingest (raw events + quarantine + data-quality stats)
# ---------------------------------------------------------------------------
def write_ingest_batch(
    batch_df: DataFrame,
    batch_id: int,
    settings: Settings | None = None,
    query_name: str = "ingest",
) -> dict:
    """foreachBatch function for the ingest query. Returns the counts it logged."""
    settings = settings or load_settings()
    started = time.monotonic()
    checked = T.parse_kafka(batch_df).persist()
    try:
        summary = _batch_summary(checked)
        total = sum(r.rows for r in summary)
        if total == 0:
            return {"rows": 0}
        rejected = sum(r.rejected for r in summary)
        dates = [r.business_date for r in summary if r.business_date is not None]
        business_date = max(dates, default=None)
        valid, invalid = T.split_valid_invalid(checked)

        # Raw events: written by the executors, partition by partition.
        _write_partitions(
            T.stream_event_rows(T.enrich(valid)),
            settings.postgres_dsn,
            INSERT_STREAM_EVENT_SQL,
            T.STREAM_EVENT_COLUMNS,
        )
        if rejected:
            _write_partitions(
                T.rejected_event_rows(invalid),
                settings.postgres_dsn,
                INSERT_REJECTED_SQL,
                T.REJECTED_EVENT_COLUMNS,
            )

        # Data-quality stats: tiny aggregates, computed and written on the driver.
        rule_failures = {}
        if rejected:
            rule_failures = {
                r.reason: r["count"]
                for r in invalid.select(F.explode("reasons").alias("reason"))
                .groupBy("reason")
                .count()
                .collect()
            }
        batch_ref = _offset_ref(summary, query_name)
        with psycopg.connect(settings.postgres_dsn) as conn:
            conn.execute(
                "DELETE FROM data_quality_stats WHERE pipeline_name = %s AND batch_ref = %s",
                (PIPELINE_NAME, batch_ref),
            )
            conn.execute(
                "INSERT INTO data_quality_stats (pipeline_name, batch_ref, business_date,"
                " records_total, records_valid, records_rejected, rule_failures)"
                " VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)",
                (
                    PIPELINE_NAME,
                    batch_ref,
                    business_date,
                    total,
                    total - rejected,
                    rejected,
                    json.dumps(rule_failures),
                ),
            )

        stats = {
            "query": query_name,
            "batch_id": batch_id,
            "rows": total,
            "valid": total - rejected,
            "rejected": rejected,
            "rule_failures": rule_failures,
            "batch_ref": batch_ref,
            "duration_ms": round((time.monotonic() - started) * 1000),
        }
        log.info("micro-batch written", extra={"fields": stats})
        return stats
    finally:
        checked.unpersist()


# ---------------------------------------------------------------------------
# Queries 2 and 3: windowed metrics (a handful of rows per batch -> collect is fine)
# ---------------------------------------------------------------------------
def _write_metrics(
    batch_df: DataFrame,
    batch_id: int,
    settings: Settings | None,
    query_name: str,
    sql: str,
    columns: tuple[str, ...],
) -> dict:
    settings = settings or load_settings()
    started = time.monotonic()
    rows = T.metric_rows(batch_df, columns).collect()
    if rows:
        _insert_rows(settings.postgres_dsn, sql, columns, rows)
    stats = {
        "query": query_name,
        "batch_id": batch_id,
        "rows": len(rows),
        "rejected": 0,
        "latest_window": max((r["window_start"] for r in rows), default=None),
        "duration_ms": round((time.monotonic() - started) * 1000),
    }
    log.info("micro-batch written", extra={"fields": stats})
    return stats


def write_fleet_metrics_batch(
    batch_df: DataFrame, batch_id: int, settings: Settings | None = None
) -> dict:
    return _write_metrics(
        batch_df,
        batch_id,
        settings,
        "fleet_metrics",
        UPSERT_FLEET_METRICS_SQL,
        T.FLEET_METRIC_COLUMNS,
    )


def write_zone_metrics_batch(
    batch_df: DataFrame, batch_id: int, settings: Settings | None = None
) -> dict:
    return _write_metrics(
        batch_df,
        batch_id,
        settings,
        "zone_metrics",
        UPSERT_ZONE_METRICS_SQL,
        T.ZONE_METRIC_COLUMNS,
    )
