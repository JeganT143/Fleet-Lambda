"""Spark Structured Streaming job: Kafka -> validate/enrich/aggregate -> PostgreSQL.

Run by the `spark-streaming` compose service:
    spark-submit --master local[2] /app/fleet/streaming/job.py

Three independent queries read the same topic, each with its own checkpoint under
SPARK_CHECKPOINT_DIR (docs/architecture.md 5.4):

    ingest         every record -> stream_events / rejected_events / data_quality_stats
    fleet_metrics  1 h (simulated) tumbling windows, 15 min watermark -> realtime_vehicle_metrics
    zone_metrics   same windows per zone                              -> realtime_zone_metrics

The first run starts at the earliest Kafka offset; later runs resume from the
checkpoint. If any query fails (e.g. PostgreSQL is down) the job exits with an
error, Docker restarts it, and the checkpoint guarantees the failed micro-batch is
processed again.
"""

from __future__ import annotations

import sys
from functools import partial

from pyspark.sql import DataFrame, SparkSession

from fleet.common.config import Settings, load_settings
from fleet.common.logs import get_logger
from fleet.streaming import sinks
from fleet.streaming import transforms as T

log = get_logger("spark-streaming")


def build_spark(settings: Settings) -> SparkSession:
    spark = (
        SparkSession.builder.appName("fleet-streaming")
        # business_date = UTC date of the event time
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(settings.stream_shuffle_partitions))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def kafka_source(spark: SparkSession, settings: Settings) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.kafka_topic)
        # only used on the very first run; afterwards the checkpoint decides
        .option("startingOffsets", "earliest")
        # a deleted/recreated topic or expired retention must not kill the stream
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", settings.stream_max_offsets_per_trigger)
        .load()
    )


def start_queries(spark: SparkSession, settings: Settings) -> list:
    source = kafka_source(spark, settings)
    checkpoint = settings.spark_checkpoint_dir.rstrip("/")
    trigger = {"processingTime": settings.stream_trigger_interval}

    ingest = (
        source.writeStream.queryName("ingest")
        .foreachBatch(partial(sinks.write_ingest_batch, settings=settings))
        .option("checkpointLocation", f"{checkpoint}/ingest")
        .trigger(**trigger)
        .start()
    )

    # Metrics are computed from valid, enriched events only.
    valid, _ = T.split_valid_invalid(T.parse_kafka(source))
    events = T.enrich(valid)
    window, watermark = settings.stream_window_duration, settings.stream_watermark_delay

    fleet = (
        T.fleet_window_metrics(events, window, watermark)
        .writeStream.queryName("fleet_metrics")
        # update mode: each micro-batch emits only the windows that changed, with their
        # full current aggregate, so an upsert on window_start is always correct
        .outputMode("update")
        .foreachBatch(partial(sinks.write_fleet_metrics_batch, settings=settings))
        .option("checkpointLocation", f"{checkpoint}/fleet_metrics")
        .trigger(**trigger)
        .start()
    )
    zones = (
        T.zone_window_metrics(events, window, watermark)
        .writeStream.queryName("zone_metrics")
        .outputMode("update")
        .foreachBatch(partial(sinks.write_zone_metrics_batch, settings=settings))
        .option("checkpointLocation", f"{checkpoint}/zone_metrics")
        .trigger(**trigger)
        .start()
    )
    return [ingest, fleet, zones]


def main() -> int:
    settings = load_settings()
    spark = build_spark(settings)
    queries = start_queries(spark, settings)
    log.info(
        "streaming job started",
        extra={
            "fields": {
                "queries": [q.name for q in queries],
                "topic": settings.kafka_topic,
                "window": settings.stream_window_duration,
                "watermark": settings.stream_watermark_delay,
                "trigger": settings.stream_trigger_interval,
                "checkpoint_dir": settings.spark_checkpoint_dir,
            }
        },
    )
    try:
        # Blocks until any query stops; a failed query raises here.
        spark.streams.awaitAnyTermination()
    except Exception:
        log.exception("streaming query failed; exiting so the container restarts")
        for q in spark.streams.active:
            q.stop()
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
