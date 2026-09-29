"""Kafka telemetry producer: `python -m fleet.producer.main`.

Every STREAM_INTERVAL_SECONDS (real) the simulator emits one event per vehicle,
which is sent to KAFKA_TOPIC with key = vehicle_id (so every event of a vehicle
lands in the same partition, in order) and a JSON value.

Reliability:
- the topic is created with KAFKA_TOPIC_PARTITIONS partitions if missing
  (the broker has auto-create disabled)
- acks=all + enable.idempotence: no loss and no duplicates on producer retries
- a delivery callback counts successes/failures and logs every failure
- the broker connection is retried at startup; SIGTERM/SIGINT flush before exit

Simulated time: a fresh topic starts at SIM_START_DATE 00:00. After a restart the
producer continues just after the newest event already in the topic, so simulated
days never repeat (the batch layer would otherwise count a day twice).
"""

from __future__ import annotations

import json
import random
import signal
import time
from datetime import UTC, datetime, timedelta
from datetime import time as dtime

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from fleet.common.config import Settings, load_settings
from fleet.common.logs import get_logger
from fleet.common.simclock import SimClock, business_date_of, to_iso
from fleet.producer.faults import make_invalid_record
from fleet.producer.simulator import TelemetrySimulator

log = get_logger("producer")

STARTUP_ATTEMPTS = 30
STARTUP_BACKOFF_SECONDS = 2.0


# ---------------------------------------------------------------------------
# Kafka setup
# ---------------------------------------------------------------------------
def producer_config(settings: Settings) -> dict:
    return {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "client.id": "fleet-telemetry-producer",
        "acks": "all",  # wait for all in-sync replicas
        "enable.idempotence": True,  # retries never create duplicates / reorder
        "retries": 10,
        "retry.backoff.ms": 500,
        "delivery.timeout.ms": 120_000,
        "linger.ms": 50,  # small batching window: one tick's events go out together
    }


def wait_for_broker(admin: AdminClient) -> None:
    """Retry until the broker answers a metadata request."""
    for attempt in range(1, STARTUP_ATTEMPTS + 1):
        try:
            admin.list_topics(timeout=5)
            return
        except KafkaException as exc:
            log.warning(
                "kafka not reachable yet",
                extra={"fields": {"attempt": attempt, "error": str(exc)}},
            )
            time.sleep(STARTUP_BACKOFF_SECONDS)
    raise RuntimeError(f"kafka unreachable after {STARTUP_ATTEMPTS} attempts")


def ensure_topic(admin: AdminClient, topic: str, partitions: int) -> None:
    """Create the topic if it does not exist (replication 1: single-broker setup)."""
    existing = admin.list_topics(timeout=10).topics
    if topic in existing:
        count = len(existing[topic].partitions)
        if count != partitions:
            log.warning(
                "topic exists with a different partition count",
                extra={"fields": {"topic": topic, "partitions": count, "wanted": partitions}},
            )
        return
    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=partitions, replication_factor=1)]
    )
    try:
        futures[topic].result(timeout=30)
        log.info("topic created", extra={"fields": {"topic": topic, "partitions": partitions}})
    except KafkaException as exc:
        if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:  # lost a creation race
            raise


def latest_event_time(settings: Settings) -> datetime | None:
    """Newest event timestamp in the topic (reads the last record of each partition)."""
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": "fleet-producer-resume",  # offsets are never committed
            "enable.auto.commit": False,
        }
    )
    try:
        metadata = consumer.list_topics(settings.kafka_topic, timeout=10)
        partitions = metadata.topics[settings.kafka_topic].partitions
        # Look at the last few records of each partition: the very last one may be an
        # injected malformed record without a readable timestamp.
        assignment = []
        for p in partitions:
            low, high = consumer.get_watermark_offsets(
                TopicPartition(settings.kafka_topic, p), timeout=10
            )
            if high > low:
                assignment.append(TopicPartition(settings.kafka_topic, p, max(low, high - 5)))
        if not assignment:
            return None
        consumer.assign(assignment)
        newest = None
        for msg in consumer.consume(num_messages=5 * len(assignment), timeout=10):
            if msg.error():
                continue
            try:
                ts = datetime.fromisoformat(json.loads(msg.value())["timestamp"])
            except (ValueError, KeyError, TypeError):
                continue
            newest = ts if newest is None or ts > newest else newest
        return newest
    finally:
        consumer.close()


class OffsetClock:
    """A SimClock shifted forward by a fixed simulated offset (used to resume)."""

    def __init__(self, base: SimClock, offset: timedelta) -> None:
        self.base = base
        self.offset = offset

    def now(self) -> datetime:
        return self.base.now() + self.offset


def build_clock(settings: Settings, resume_after: datetime | None, tick_sim: float):
    clock = SimClock(settings.sim_start_date, settings.sim_day_real_seconds)
    if resume_after is None:
        return clock
    start = datetime.combine(settings.sim_start_date, dtime.min, tzinfo=UTC)
    offset = resume_after + timedelta(seconds=tick_sim) - start
    return OffsetClock(clock, max(offset, timedelta(0)))


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
class DeliveryStats:
    def __init__(self) -> None:
        self.sent = 0
        self.delivered = 0
        self.failed = 0
        self.invalid_injected = 0
        self.partitions: dict[int, int] = {}

    def on_delivery(self, err, msg) -> None:
        """Called by librdkafka (from poll/flush) once per message."""
        if err is not None:
            self.failed += 1
            log.error(
                "delivery failed",
                extra={"fields": {"key": (msg.key() or b"").decode(), "error": str(err)}},
            )
        else:
            self.delivered += 1
            self.partitions[msg.partition()] = self.partitions.get(msg.partition(), 0) + 1


def send(producer: Producer, topic: str, key: str, value: bytes, stats: DeliveryStats) -> None:
    while True:
        try:
            producer.produce(topic, key=key.encode(), value=value, on_delivery=stats.on_delivery)
            stats.sent += 1
            return
        except BufferError:  # local queue full: serve callbacks, then retry
            producer.poll(1.0)


def run() -> int:
    settings = load_settings()
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    wait_for_broker(admin)
    ensure_topic(admin, settings.kafka_topic, settings.kafka_topic_partitions)

    tick_sim = settings.stream_interval_seconds * settings.sim_speedup
    resume_after = latest_event_time(settings)
    clock = build_clock(settings, resume_after, tick_sim)
    # Behaviour is reproducible (SIM_RANDOM_SEED); ids are fresh per run (id_seed=None
    # would reuse the same ids after a restart and they would be dropped as duplicates).
    simulator = TelemetrySimulator(
        settings.fleet_size,
        clock,
        tick_sim,
        seed=settings.sim_random_seed,
        id_seed=random.SystemRandom().getrandbits(64),
    )
    fault_rng = random.Random()  # separate RNG: injection never changes normal events
    producer = Producer(producer_config(settings))
    stats = DeliveryStats()

    stop = {"requested": False}

    def request_stop(signum, _frame) -> None:
        stop["requested"] = True
        log.info("stop requested", extra={"fields": {"signal": signal.Signals(signum).name}})

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    log.info(
        "producer started",
        extra={
            "fields": {
                "topic": settings.kafka_topic,
                "fleet_size": settings.fleet_size,
                "interval_s": settings.stream_interval_seconds,
                "tick_sim_s": tick_sim,
                "sim_start": to_iso(clock.now()),
                "resumed_after": to_iso(resume_after) if resume_after else None,
                "invalid_event_rate": settings.producer_invalid_event_rate,
            }
        },
    )

    next_tick = time.monotonic()
    last_stats = time.monotonic()
    ticks = 0
    sim_now = clock.now()
    while not stop["requested"]:
        for event in simulator.tick():
            send(
                producer,
                settings.kafka_topic,
                event["vehicle_id"],
                json.dumps(event).encode(),
                stats,
            )
            sim_now = event["timestamp"]
            if fault_rng.random() < settings.producer_invalid_event_rate:
                _, bad_value = make_invalid_record(fault_rng, event)
                send(producer, settings.kafka_topic, event["vehicle_id"], bad_value, stats)
                stats.invalid_injected += 1
        ticks += 1
        producer.poll(0)  # serve delivery callbacks

        if time.monotonic() - last_stats >= settings.producer_stats_interval_seconds:
            last_stats = time.monotonic()
            log.info(
                "producer stats",
                extra={
                    "fields": {
                        "ticks": ticks,
                        "sent": stats.sent,
                        "delivered": stats.delivered,
                        "failed": stats.failed,
                        "invalid_injected": stats.invalid_injected,
                        "in_flight": len(producer),
                        "by_partition": stats.partitions,
                        "sim_time": sim_now,
                        "business_date": str(business_date_of(clock.now())),
                    }
                },
            )

        # Fixed-rate schedule: sleep until the next tick (skip ahead if we fell behind).
        next_tick += settings.stream_interval_seconds
        delay = next_tick - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        else:
            next_tick = time.monotonic()

    remaining = producer.flush(30)
    log.info(
        "producer stopped",
        extra={
            "fields": {
                "sent": stats.sent,
                "delivered": stats.delivered,
                "failed": stats.failed,
                "not_flushed": remaining,
            }
        },
    )
    return 0 if remaining == 0 else 1


if __name__ == "__main__":
    raise SystemExit(run())
