"""Kafka round trip: the real producer config against the running broker.

Uses a throwaway `test.`-prefixed topic with 3 partitions and deletes it afterwards.
"""

from __future__ import annotations

import json
import time
import uuid
from collections import defaultdict
from datetime import date

import pytest

from fleet.common.contracts import EVENT_FIELDS
from fleet.common.simclock import SimClock
from fleet.streaming.validation import validate_event

pytestmark = pytest.mark.integration

ck = pytest.importorskip("confluent_kafka")
from confluent_kafka.admin import AdminClient  # noqa: E402

from fleet.producer.main import (  # noqa: E402
    DeliveryStats,
    ensure_topic,
    producer_config,
    send,
    wait_for_broker,
)
from fleet.producer.simulator import TelemetrySimulator  # noqa: E402

FLEET, TICKS, PARTITIONS = 12, 5, 3


@pytest.fixture
def topic(settings):
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    wait_for_broker(admin)
    name = f"test.telemetry.{uuid.uuid4().hex[:8]}"
    ensure_topic(admin, name, PARTITIONS)
    try:
        yield name
    finally:
        admin.delete_topics([name])[name].result(timeout=30)


def produce_events(settings, topic_name) -> tuple[list[dict], DeliveryStats]:
    real = [0.0]
    clock = SimClock(date(2099, 1, 1), 300.0, real_now=lambda: real[0])
    sim = TelemetrySimulator(FLEET, clock, 288.0, seed=5)
    producer = ck.Producer(producer_config(settings))
    stats = DeliveryStats()
    events = []
    for _ in range(TICKS):
        for event in sim.tick():
            send(producer, topic_name, event["vehicle_id"], json.dumps(event).encode(), stats)
            events.append(event)
        real[0] += 1.0
    assert producer.flush(30) == 0
    return events, stats


def consume_all(settings, topic_name, expected: int, timeout_s: float = 60.0) -> list:
    consumer = ck.Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": f"test-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic_name])
    messages = []
    deadline = time.monotonic() + timeout_s
    try:
        while len(messages) < expected and time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            assert msg.error() is None, msg.error()
            messages.append(msg)
    finally:
        consumer.close()
    return messages


def test_topic_is_created_with_three_partitions_idempotently(settings, topic):
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    ensure_topic(admin, topic, PARTITIONS)  # second call: no error, no change
    metadata = admin.list_topics(topic, timeout=10)
    assert len(metadata.topics[topic].partitions) == PARTITIONS


def test_producer_consumer_round_trip(settings, topic):
    events, stats = produce_events(settings, topic)
    assert stats.failed == 0
    assert stats.delivered == stats.sent == FLEET * TICKS

    messages = consume_all(settings, topic, expected=len(events))
    assert len(messages) == len(events)

    partitions_of = defaultdict(set)
    timestamps_of = defaultdict(list)
    for msg in sorted(messages, key=lambda m: (m.partition(), m.offset())):
        value = json.loads(msg.value())
        assert tuple(value) == EVENT_FIELDS  # the contract, in order
        assert validate_event(value) == []
        assert msg.key().decode() == value["vehicle_id"]  # key = vehicle_id
        partitions_of[value["vehicle_id"]].add(msg.partition())
        timestamps_of[value["vehicle_id"]].append(value["timestamp"])

    # same vehicle -> always the same partition, and its events stay in order
    assert all(len(p) == 1 for p in partitions_of.values())
    assert all(ts == sorted(ts) and len(ts) == TICKS for ts in timestamps_of.values())
    # keys are spread over more than one partition
    assert len({next(iter(p)) for p in partitions_of.values()}) > 1
    # nothing lost or changed on the way
    assert sorted(json.loads(m.value())["event_id"] for m in messages) == sorted(
        e["event_id"] for e in events
    )
