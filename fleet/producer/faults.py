"""Deliberately invalid records, used ONLY to demonstrate the quarantine path.

Kept apart from the simulator on purpose: normal generation never produces invalid
data. The producer injects these as EXTRA records (it never replaces a real event,
so trip lifecycles and fares stay intact) at rate PRODUCER_INVALID_EVENT_RATE.

Each kind maps to the reason code the streaming validation must report
(fleet/streaming/validation.py).
"""

from __future__ import annotations

import json
import random
import uuid

MALFORMED_JSON = "malformed_json"
NEGATIVE_SPEED = "negative_speed"
UNKNOWN_STATUS = "unknown_status"
FAULT_KINDS: tuple[str, ...] = (MALFORMED_JSON, NEGATIVE_SPEED, UNKNOWN_STATUS)

# Reason code the validator is expected to produce for each fault kind
EXPECTED_REASON = {
    MALFORMED_JSON: "malformed_json",
    NEGATIVE_SPEED: "invalid_speed",
    UNKNOWN_STATUS: "invalid_status",
}


def make_invalid_record(rng: random.Random, template: dict) -> tuple[str, bytes]:
    """Build one invalid Kafka value from a valid event. Returns (fault_kind, value bytes).

    The record gets a fresh event_id so it can never collide with a real event.
    """
    kind = rng.choice(FAULT_KINDS)
    bad = dict(template, event_id=str(uuid.UUID(int=rng.getrandbits(128), version=4)))
    if kind == NEGATIVE_SPEED:
        bad["speed"] = -round(rng.uniform(1.0, 50.0), 1)
        return kind, json.dumps(bad).encode()
    if kind == UNKNOWN_STATUS:
        bad["status"] = "teleporting"
        return kind, json.dumps(bad).encode()
    # malformed JSON: a valid document cut off in the middle
    text = json.dumps(bad)
    return kind, text[: len(text) // 2].encode()
