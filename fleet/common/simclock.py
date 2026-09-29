"""Simulated time (AGENTS.md 8.3): 1 simulated business day = SIM_DAY_REAL_SECONDS real seconds.

Four timestamps are kept distinct across the project:

    event timestamp      simulated time the event happened (set by the producer)
    business date        UTC calendar date of the event timestamp
    ingestion timestamp  real time on the Kafka record (CreateTime: set when the producer sends it)
    processing timestamp real time Spark processed the record

Only the producer owns a SimClock. Downstream components derive simulated time
from the event timestamps in the data, never from their own wall clock.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from datetime import time as dtime


class SimClock:
    def __init__(
        self,
        sim_start_date: date,
        sim_day_real_seconds: float = 300.0,
        real_now: Callable[[], float] = time.time,
    ) -> None:
        self._real_now = real_now
        self._real_anchor = real_now()
        self._sim_anchor = datetime.combine(sim_start_date, dtime.min, tzinfo=UTC)
        self.speedup = 86400.0 / sim_day_real_seconds

    def now(self) -> datetime:
        elapsed_real = self._real_now() - self._real_anchor
        return self._sim_anchor + timedelta(seconds=elapsed_real * self.speedup)

    def business_date(self) -> date:
        return business_date_of(self.now())

    def sim_seconds(self, real_seconds: float) -> float:
        """Simulated duration corresponding to a real duration."""
        return real_seconds * self.speedup


def business_date_of(ts: datetime) -> date:
    return ts.astimezone(UTC).date()


def to_iso(ts: datetime) -> str:
    """Event-contract timestamp format: ISO-8601, UTC, millisecond precision, 'Z' suffix."""
    return ts.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
