"""Small PostgreSQL helper (psycopg 3) used by Python components outside Spark."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row

from fleet.common.config import Settings, load_settings


@contextmanager
def connect(settings: Settings | None = None) -> Iterator[psycopg.Connection]:
    """Open a connection that commits on success and rolls back on error."""
    settings = settings or load_settings()
    with psycopg.connect(settings.postgres_dsn, row_factory=dict_row) as conn:
        yield conn
