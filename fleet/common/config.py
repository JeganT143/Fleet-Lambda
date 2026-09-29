"""Central configuration, read from environment variables (see .env.example).

Every component reads settings through `load_settings()` so defaults live in one place.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date

from psycopg.conninfo import make_conninfo


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


def _int(name: str, default: int) -> int:
    return int(_env(name, str(default)))


def _float(name: str, default: float) -> float:
    return float(_env(name, str(default)))


@dataclass(frozen=True)
class Settings:
    # PostgreSQL
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str

    # Kafka
    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_topic_partitions: int

    # Simulation
    sim_day_real_seconds: float
    sim_start_date: date
    fleet_size: int
    stream_interval_seconds: float
    sim_random_seed: int

    # Streaming
    stream_window_duration: str
    stream_watermark_delay: str
    stream_trigger_interval: str
    spark_checkpoint_dir: str

    # Batch
    landing_dir: str

    # Alerts
    alert_no_data_seconds: int
    alert_idle_minutes: int
    alert_min_daily_profit: float

    # Profitability
    profitable_min_profit: float
    watch_min_profit: float

    log_level: str

    # Additions (Streaming agent). They have defaults so existing Settings(...) calls keep working.
    # Producer: share of EXTRA deliberately-invalid records injected to demo the quarantine path
    producer_invalid_event_rate: float = 0.005
    # Producer: real seconds between structured stats log lines
    producer_stats_interval_seconds: float = 30.0
    # Streaming: cap on Kafka records per micro-batch (bounds memory when replaying the topic)
    stream_max_offsets_per_trigger: int = 20000
    # Streaming: shuffle partitions for the windowed aggregations (tiny data, small machine)
    stream_shuffle_partitions: int = 2

    # Additions (Batch agent), also with defaults.
    # Expense generator --follow: real seconds between polls for completed business dates
    expense_poll_seconds: float = 30.0
    # Reconciliation: gaps between two events of a vehicle longer than this many SIMULATED
    # minutes are not counted as driving when estimating stream_distance_km
    stream_distance_max_gap_minutes: float = 30.0

    # Dashboard (Streamlit): internal URLs it calls, public URLs it links to
    api_base_url: str = "http://api:8000"
    api_public_url: str = "http://localhost:8000"
    airflow_base_url: str = "http://airflow:8080"
    airflow_public_url: str = "http://localhost:8080"
    airflow_admin_user: str = "admin"
    airflow_admin_password: str = ""

    @property
    def postgres_dsn(self) -> str:
        """libpq connection string for psycopg."""
        # make_conninfo quotes values, so passwords with spaces or quotes work
        return make_conninfo(
            host=self.postgres_host,
            port=self.postgres_port,
            dbname=self.postgres_db,
            user=self.postgres_user,
            password=self.postgres_password,
        )

    @property
    def postgres_jdbc_url(self) -> str:
        """JDBC URL for Spark (credentials are passed separately)."""
        return f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

    @property
    def sim_speedup(self) -> float:
        """Simulated seconds per real second (86400 / 300 = 288 by default)."""
        return 86400.0 / self.sim_day_real_seconds


def load_settings() -> Settings:
    return Settings(
        postgres_host=_env("POSTGRES_HOST", "localhost"),
        postgres_port=_int("POSTGRES_PORT", 5432),
        postgres_db=_env("POSTGRES_DB", "fleet"),
        postgres_user=_env("POSTGRES_USER", "fleet"),
        postgres_password=_env("POSTGRES_PASSWORD", ""),
        kafka_bootstrap_servers=_env("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
        kafka_topic=_env("KAFKA_TOPIC", "vehicle.telemetry"),
        kafka_topic_partitions=_int("KAFKA_TOPIC_PARTITIONS", 3),
        sim_day_real_seconds=_float("SIM_DAY_REAL_SECONDS", 300.0),
        sim_start_date=date.fromisoformat(_env("SIM_START_DATE", "2026-01-01")),
        fleet_size=_int("FLEET_SIZE", 20),
        stream_interval_seconds=_float("STREAM_INTERVAL_SECONDS", 1.0),
        sim_random_seed=_int("SIM_RANDOM_SEED", 42),
        stream_window_duration=_env("STREAM_WINDOW_DURATION", "1 hour"),
        stream_watermark_delay=_env("STREAM_WATERMARK_DELAY", "15 minutes"),
        stream_trigger_interval=_env("STREAM_TRIGGER_INTERVAL", "10 seconds"),
        spark_checkpoint_dir=_env("SPARK_CHECKPOINT_DIR", "/checkpoints"),
        landing_dir=_env("LANDING_DIR", "data/landing"),
        alert_no_data_seconds=_int("ALERT_NO_DATA_SECONDS", 60),
        alert_idle_minutes=_int("ALERT_IDLE_MINUTES", 240),
        alert_min_daily_profit=_float("ALERT_MIN_DAILY_PROFIT", 0.0),
        profitable_min_profit=_float("PROFITABLE_MIN_PROFIT", 2500.0),
        watch_min_profit=_float("WATCH_MIN_PROFIT", 0.0),
        log_level=_env("LOG_LEVEL", "INFO"),
        producer_invalid_event_rate=_float("PRODUCER_INVALID_EVENT_RATE", 0.005),
        producer_stats_interval_seconds=_float("PRODUCER_STATS_INTERVAL_SECONDS", 30.0),
        stream_max_offsets_per_trigger=_int("STREAM_MAX_OFFSETS_PER_TRIGGER", 20000),
        stream_shuffle_partitions=_int("STREAM_SHUFFLE_PARTITIONS", 2),
        expense_poll_seconds=_float("EXPENSE_POLL_SECONDS", 30.0),
        stream_distance_max_gap_minutes=_float("STREAM_DISTANCE_MAX_GAP_MINUTES", 30.0),
        api_base_url=_env("API_BASE_URL", "http://api:8000"),
        api_public_url=_env("API_PUBLIC_URL", "http://localhost:8000"),
        airflow_base_url=_env("AIRFLOW_BASE_URL", "http://airflow:8080"),
        airflow_public_url=_env("AIRFLOW_PUBLIC_URL", "http://localhost:8080"),
        airflow_admin_user=_env("AIRFLOW_ADMIN_USER", "admin"),
        airflow_admin_password=_env("AIRFLOW_ADMIN_PASSWORD", ""),
    )
