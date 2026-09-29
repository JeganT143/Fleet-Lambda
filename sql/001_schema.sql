-- Fleet application schema (AGENTS.md section 9 — database contract).
-- Runs once, on first start of an empty PostgreSQL volume
-- (mounted into /docker-entrypoint-initdb.d). All statements are idempotent.
--
-- Time columns:
--   event_timestamp  simulated event time (from the producer)
--   business_date    Sri Lankan (Asia/Colombo) date of event_timestamp (simulated)
--   ingestion_ts     real time Kafka received the record
--   processing_ts    real time Spark processed the record
-- Money columns are LKR (Sri Lankan rupees), distances are km, speeds km/h.
-- Timestamps are stored in UTC.

-- ---------------------------------------------------------------------------
-- Speed layer: raw validated events (also the history the batch layer reconciles)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stream_events (
    event_id         UUID PRIMARY KEY,               -- replays are idempotent (ON CONFLICT DO NOTHING)
    trip_id          TEXT,
    driver_id        TEXT             NOT NULL,
    vehicle_id       TEXT             NOT NULL,
    latitude         DOUBLE PRECISION NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    longitude        DOUBLE PRECISION NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    speed            DOUBLE PRECISION NOT NULL CHECK (speed >= 0),
    status           TEXT             NOT NULL CHECK (status IN ('idle', 'enroute', 'on_trip')),
    fare             NUMERIC(10, 2)   NOT NULL CHECK (fare >= 0),
    event_timestamp  TIMESTAMPTZ      NOT NULL,
    business_date    DATE             NOT NULL,
    zone             TEXT             NOT NULL,
    kafka_partition  INTEGER          NOT NULL,
    kafka_offset     BIGINT           NOT NULL,
    ingestion_ts     TIMESTAMPTZ      NOT NULL,
    processing_ts    TIMESTAMPTZ      NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_stream_events_date_vehicle ON stream_events (business_date, vehicle_id);
CREATE INDEX IF NOT EXISTS ix_stream_events_vehicle_ts   ON stream_events (vehicle_id, event_timestamp DESC);
CREATE INDEX IF NOT EXISTS ix_stream_events_ingestion    ON stream_events (ingestion_ts DESC);

-- Quarantine for malformed / invalid streaming records (stream keeps running)
CREATE TABLE IF NOT EXISTS rejected_events (
    rejected_id      BIGSERIAL PRIMARY KEY,
    raw_value        TEXT,
    reason           TEXT        NOT NULL,
    kafka_partition  INTEGER     NOT NULL,
    kafka_offset     BIGINT      NOT NULL,
    ingestion_ts     TIMESTAMPTZ NOT NULL,
    processing_ts    TIMESTAMPTZ NOT NULL,
    UNIQUE (kafka_partition, kafka_offset)
);

-- ---------------------------------------------------------------------------
-- Speed layer: event-time windowed metrics (one row per window, upserted)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS realtime_vehicle_metrics (
    window_start        TIMESTAMPTZ PRIMARY KEY,
    window_end          TIMESTAMPTZ      NOT NULL,
    business_date       DATE             NOT NULL,
    vehicles_reporting  INTEGER          NOT NULL,  -- distinct vehicles seen in window
    active_vehicles     INTEGER          NOT NULL,  -- distinct vehicles with an enroute/on_trip event
    idle_vehicles       INTEGER          NOT NULL,  -- vehicles_reporting - active_vehicles
    event_count         INTEGER          NOT NULL,
    idle_event_count    INTEGER          NOT NULL,
    idle_ratio          DOUBLE PRECISION NOT NULL,  -- idle_event_count / event_count
    trips_completed     INTEGER          NOT NULL,  -- COUNT(fare > 0)
    total_earnings      NUMERIC(12, 2)   NOT NULL,  -- SUM(fare)
    avg_fare            NUMERIC(10, 2),             -- AVG(fare) WHERE fare > 0
    updated_at          TIMESTAMPTZ      NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_rt_vehicle_metrics_date ON realtime_vehicle_metrics (business_date);

CREATE TABLE IF NOT EXISTS realtime_zone_metrics (
    window_start        TIMESTAMPTZ    NOT NULL,
    zone                TEXT           NOT NULL,
    window_end          TIMESTAMPTZ    NOT NULL,
    business_date       DATE           NOT NULL,
    vehicles_reporting  INTEGER        NOT NULL,
    active_vehicles     INTEGER        NOT NULL,
    event_count         INTEGER        NOT NULL,
    trips_completed     INTEGER        NOT NULL,
    total_earnings      NUMERIC(12, 2) NOT NULL,
    avg_fare            NUMERIC(10, 2),
    updated_at          TIMESTAMPTZ    NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, zone)
);
CREATE INDEX IF NOT EXISTS ix_rt_zone_metrics_date ON realtime_zone_metrics (business_date);

-- ---------------------------------------------------------------------------
-- Batch layer
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          BIGSERIAL PRIMARY KEY,
    pipeline_name   TEXT        NOT NULL,           -- e.g. 'expenses_load', 'reconciliation'
    business_date   DATE,
    status          TEXT        NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    rows_read       INTEGER,
    rows_written    INTEGER,
    rows_rejected   INTEGER,
    error_message   TEXT,
    airflow_run_id  TEXT
);
CREATE INDEX IF NOT EXISTS ix_pipeline_runs_name_date ON pipeline_runs (pipeline_name, business_date, started_at DESC);

CREATE TABLE IF NOT EXISTS vehicle_expenses (
    vehicle_id        TEXT           NOT NULL,
    business_date     DATE           NOT NULL,
    fuel_cost         NUMERIC(10, 2) NOT NULL CHECK (fuel_cost >= 0),
    maintenance_cost  NUMERIC(10, 2) NOT NULL CHECK (maintenance_cost >= 0),
    distance_covered  NUMERIC(10, 2) NOT NULL CHECK (distance_covered >= 0),
    service_flag      BOOLEAN        NOT NULL,
    source_file       TEXT           NOT NULL,
    run_id            BIGINT REFERENCES pipeline_runs (run_id),
    loaded_at         TIMESTAMPTZ    NOT NULL DEFAULT now(),
    PRIMARY KEY (vehicle_id, business_date)          -- re-runs replace, never duplicate
);
CREATE INDEX IF NOT EXISTS ix_vehicle_expenses_date ON vehicle_expenses (business_date);

-- Serving layer: reconciliation of stream history + daily expenses
CREATE TABLE IF NOT EXISTS daily_vehicle_profitability (
    vehicle_id            TEXT             NOT NULL,
    business_date         DATE             NOT NULL,
    trips                 INTEGER          NOT NULL,  -- completed trips (fare > 0 events)
    event_count           INTEGER          NOT NULL,
    on_trip_event_count   INTEGER          NOT NULL,
    stream_distance_km    NUMERIC(10, 2)   NOT NULL,  -- estimated from telemetry speed x interval
    distance_km           NUMERIC(10, 2)   NOT NULL,  -- odometer distance from expenses file
    earnings              NUMERIC(12, 2)   NOT NULL,
    fuel_cost             NUMERIC(10, 2)   NOT NULL,
    maintenance_cost      NUMERIC(10, 2)   NOT NULL,
    total_operating_cost  NUMERIC(12, 2)   NOT NULL,  -- fuel_cost + maintenance_cost
    estimated_profit      NUMERIC(12, 2)   NOT NULL,  -- earnings - fuel_cost - maintenance_cost
    utilization_rate      DOUBLE PRECISION NOT NULL CHECK (utilization_rate BETWEEN 0 AND 1),
    service_flag          BOOLEAN          NOT NULL,
    profitability_status  TEXT             NOT NULL
        CHECK (profitability_status IN ('profitable', 'watch', 'unprofitable')),
    run_id                BIGINT REFERENCES pipeline_runs (run_id),
    computed_at           TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (vehicle_id, business_date)
);
CREATE INDEX IF NOT EXISTS ix_daily_profit_date ON daily_vehicle_profitability (business_date);

-- ---------------------------------------------------------------------------
-- Data quality statistics (stream micro-batches and batch files)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS data_quality_stats (
    stat_id           BIGSERIAL PRIMARY KEY,
    pipeline_name     TEXT        NOT NULL,          -- 'stream_ingest' | 'expenses_load'
    batch_ref         TEXT        NOT NULL,          -- Spark micro-batch id or file path
    business_date     DATE,
    records_total     INTEGER     NOT NULL,
    records_valid     INTEGER     NOT NULL,
    records_rejected  INTEGER     NOT NULL,
    rule_failures     JSONB       NOT NULL DEFAULT '{}'::jsonb,  -- {"invalid_status": 3, ...}
    recorded_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_dq_stats_pipeline ON data_quality_stats (pipeline_name, recorded_at DESC);

-- ---------------------------------------------------------------------------
-- Alerts
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS alerts (
    alert_id       BIGSERIAL PRIMARY KEY,
    alert_type     TEXT        NOT NULL CHECK (alert_type IN ('no_stream_data', 'vehicle_idle', 'low_profitability')),
    severity       TEXT        NOT NULL CHECK (severity IN ('info', 'warning', 'critical')),
    vehicle_id     TEXT,                                    -- NULL for fleet-wide alerts
    business_date  DATE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    message        TEXT        NOT NULL,
    status         TEXT        NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'resolved')),
    resolved_at    TIMESTAMPTZ,
    dedup_key      TEXT        NOT NULL UNIQUE             -- same condition never raises twice
);
CREATE INDEX IF NOT EXISTS ix_alerts_status_created ON alerts (status, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_alerts_vehicle        ON alerts (vehicle_id);
