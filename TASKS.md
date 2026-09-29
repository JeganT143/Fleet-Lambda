# Data Engineering Mini-Project — Tasks

## Project

**Use Case:** Ride-Hailing Fleet Operations  
**Architecture:** Lambda Architecture  
**Simulated day:** 5 real minutes

## Required Stack

- Python
- Apache Kafka
- Apache Spark Structured Streaming
- Apache Spark Batch
- Apache Airflow
- PostgreSQL
- FastAPI
- Docker Compose
- pytest
- Ruff

---

# Phase 0 — Repository & Architecture

- [x] Inspect existing repository and environment.
- [x] Confirm Python, Docker, and Docker Compose availability.
- [x] Create project structure.
- [x] Create `.env.example`.
- [x] Create `pyproject.toml`.
- [x] Create initial `README.md`.
- [x] Create `docs/architecture.md`.
- [x] Create Lambda Architecture Mermaid diagram.
- [x] Document Lambda vs Kappa decision.
- [x] Document technology-stack decisions.

---

# Phase 1 — Infrastructure

## Docker

- [x] Create Docker Compose configuration.
- [x] Add Kafka.
- [x] Add PostgreSQL.
- [x] Add Spark.
- [x] Add Airflow.
- [x] Add API service where appropriate. (defined; starts once Phase 8 code exists)
- [x] Add health checks.
- [x] Verify all services start successfully.

## PostgreSQL

- [x] Create database initialization/schema.
- [x] Create required tables.
- [x] Add primary keys and useful indexes.
- [x] Add uniqueness constraints needed for idempotency.

Initial tables:

- `stream_events`
- `realtime_vehicle_metrics`
- `vehicle_expenses`
- `daily_vehicle_profitability`
- `alerts`
- `pipeline_runs`

---

# Phase 2 — Streaming Producer

Create a realistic Python vehicle telemetry simulator.

## Event fields

- [x] `event_id`
- [x] `trip_id`
- [x] `driver_id`
- [x] `vehicle_id`
- [x] `latitude`
- [x] `longitude`
- [x] `speed`
- [x] `status`
- [x] `fare`
- [x] `timestamp`

## Behavior

- [x] Generate multiple vehicles.
- [x] Generate coherent trip lifecycles.
- [x] Simulate `idle -> enroute -> on_trip -> idle`.
- [x] Generate realistic speed/fare values.
- [x] Use simulated timestamps.
- [x] Generate events continuously.

## Kafka

- [x] Create `vehicle.telemetry` topic.
- [x] Configure multiple partitions.
- [x] Use `vehicle_id` as message key.
- [x] Serialize events as JSON.
- [x] Implement producer retries/error handling.
- [x] Add structured logging.
- [x] Test producer independently.

---

# Phase 3 — Spark Structured Streaming

Implement:

`Kafka -> Spark Structured Streaming -> PostgreSQL`

- [x] Read Kafka topic.
- [x] Parse JSON.
- [x] Validate events.
- [x] Reject malformed records without stopping the stream.
- [x] Add processing timestamp.
- [x] Derive simulated business date.
- [x] Derive geographic zone.
- [x] Use event-time processing.
- [x] Implement time-window aggregation.
- [x] Calculate real-time fleet metrics.
- [x] Write results to PostgreSQL.
- [x] Track rejected/processed record counts.
- [x] Add logging.
- [x] Verify metrics change while producer is running.

## Real-time metrics

At minimum:

- [x] active vehicles
- [x] idle vehicles
- [x] idle ratio
- [x] trips/events per hour
- [x] total earnings
- [x] average fare
- [x] earnings by zone

---

# Phase 4 — Daily Batch Source

Create a Python generator for daily vehicle expenses.

Required fields:

- [x] `vehicle_id`
- [x] `fuel_cost`
- [x] `maintenance_cost`
- [x] `distance_covered`
- [x] `service_flag`
- [x] `business_date`

- [x] Generate realistic cost variation.
- [x] Include low-utilization vehicles.
- [x] Include high-cost vehicles.
- [x] Include maintenance-heavy vehicles.
- [x] Write files to a landing directory.
- [x] Validate generated data.
- [x] Add structured logging.

Example:

`data/landing/expenses/<business_date>/vehicle_expenses.csv`

---

# Phase 5 — Airflow + Spark Batch

Implement:

`Batch File -> Airflow -> Spark Batch -> PostgreSQL`

- [x] Create Airflow DAG.
- [x] Detect/wait for daily batch file.
- [x] Validate input.
- [x] Run Spark batch processing.
- [x] Clean/transform batch data.
- [x] Load `vehicle_expenses`.
- [x] Make processing idempotent.
- [x] Record pipeline execution.
- [x] Handle failures clearly.
- [x] Add task logging.

---

# Phase 6 — Reconciliation & Business Logic

Combine historical streaming information with daily expenses.

For every vehicle/day calculate:

- [x] trips
- [x] distance
- [x] earnings
- [x] fuel cost
- [x] maintenance cost
- [x] total operating cost
- [x] estimated profit
- [x] utilization rate
- [x] service flag
- [x] profitability status

Formula:

`estimated_profit = earnings - fuel_cost - maintenance_cost`

Define and document utilization mathematically.

Define configurable profitability thresholds.

Statuses:

- `profitable`
- `watch`
- `unprofitable`

Store results in:

`daily_vehicle_profitability`

---

# Phase 7 — Alerts

Implement database-backed alerts.

Required:

- [x] No streaming data for configurable period.
- [x] Vehicle idle beyond configurable threshold.
- [x] Vehicle profitability below configurable threshold.

Alert fields:

- [x] `alert_id`
- [x] `alert_type`
- [x] `severity`
- [x] `vehicle_id`
- [x] `business_date`
- [x] `created_at`
- [x] `message`
- [x] `status`

---

# Phase 8 — FastAPI

Implement:

- [x] `GET /health`
- [x] `GET /api/v1/fleet/summary`
- [x] `GET /api/v1/fleet/zones`
- [x] `GET /api/v1/vehicles/{vehicle_id}`
- [x] `GET /api/v1/alerts`
- [x] `GET /api/v1/reports/daily/{business_date}`

Requirements:

- [x] Pydantic response models.
- [x] Proper HTTP status codes.
- [x] Database abstraction.
- [x] Error handling.
- [x] API logging.
- [x] API tests.

---

# Phase 9 — Data Quality

Implement validation for:

- [x] Required fields.
- [x] Valid status.
- [x] Non-negative speed.
- [x] Non-negative fare.
- [x] Valid coordinates.
- [x] Valid vehicle IDs.
- [x] Non-negative costs.
- [x] Valid business dates.

- [x] Quarantine/reject invalid streaming records.
- [x] Fail invalid batch input clearly.
- [x] Record validation statistics.

---

# Phase 10 — Testing

## Unit tests

- [x] Event generation.
- [x] Zone calculation.
- [x] Validation.
- [x] Utilization calculation.
- [x] Profitability calculation.
- [x] Alert thresholds.
- [x] Batch transformations.

## Integration tests

- [x] Kafka producer/consumer.
- [x] Spark -> PostgreSQL.
- [x] Batch -> PostgreSQL.
- [x] API -> PostgreSQL.

- [x] Ensure deterministic fixtures.
- [x] Run complete pytest suite.
- [x] Fix failures rather than weakening tests.

---

# Phase 11 — Observability

- [x] Structured JSON logging.
- [x] Producer logs.
- [x] Spark processing logs.
- [x] Airflow task visibility.
- [x] Batch processing logs.
- [x] Database load statistics.
- [x] API request/latency logs.
- [x] Pipeline run tracking.
- [x] Health checks.
- [x] Alert visibility.

---

# Phase 12 — Documentation

Create:

- [x] `README.md`
- [x] `docs/architecture.md`
- [x] `docs/decisions.md`
- [x] `docs/data-model.md`
- [x] `docs/operations.md`
- [x] `docs/report.md`
- [x] `docs/demo-runbook.md`
- [x] `docs/rubric-mapping.md`

The report must cover:

- [x] Problem definition.
- [x] Business question.
- [x] Lambda architecture.
- [x] Lambda vs Kappa.
- [x] Technology-stack justification.
- [x] Kafka design.
- [x] Streaming processing.
- [x] Batch processing.
- [x] Data model.
- [x] Reconciliation.
- [x] API.
- [x] Observability.
- [x] Data quality.
- [x] Failure handling.
- [x] Idempotency.
- [x] Testing.
- [x] Simulated time.
- [x] Results.
- [x] Limitations.
- [x] Production considerations.

Do not fabricate performance results.

---

# Phase 13 — Final Validation

- [ ] Start system from clean environment. (NOT RUN: `make reset` needs user approval - deletes project volumes)
- [x] Verify Kafka.
- [x] Verify PostgreSQL.
- [x] Verify Spark.
- [x] Verify Airflow.
- [x] Start streaming producer.
- [x] Verify real-time metrics.
- [x] Generate daily batch.
- [x] Verify Airflow processing.
- [x] Verify Spark batch.
- [x] Verify reconciliation.
- [x] Verify profitability report.
- [x] Verify alerts.
- [x] Verify API.
- [x] Run complete test suite.
- [x] Verify documentation. (Reviewer audit; drift findings fixed)
- [x] Verify rubric mapping. (Reviewer audit: 20/21 PASS; #21 fixed via Makefile, clean run pending)

---

# Phase 14 — Dashboard (added on request)

- [x] Streamlit dashboard covering every feature (6 pages).
- [x] Reads only the FastAPI serving layer (5 read-only endpoints added).
- [x] Airflow REST API: DAG run states + idempotent re-run of a business date.
- [x] Docker Compose service `dashboard` (port 8501) with health check.
- [x] Unit tests (all pages render against the real API with a fake repository; clients; failure states).
- [x] Live integration test (all pages against running services) and a live re-run check.
- [x] Documentation updated.

---

# Final Acceptance Criteria

The project must demonstrate:

1. Continuous Kafka ingestion.
2. Kafka partitions and message keys.
3. Spark Structured Streaming.
4. Meaningful streaming transformations.
5. Time-window aggregation.
6. Daily batch ingestion.
7. Airflow orchestration.
8. Spark batch processing.
9. PostgreSQL storage.
10. Streaming/batch reconciliation.
11. Fleet utilization.
12. Vehicle profitability.
13. FastAPI serving.
14. Threshold-based alerts.
15. Structured logging.
16. Data-quality validation.
17. Idempotent batch processing.
18. Unit and integration testing.
19. Dockerized local execution.
20. Complete technical documentation.
21. A reproducible end-to-end demonstration.