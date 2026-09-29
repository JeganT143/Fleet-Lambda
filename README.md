# Fleet Lambda: Ride-Hailing Fleet Operations Pipeline

A small, end-to-end **Lambda architecture** data pipeline for a simulated ride-hailing fleet
in **Colombo, Sri Lanka** (money in LKR, business days in Sri Lanka time).
It answers one question: *which vehicles are under-utilised or unprofitable, and where and
when is the fleet earning money?*

```
Speed layer   Python producer ──► Kafka (3 partitions, key=vehicle_id) ──► Spark Structured Streaming ──┐
                                                                                                      ├─► PostgreSQL ──► FastAPI
Batch layer   Daily expense CSV ──► Airflow ──► Spark batch (load + reconcile with stream history) ────┘
```

One simulated business day lasts **5 real minutes**.

## Quick start

```bash
make env        # create .env from .env.example, then change the passwords
make build      # build the images (first time: several minutes)
make up         # postgres, kafka, airflow (waits until healthy)
make up-app     # producer, spark-streaming, api, expense-generator, dashboard
```

| What | Where |
|---|---|
| **Dashboard (start here)** | http://localhost:8501 |
| API docs | http://localhost:8000/docs |
| Airflow | http://localhost:8080 |
| SQL shell | `make psql` |
| Tests | `make test` |
| Lint | `make lint` |

The first daily report appears about 6–8 minutes after start: simulated day 1 must complete (5 min), then the expense file is generated (≤ 30 s poll) and the batch DAG runs (every 2 min, ≈ 30 s):

```bash
curl -s localhost:8000/api/v1/reports/daily/2026-01-01 | python3 -m json.tool
```

Requirements: Docker with Compose v2 and about 4 GB of free RAM.

## Repository layout

```
fleet/common/     shared contracts, config, zones, simulated clock, JSON logging, vehicle profiles
fleet/producer/   telemetry simulator + Kafka producer
fleet/streaming/  Spark Structured Streaming job (validation, enrichment, windows, sinks)
fleet/batch/      expense generator, file validation, Spark batch jobs, reconciliation
fleet/alerts/     alert rules and evaluator
fleet/api/        FastAPI app, Pydantic schemas, repository
fleet/dashboard/  Streamlit dashboard (reads the API, triggers Airflow re-runs)
airflow/dags/     fleet_daily_batch, fleet_stream_alerts
sql/              database schema
docker/           Dockerfiles, smoke test
tests/            unit + integration tests
docs/             documentation (below)
```

## Documentation

| Document | Contents |
|---|---|
| [docs/architecture.md](docs/architecture.md) | Diagram, Lambda vs Kappa, stack, design contracts |
| [docs/decisions.md](docs/decisions.md) | Architecture decision records |
| [docs/data-model.md](docs/data-model.md) | Tables, keys, metric definitions |
| [docs/operations.md](docs/operations.md) | Configuration, commands, monitoring, failure runbook |
| [docs/report.md](docs/report.md) | Full technical report, including measured results and limitations |
| [docs/demo-runbook.md](docs/demo-runbook.md) | Step-by-step reproducible demo |
| [docs/rubric-mapping.md](docs/rubric-mapping.md) | Acceptance criteria → implementation → evidence |

## Stack
Python 3.11 · Apache Kafka 3.6 (Confluent 7.6 image, KRaft) · Apache Spark 3.5.4 (Structured Streaming + batch) ·
Apache Airflow 2.10.5 · PostgreSQL 16 · FastAPI · Streamlit · Docker Compose · pytest · Ruff
