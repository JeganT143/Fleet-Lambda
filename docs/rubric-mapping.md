# Rubric Mapping

Each acceptance criterion from `TASKS.md`, with where it is implemented and how to verify it.
Demo steps refer to [demo-runbook.md](demo-runbook.md).

| # | Criterion | Implementation | Evidence / how to verify |
|---|---|---|---|
| 1 | Continuous Kafka ingestion | `fleet/producer/main.py`, `simulator.py` | Producer stats logs; `stream_events` count grows (demo 1–2) |
| 2 | Kafka partitions and message keys | Topic created with 3 partitions; key = `vehicle_id` | `kafka-topics --describe`; `tests/integration/test_kafka_producer.py` (same vehicle → same partition) |
| 3 | Spark Structured Streaming | `fleet/streaming/job.py` (3 queries, checkpoints) | `make logs S=spark-streaming` (per-batch JSON logs) |
| 4 | Meaningful streaming transformations | `fleet/streaming/transforms.py`, `validation.py`: parse, validate, quarantine, zone, business date, timestamps | `rejected_events`; `tests/unit/test_streaming_*.py` |
| 5 | Time-window aggregation | 1 h event-time tumbling windows, 15 min watermark | `realtime_vehicle_metrics`, `realtime_zone_metrics`; exact-number unit test |
| 6 | Daily batch ingestion | `fleet/batch/generator.py --follow` → landing CSV | `data/landing/expenses/<date>/` (demo 3) |
| 7 | Airflow orchestration | `airflow/dags/fleet_daily_batch.py`, `fleet_stream_alerts.py` | Airflow UI, sensor + retries + task logs (demo 3) |
| 8 | Spark batch processing | `fleet/batch/jobs.py` (`load`, `reconcile`), `transforms.py` | `pipeline_runs`; `tests/unit/test_batch_transforms.py` |
| 9 | PostgreSQL storage | `sql/001_schema.sql` (9 tables, PKs, CHECKs, indexes) | [data-model.md](data-model.md); `make psql` |
| 10 | Streaming/batch reconciliation | `reconcile` job joins `stream_events` with `vehicle_expenses` | `daily_vehicle_profitability`; integration test `test_batch_postgres.py` |
| 11 | Fleet utilisation | Real time: `idle_ratio`, active/idle vehicles. Daily: `utilization_rate = on_trip / all events` | `/api/v1/fleet/summary`; daily report (demo 2, 4) |
| 12 | Vehicle profitability | `fleet/batch/profitability.py` (single definition, configurable thresholds) | `/api/v1/reports/daily/{date}`; boundary tests |
| 13 | FastAPI serving | `fleet/api/` (6 required + 5 dashboard endpoints, Pydantic models, repository); Streamlit dashboard `fleet/dashboard/` | http://localhost:8000/docs, http://localhost:8501; `tests/unit/test_api*.py`, `tests/unit/test_dashboard.py`, `tests/integration/test_api.py`, `test_dashboard_live.py` |
| 14 | Threshold-based alerts | `fleet/alerts/rules.py`, `evaluator.py`; dedup + auto-resolve | `/api/v1/alerts`; no-data demo (demo 7); `test_alert_rules.py`, `test_alerts_postgres.py` |
| 15 | Structured logging | `fleet/common/logs.py` (JSON lines) in every component | `docker compose logs <service>` |
| 16 | Data-quality validation | Stream: quarantine + `data_quality_stats`. Batch: whole-file rejection | Demo 6; `tests/unit/test_*validation.py` |
| 17 | Idempotent batch processing | Staging-table swap in one transaction; natural keys | Demo 5; idempotency integration test |
| 18 | Unit and integration testing | `tests/unit`, `tests/integration` (242 tests) | `make test` |
| 19 | Dockerised local execution | `docker-compose.yml`, `docker/`, `Makefile` | `make build && make up && make up-app` |
| 20 | Complete technical documentation | `README.md`, `docs/*.md` | This folder |
| 21 | Reproducible end-to-end demo | [demo-runbook.md](demo-runbook.md) from `make reset` | Follow the runbook |

## Report coverage (`docs/report.md`)
Problem §1 · Business question §2 · Lambda §3 · Lambda vs Kappa §4 · Stack §5 · Kafka §6 ·
Streaming §7 · Batch §8 · Data model §9 · Reconciliation §10 · API §11 · Observability §12 ·
Data quality §13 · Failure handling §14 · Idempotency §15 · Testing §16 · Simulated time §17 ·
Results §18 · Limitations §19 · Production considerations §20.
