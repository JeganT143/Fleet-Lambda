# Operations Guide

## Prerequisites
- Docker with Compose v2, about 4 GB of free RAM.
- Ports 5432, 8000, 8080 and 29092 must be free.
- Optional for linting on the host: [uv](https://docs.astral.sh/uv/). Run `uv venv -p 3.11 .venv && uv pip install -p .venv/bin/python -e '.[dev]'`.

## Configuration
All settings are environment variables. `make env` copies `.env.example` to `.env`, which is git-ignored. **Change the passwords in `.env`**, since the example values are placeholders.

| Group | Variables |
|---|---|
| PostgreSQL | `POSTGRES_*`, `AIRFLOW_DB*` |
| Kafka | `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `KAFKA_TOPIC_PARTITIONS` |
| Simulation | `SIM_DAY_REAL_SECONDS` (300), `SIM_START_DATE`, `FLEET_SIZE`, `STREAM_INTERVAL_SECONDS`, `SIM_RANDOM_SEED`, `PRODUCER_INVALID_EVENT_RATE` |
| Streaming | `STREAM_WINDOW_DURATION`, `STREAM_WATERMARK_DELAY`, `STREAM_TRIGGER_INTERVAL`, `STREAM_MAX_OFFSETS_PER_TRIGGER` |
| Batch | `LANDING_DIR`, `EXPENSE_POLL_SECONDS`, `STREAM_DISTANCE_MAX_GAP_MINUTES` |
| Alerts | `ALERT_NO_DATA_SECONDS` (60 real s), `ALERT_IDLE_MINUTES` (240 simulated min), `ALERT_MIN_DAILY_PROFIT` (0) |
| Profitability | `PROFITABLE_MIN_PROFIT` (800), `WATCH_MIN_PROFIT` (0) |

Containers read `.env` when they are created. After changing it, run `docker compose up -d <service>` to recreate the affected service.

## Everyday commands

| Command | What it does |
|---|---|
| `make build` | Build the `fleet-runtime` and `fleet-airflow` images |
| `make up` | Start the infrastructure (postgres, kafka, airflow) and wait until healthy |
| `make up-app` | Also start the producer, spark-streaming, api and expense-generator |
| `make ps` | Show service status |
| `make logs S=spark-streaming` | Follow one service's logs |
| `make psql` | Open a SQL shell on `fleet` |
| `make test` / `make test-unit` / `make test-integration` | Run pytest inside the runner container |
| `make lint` | Run Ruff check and format check |
| `make smoke` | Run the Spark ↔ Kafka ↔ JDBC infrastructure smoke test |
| `make down` | Stop everything and keep the data |
| `make reset` | Stop everything and **delete** this project's volumes (DB, Kafka, checkpoints) and the generated files in `data/landing/expenses/` |

## Service endpoints

| Service | URL |
|---|---|
| API and Swagger UI | http://localhost:8000/docs |
| Airflow UI | http://localhost:8080 (login from `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD`) |
| PostgreSQL | `localhost:5432`, database `fleet` |
| Kafka from the host | `localhost:29092` |

## Health and monitoring

- **Container health:** run `make ps`. Postgres, kafka, airflow and api have health checks.
- **API:** `curl localhost:8000/health` returns 200, or 503 if the database is unreachable.
- **Logs:** every Python component writes one JSON object per line, with `ts`, `level`, `component`, `message` and context fields. For example, `docker compose logs spark-streaming | grep '"component"'`.
- **Stream freshness:**
  ```sql
  SELECT now() - max(ingestion_ts) FROM stream_events;
  ```
- **Data quality:**
  ```sql
  SELECT * FROM data_quality_stats ORDER BY recorded_at DESC LIMIT 10;
  ```
- **Batch runs:** check the Airflow UI (DAG `fleet_daily_batch`) or:
  ```sql
  SELECT * FROM pipeline_runs ORDER BY run_id DESC LIMIT 10;
  ```
- **Alerts:** `curl 'localhost:8000/api/v1/alerts?status=open'`.

## Airflow DAGs

| DAG | Schedule | Tasks |
|---|---|---|
| `fleet_daily_batch` | every 2 min | resolve_business_date → wait_for_expense_file → validate_expenses → load_expenses (spark-submit) → reconcile (spark-submit) → raise_profitability_alerts |
| `fleet_stream_alerts` | every minute | check_no_stream_data, check_idle_vehicles |

Without `conf`, a scheduled run processes the **oldest completed** business date that has a landing file and no successful reconciliation yet. If there is none, it skips. To process one specific date, or re-run it:

```bash
docker compose exec airflow airflow dags trigger fleet_daily_batch \
  --conf '{"business_date": "2026-01-02"}'
```

Re-runs are safe, because they replace that date's rows in one transaction.

## Failure handling runbook

| Symptom | Likely cause | Action |
|---|---|---|
| `no_stream_data` alert open | Producer stopped or Kafka down | Check `make ps` and `make logs S=producer`. It auto-resolves once events flow again. |
| Realtime metrics not updating | Spark streaming job died | Check `make logs S=spark-streaming`. The container restarts automatically and resumes from its checkpoint. |
| `validate_expenses` failed | Invalid expense file | The task log lists each failed rule with example lines, and nothing is loaded. Fix or regenerate the file (`python -m fleet.batch.generator --business-date D`), then re-trigger. Later dates wait behind a failed date. |
| `reconcile` failed | DB or Spark error | Airflow retries it. The previous day's rows are untouched, because the staging swap is atomic. |
| API returns 503 | PostgreSQL unreachable | Check `make ps`, then restart postgres. |
| Container `OOMKilled` | Machine low on RAM | Close other applications. Memory limits are in docker-compose.yml. |

## Changing the streaming aggregation
Window, watermark and `APPROX_DISTINCT_RSD` changes alter the Spark state schema. Stop `spark-streaming` and delete the `fleet_metrics` and `zone_metrics` directories in the `fleet_spark-checkpoints` volume, then start it again. Metrics are recomputed from Kafka. **Do not delete the `ingest` checkpoint**, because the events are already stored.
