# Technical Report — Ride-Hailing Fleet Operations on a Lambda Architecture

## 1. Problem definition
A ride-hailing operator in **Colombo, Sri Lanka** runs a fleet of 20 cars and vans. Telemetry (position, speed, status,
fares) streams continuously from every vehicle. Operating costs (fuel, maintenance,
odometer distance, service visits) come once a day as a file from a separate expense
system. Today the two are never combined, so the operator cannot see whether a vehicle
that looks busy actually makes money.

## 2. Business question
> *Which vehicles are under-utilised or unprofitable, and where and when is the fleet earning money?*

This splits into a **real-time** question (how many vehicles are active or idle right now,
and what is being earned per zone) and a **daily** question (per vehicle: trips, earnings,
costs, estimated profit, utilisation, status).

## 3. Lambda architecture
See the diagram in [architecture.md](architecture.md).

- **Speed layer:** Python producer → Kafka → Spark Structured Streaming → PostgreSQL (`stream_events`, `realtime_*_metrics`).
- **Batch layer:** expense CSV → Airflow → Spark batch → PostgreSQL (`vehicle_expenses`, `daily_vehicle_profitability`).
- **Serving layer:** PostgreSQL read through FastAPI.

The batch layer recomputes the authoritative daily view from the full stored event history.
Any speed-layer approximation (approximate distinct counts, events dropped by the watermark)
is therefore corrected in the daily report.

## 4. Lambda vs Kappa
Kappa (a single streaming path, reprocessing by replay) is simpler when every source is a
stream. Here one source is inherently a **daily file**, and the business wants an auditable
**daily** report. Lambda maps directly onto these two needs. Its known cost is duplicated
logic. We limit that in two ways. Shared rules and constants live in
`fleet/common/contracts.py`. And the two paths compute different things (live windows vs
per-vehicle daily reconciliation), with identical metric definitions
(`earnings = SUM(fare)`, `trips = COUNT(fare > 0)`). See ADR-001 in [decisions.md](decisions.md).

## 5. Technology-stack justification
| Component | Why it was chosen | Alternatives rejected |
|---|---|---|
| Kafka (KRaft, 1 broker) | Durable, partitioned, replayable log; key-based ordering | RabbitMQ (no replay); ZooKeeper mode (extra container) |
| Spark Structured Streaming | Event-time windows, watermarks, checkpointed exactly-once source offsets | Plain Python consumer (no windowing/state management) |
| Spark batch | Same DataFrame API and engine as streaming | pandas (single machine, second API) |
| Airflow | Sensors, retries, run history and task logs in a UI | cron (no visibility or retries) |
| PostgreSQL | Constraints make idempotent upserts easy; SQL serving | A data lake (overkill for this volume) |
| FastAPI + Pydantic | Typed responses, automatic OpenAPI docs | Flask (no built-in validation) |
| Docker Compose | One-command reproducible environment | Kubernetes (overkill locally) |

The machine has about 7 GB RAM. Spark runs in local mode inside containers, and Airflow runs
scheduler and webserver in one container with the `LocalExecutor`. Every service has a
memory limit (ADR-002).

## 6. Kafka design
- Topic `vehicle.telemetry` has **3 partitions**. The producer creates it (broker auto-create is disabled, so the partition count is deliberate).
- **Message key = `vehicle_id`**. All events of a vehicle go to the same partition, which preserves per-vehicle order (needed for stream distance and idle detection).
- Values are JSON following the event contract.
- The producer uses `acks=all`, idempotence, retries with backoff and a delivery callback that counts failures.
- On restart it continues simulated time from the newest timestamp already in the topic, so a restart never replays a business day.
- **Observed skew:** 20 vehicle ids hash to partitions as 5/5/10, so partition 2 holds twice as much data (12,910 / 12,910 / 25,820 events at the time of writing). Key-based partitioning trades balance for ordering. With a larger fleet the distribution evens out.

## 7. Streaming processing
The Spark job (`fleet/streaming/job.py`) runs three queries on the same Kafka source:

1. **Ingest.**
   - Parse JSON with every field read as a string, so bad values are caught rather than turned into NULLs.
   - Validate against the contract; the reason codes are identical to the Python validator, and a test proves it.
   - Enrich with `business_date`, `zone` (3×3 grid), `processing_ts`, `ingestion_ts` and Kafka lineage.
   - Valid rows go to `stream_events` (insert-or-ignore on `event_id`). Invalid rows go to `rejected_events`, so the stream never stops.
   - Counts per rule go to `data_quality_stats`.
2. **Fleet metrics.**
   - 1-hour (simulated) tumbling **event-time windows** with a 15-minute watermark, in update mode.
   - Upserted into `realtime_vehicle_metrics`.
   - Metrics: vehicles reporting, active, idle, idle ratio, events/trips per hour, earnings, average fare.
3. **Zone metrics.** The same windows grouped by zone, written to `realtime_zone_metrics`.

`idle_ratio = idle events / all events` (a time share, because every vehicle reports at a
fixed cadence). Distinct vehicle counts use `approx_count_distinct(rsd=0.02)`. A unit test
proves this is exact for the fixed ids V001–V050. The precision was lowered from 0.01 after
measuring memory pressure: a metrics micro-batch took about 5.4 s at 0.01 and about 2.1 s at 0.02.

## 8. Batch processing
- The DAG `fleet_daily_batch` runs every 2 minutes.
- **Date selection:**
  - It picks the oldest *completed* business date that has a landing file and no successful reconciliation.
  - A date is completed when the newest *ingested* event belongs to a later date.
  - `dag_run.conf` can override the date.
- Tasks: `resolve_business_date → wait_for_expense_file` (sensor, reschedule mode) `→ validate_expenses → load_expenses` (spark-submit) `→ reconcile` (spark-submit) `→ raise_profitability_alerts`.
- The Spark load reads the CSV with an explicit schema, cleans and casts it, and loads `vehicle_expenses`.
- The expense generator (`fleet/batch/generator.py --follow`) writes the file for each completed simulated day. Files are written atomically: a temp file, then a rename.

## 9. Data model
See [data-model.md](data-model.md). There are nine tables. Four kinds of timestamp are kept
separate: simulated event time, business date, real ingestion time and real processing time.

## 10. Reconciliation
For each `(vehicle_id, business_date)`, the reconcile job reads that date's `stream_events`
via JDBC (predicate pushed down) and joins them with `vehicle_expenses`:

```
trips                = COUNT(events with fare > 0)
earnings             = SUM(fare)
utilization_rate     = on_trip events / all events                  (0..1)
stream_distance_km   = Σ speed_kmh × hours since previous event of the vehicle
                       (LAG window; gaps > 30 simulated minutes ignored)
total_operating_cost = fuel_cost + maintenance_cost
estimated_profit     = earnings − fuel_cost − maintenance_cost
status               = profitable   if profit ≥ LKR 2,500
                       watch        if profit ≥ 0
                       unprofitable otherwise                      (thresholds configurable)
```

`estimated_profit` is an operational estimate. It excludes driver pay, insurance and
depreciation.

Edge cases:
- A vehicle with expenses but no events gets zero trips and earnings, and its costs still count.
- A vehicle with events but no expense row is **excluded** rather than given fake zero costs. It is logged as a warning and counted in `data_quality_stats` (`missing_expense_row`).
- The telemetry-derived distance and the odometer distance come from independent systems and are both kept. The difference is itself a useful reconciliation signal.

The profitability rule is defined once in Python (`fleet/batch/profitability.py`). The Spark
expression is built from the same thresholds, and a test checks that both agree on boundary
values.

## 11. API
FastAPI (`fleet/api/`) is read-only over the serving tables and holds no business logic.

| Endpoint | Purpose |
|---|---|
| `GET /health` | 200, or 503 when the DB is unreachable |
| `GET /api/v1/fleet/summary` | Latest window, day totals, last ingestion time, open alerts |
| `GET /api/v1/fleet/zones` | Per-zone earnings, trips and average fare for a business date |
| `GET /api/v1/vehicles/{id}` | Latest position and status, today's stats, last 7 daily rows, open alerts |
| `GET /api/v1/alerts` | Filtered by status, type and vehicle, with a limit |
| `GET /api/v1/reports/daily/{date}` | Daily profitability report with a summary and per-vehicle rows |
| `GET /api/v1/reports/daily` | Business dates that have a report, with status counts |
| `GET /api/v1/fleet/windows` | Recent fleet windows for charts |
| `GET /api/v1/vehicles` | Latest position and status of every vehicle |
| `GET /api/v1/pipeline/runs` | Recent batch job runs |
| `GET /api/v1/data-quality` | Stream validation totals, quarantine reasons, Kafka partitions, batch checks |

It uses Pydantic response models and a repository class with parameterised SQL. Status codes:
422 for invalid input (vehicle id pattern, date, filter values), 404 for unknown resources and
503 when the database is down. Every request logs one JSON line with latency and a request id,
which is echoed in `X-Request-ID`.

**Dashboard.** A Streamlit app (`fleet/dashboard/`, http://localhost:8501) presents every
feature on six pages: Overview (health and architecture), Live Fleet (windows, map, zones,
auto-refresh), Vehicles, Daily Report (profitability and reconciliation), Alerts, and
Pipeline & Quality. It reads only the API. For orchestration it calls the Airflow REST API,
to list DAG runs and to re-run a business date as a live idempotency demo, so it holds no
business logic and never touches the database directly.

## 12. Observability
- Structured JSON logs from the producer (sent/failed stats), the Spark job (per micro-batch: query, batch id, rows, rejected, duration), the batch jobs, the alert checks and the API (latency).
- Airflow UI: task states, retries and task logs.
- `pipeline_runs`: every batch job with status, row counts, duration and error message.
- `data_quality_stats`: validation counts per micro-batch and per file.
- Health checks: container health checks for postgres, kafka, airflow and api, plus the API `/health` endpoint.
- Alerts are visible through `/api/v1/alerts`.

## 13. Data quality
| Rule | Stream | Batch |
|---|---|---|
| Required fields present | ✔ `missing_<field>` | ✔ `missing_value` |
| Valid status | ✔ | – |
| Non-negative speed (and ≤ 200 km/h) / fare | ✔ | – |
| Valid coordinates | ✔ | – |
| Valid vehicle/driver ids | ✔ | ✔ |
| Non-negative costs and distance | – | ✔ |
| Valid business date (and matches folder) | ✔ timestamp parseable | ✔ |
| Malformed JSON / duplicate vehicle | ✔ | ✔ |
| **On failure** | Quarantine row, stream continues | Whole file rejected, nothing loaded, run marked failed |

## 14. Failure handling
| Failure | Behaviour |
|---|---|
| Kafka unavailable at producer start | Retries with backoff; delivery failures counted and logged |
| Producer or Spark container crash | `restart: unless-stopped`. Spark resumes from its checkpoint, and replays are harmless (insert-or-ignore / upsert). |
| Database error in a micro-batch | The batch fails loudly (no swallowed exceptions), the job restarts and the same offsets are reprocessed |
| Invalid expense file | `AirflowFailException` (no pointless retries), failed `pipeline_runs` row, zero rows loaded |
| Batch job crash mid-write | The staging-table swap happens in one transaction, so the target table is unchanged |
| Stream stops | A `no_stream_data` alert is raised within about 1 minute and auto-resolves when data resumes |

## 15. Idempotency
- Every write is keyed by a natural key (see the table in [data-model.md](data-model.md)).
- The batch layer writes through a per-run staging table. One transaction then deletes the date's rows, inserts from staging and drops the staging table.
- Verified live: re-running 2026-01-01 gave the same 20 rows and the same profit sum (₹25,225.79), and one extra `success` run was recorded.
- Alerts are de-duplicated by `dedup_key`.

## 16. Testing
242 tests run in the `runner` container. The last full run passed with none skipped.

- **Unit tests:**
  - simulator lifecycle and determinism
  - validation (one case per rule, and Python vs Spark agreement)
  - the zone grid (Python vs Spark over about 2,100 points)
  - window aggregation with exact numbers
  - expense generator and file validation
  - reconciliation on hand-made DataFrames
  - profitability thresholds (Python vs Spark, including exact boundaries)
  - alert rules
  - API endpoints with a fake repository
  - dashboard pages rendered with Streamlit AppTest against the real API (fake repository), plus the HTTP clients
- **Integration tests:**
  - Kafka producer/consumer on a throw-away 3-partition topic
  - Spark writers → PostgreSQL, re-run without duplicates
  - batch load and reconcile → PostgreSQL, idempotent re-run and invalid file
  - alert deduplication
  - API → PostgreSQL with exact values
  - dashboard pages rendered against the live API and Airflow
- **Deterministic fixtures:** seeded generators, and reserved test ids (V9xx) and dates (2099). Tests delete only their own rows.

## 17. Simulated time
- 1 simulated day = 300 real seconds, a speed-up of 288×.
- A business day is a Sri Lankan calendar day (Asia/Colombo, UTC+05:30); the simulation starts at 00:00 Colombo time. Stored timestamps are UTC.
- With a 1-second tick, each vehicle emits one event every 4.8 simulated minutes, which is 300 events per vehicle per day and 6,000 per day for 20 vehicles.
- Only the producer owns a clock. Everyone else derives time from the event timestamps (ADR-003).
- Real-time checks (no-data alert) use real ingestion time. Simulated-time checks (idle alert) compare against the latest fleet event time.

## 18. Results (measured on the development machine, 2026-09-29)
> **Note:** these figures were measured *before* the switch to Colombo / LKR / Sri Lankan
> business dates. The money and zone rows below are in INR with the old zone names, and will
> be re-measured on the Sri Lankan data. Row counts, latency and job durations do not depend
> on the locale.

These figures come from one run of about 1 hour of real time, covering about 9 simulated
days. They are observations, not benchmarks.

| Measure | Value |
|---|---|
| Events stored | 51,640 over 9 business dates (6,000 per complete day) |
| Records quarantined | 260: invalid_status 92, invalid_speed 88, malformed_json 80 |
| Throughput (last 10 min) | ≈ 18 events per real second |
| End-to-end latency, producer send (Kafka record timestamp) → stored in PostgreSQL | mean 8.1 s, p50 8.0 s, p95 12.7 s (10 s trigger interval) |
| Batch job duration (mean) | load 6.8 s, reconcile 8.4 s; a full DAG run about 30 s |
| Vehicle-days reconciled | 160: 106 profitable, 23 watch, 31 unprofitable |
| Mean profit by status | profitable ₹1,741; watch ₹425; unprofitable −₹1,471 |
| Highest-earning zone | central (≈ ₹146.7k), about twice any other zone |
| Idempotent re-run | identical rows and totals |
| No-data alert | raised 72 s after the last ingested event (threshold 60 s, checked every minute); resolved at the first check after the producer restarted |

**Interpretation:**
- The mix of statuses follows the vehicle profiles. High-cost vehicles (poor fuel efficiency) are mostly unprofitable despite normal demand (17 of 22 vehicle-days at the time of checking).
- Maintenance-heavy vehicles lose money on service days.
- Low-utilisation vehicles have low costs but also low, volatile earnings. Their days split between `profitable` and `watch` (9 / 9, plus 4 `unprofitable`), so they are the least predictable group.
- The operational action is to review the high-cost vehicles first.

## 19. Limitations
- Single-node Kafka, Spark local mode and single-container Airflow: no high availability.
- The data is simulated; the demand and cost models are plausible but not calibrated to real data.
- Distinct vehicle counts in the speed layer are approximate by design. They are exact for this fleet size, and the batch layer corrects them in any case.
- Events later than the 15-minute watermark are missing from the real-time windows. They are still stored and counted in the daily report.
- A trip that is in progress when the producer restarts is lost (it never gets a fare).
- Partitions are skewed with 20 keys (see section 6).
- A failed expense file blocks later dates until it is fixed. This is a deliberate choice to process days in order.
- A day counts as complete as soon as the newest ingested event belongs to the next day. If a streaming micro-batch failed part-way at exactly the day boundary, the day could be reconciled before its last events are stored. Normal operation is not affected. A production system would re-reconcile the previous day once, or require every vehicle to have reported on the next day.
- Scheduled runs only select dates whose file already exists, so the `wait_for_expense_file` sensor only really waits for manually triggered dates.
- Fare aggregation (`SUM(fare)`, `COUNT(fare > 0)`) is written in three places: streaming, batch and API summaries. It follows ADR-004, but a shared SQL view would remove the duplication. The Spark expense cleaning upper-cases vehicle ids, while the file validator (which runs first) rejects lowercase ids, so lowercase ids never reach Spark.
- Ports are bound to 127.0.0.1 and traffic is plaintext; `AIRFLOW_FERNET_KEY` is empty by default (set it in `.env`).
- Idle alerts are still frequent at night (low demand). The threshold is configurable (currently 240 simulated minutes); night-time suppression would be the next step.

## 20. Production considerations
- A Kafka cluster with replication factor 3 and `min.insync.replicas=2`, plus a schema registry (Avro or Protobuf) for contract evolution.
- A Spark cluster (or managed service) with checkpoints on durable object storage.
- Airflow with the Celery or Kubernetes executor, remote logging, and SLAs and alert callbacks.
- Secrets in a secret manager instead of `.env`. TLS and authentication on Kafka, PostgreSQL and the API.
- Raw events in a data lake (Parquet or Iceberg) as the batch source of truth, with PostgreSQL only as the serving store. Partition `stream_events` by date.
- Alert delivery (email or pager) and metrics dashboards (Prometheus and Grafana) on top of the existing tables and logs.
- CI running Ruff and the unit tests on every change, with integration tests against ephemeral containers.
