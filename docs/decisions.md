# Architecture Decision Records

Short records of decisions that shape the system. Each lists the context, the
decision, and its consequences, so it can be defended (or revisited) later.

---

## ADR-001 — Lambda architecture instead of Kappa

**Context.** Telemetry is a continuous stream; vehicle expenses arrive as a daily file
from a separate system. The business needs live utilisation *and* an auditable daily
profitability report.

**Decision.** Lambda: a speed layer (Kafka → Spark Structured Streaming) and a batch
layer (file → Airflow → Spark batch), both writing to a PostgreSQL serving layer.

**Consequences.** Two code paths to maintain. Mitigated by sharing constants and rules
through `fleet/common/contracts.py`, and by the two paths computing different things
(live windows vs daily reconciliation). The batch layer recomputes daily numbers from
the full stored event history, so speed-layer approximations never reach the report.

---

## ADR-002 — Single-node, resource-light infrastructure

**Context.** The target machine has about 7 GB RAM and other workloads.

**Decision.** One Kafka broker in KRaft mode (no ZooKeeper), Spark in `local[*]` mode
inside containers (no master/worker cluster), Airflow scheduler + webserver in one
container with the `LocalExecutor`, one PostgreSQL server hosting both the application
database and the Airflow metadata database. Every service has a `mem_limit`.

**Consequences.** Not highly available and not horizontally scaled. The code
(Kafka partitions, Spark DataFrames, idempotent writes) does not change if the
infrastructure is scaled out; only configuration does.

---

## ADR-003 — Simulated time owned by the producer

**Context.** One business day must pass in 5 real minutes, and all components must agree
on business dates.

**Decision.** Only the producer has a clock (`SimClock`, speed-up 288×). Event
timestamps carry simulated time (in UTC); `business_date` is the Sri Lankan calendar date
(Asia/Colombo) of the event timestamp (see ADR-013).
Other components derive simulated time from data, never from their own wall clock.
Ingestion (Kafka) and processing (Spark) timestamps stay in real time.

**Consequences.** No shared clock service is needed and replays produce identical
business dates. Real-time checks such as "no data received" use real ingestion time.

---

## ADR-004 — Fare reported once per trip

**Context.** Streaming aggregations cannot easily do "sum of max fare per trip".

**Decision.** The trip's full fare is carried only on the final `on_trip` event; all
other events have `fare = 0`.

**Consequences.** Earnings = `SUM(fare)`, trips = `COUNT(fare > 0)`,
average fare = `AVG(fare | fare > 0)` — simple and identical in streaming and batch.
A trip whose final event is lost is not counted, which the rejected-events table makes
visible.

---

## ADR-005 — Idle ratio and utilisation from event shares

**Context.** Utilisation must be defined mathematically.

**Decision.** Every vehicle reports at a fixed cadence, so the share of events in a
state equals the share of time in that state.

- `idle_ratio (window) = idle events / all events`
- `utilization_rate (vehicle, day) = on_trip events / all events of that vehicle`

**Consequences.** No session reconstruction is needed. The estimate degrades if events
are lost, which data-quality statistics make visible.

---

## ADR-006 — Idempotency through natural keys

**Decision.**
- `stream_events`: primary key `event_id`, insert `ON CONFLICT DO NOTHING` (safe replays after a streaming restart).
- `realtime_*_metrics`: upsert on window (and zone).
- `vehicle_expenses`, `daily_vehicle_profitability`: primary key `(vehicle_id, business_date)`; a batch run replaces the whole business date in one transaction.
- `alerts`: unique `dedup_key` so the same condition is never raised twice.

**Consequences.** Any job can be re-run safely; Airflow retries are harmless.

---

## ADR-007 — Shared vehicle profiles

**Context.** The telemetry simulator and the expense generator are independent
"source systems" but describe the same fleet.

**Decision.** `fleet/common/fleet_profiles.py` maps each vehicle id deterministically
to a profile (normal, low utilisation, high cost, maintenance heavy).

**Consequences.** The daily report shows a realistic mix of profitable, watch and
unprofitable vehicles, and the result can be explained from the profiles.

---

## ADR-008 — Batch writes through a staging-table swap

**Context.** Spark's JDBC writer commits per partition, so it cannot atomically
"delete the day, then insert the day".

**Decision.** Each batch job JDBC-writes to a per-run staging table
(`stg_<target>_<run_id>`). One psycopg transaction then deletes the date's rows, inserts
from staging and drops the staging table.

**Consequences.** Readers never see a half-written day, a crashed run leaves the target
unchanged, and re-runs are safe. It costs one extra table write per run.

---

## ADR-009 — Batch date selection from ingested data

**Decision.** Simulated "today" is the business date of the most recently *ingested* event.
A scheduled `fleet_daily_batch` run processes the oldest earlier date that has a landing file
and no successful reconciliation. `dag_run.conf` can force a specific date.

**Consequences.** Days are processed in order and catch up automatically after an
outage. Test rows (year 2099, old ingestion time) can never make a live day look complete.
An invalid file blocks later dates until it is fixed, which keeps the failure visible.

---

## ADR-010 — Vehicles with events but no expense row are excluded

**Decision.** They are left out of `daily_vehicle_profitability`, logged as a warning, and
counted as `missing_expense_row` in `data_quality_stats`.

**Consequences.** No fake profit is reported from assumed zero costs. The gap stays
visible in the data-quality statistics.

---

## ADR-011 — Speed-layer tuning for a small machine

**Context.** With `approx_count_distinct(rsd=0.01)` the streaming container sat at its
memory limit, and metrics micro-batches took about 5.4 s.

**Decision.** Use `rsd=0.02` (about 2.1 s per batch, and still exact for the fixed ids
V001–V050, as a unit test proves). Raise the idle-alert threshold from 120 to 240 simulated
minutes, because 120 raised an alert for most vehicles every night.

**Consequences.** Changing the precision changes the Spark state schema, so the metrics
checkpoints had to be reset. Metrics were recomputed from Kafka and no events were lost.

---

## ADR-012 — Streamlit dashboard on top of the API

**Context.** The project needs a simple UI that demonstrates every feature clearly.

**Decision.** A Streamlit app (`fleet/dashboard/`) that talks only to the FastAPI serving
layer, plus the Airflow REST API (basic auth with the admin user) for DAG run states and
re-running a date. Five read-only endpoints were added to the API for the dashboard's
charts and operations views. The dashboard adds no business logic.

**Consequences.** One more container (≈ 384 MB limit). The UI stays a thin presentation
layer: if it were replaced, nothing else would change. Airflow's REST API now accepts basic
auth, which is acceptable for a local demo; production would use a service account and TLS.

---

## ADR-013 — Sri Lankan locale (Colombo, LKR, Asia/Colombo business day)

**Context.** The project simulates a Sri Lankan ride-hailing fleet end to end.

**Decision.**
- City grid over Colombo with neighbourhood labels (`fleet/common/zones.py`).
- All money in LKR: fares `160 + 48/km + 6/min` (minimum 320), fuel ≈ LKR 325/litre,
  routine maintenance LKR 500–1,100, workshop visits LKR 8,000–19,000, "profitable" from
  LKR 2,500 profit. Every value is the earlier model scaled by the same factor (×3.2), so the
  profit mix stays realistic. The tariff is illustrative, not a real operator's price list.
- Business date = Sri Lankan calendar date (`BUSINESS_TIMEZONE = "Asia/Colombo"` in
  `contracts.py`). Timestamps stay UTC on the wire and in the database; only the date and
  the display are local. Hourly windows are aligned to Colombo clock hours
  (`transforms.window_start_time`).
- Number plates (WP CAx-1234), car models and driver names are display-only attributes
  derived from the vehicle id (`fleet_profiles.py`); the pipeline keys stay `V###`/`D###`.

**Consequences.** Data produced before this change (INR, UTC business dates) is not
comparable, so the stack must be reset (`make reset`) once. Sri Lanka has no daylight saving,
so the fixed 30-minute window shift is exact.
