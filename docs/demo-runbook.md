# Demo Runbook (about 15 minutes)

A reproducible, end-to-end demonstration from a clean environment. Each step says what
to show and what the audience should see.

## 0. Clean start (≈ 5 min, the first build takes longer)

```bash
make env            # creates .env if missing (edit passwords)
make reset          # removes this project's containers, volumes and generated landing files
make build
make up             # postgres, kafka, airflow -> healthy
make up-app         # producer, spark-streaming, api, expense-generator
make ps
```
**Expect:** all services Up. postgres, kafka, airflow and api are `(healthy)`.

## 1. Kafka: partitions and keys

```bash
docker compose exec -T -e KAFKA_HEAP_OPTS=-Xmx128m kafka \
  kafka-topics --bootstrap-server kafka:9092 --describe --topic vehicle.telemetry
docker compose exec -T -e KAFKA_HEAP_OPTS=-Xmx128m kafka \
  kafka-console-consumer --bootstrap-server kafka:9092 --topic vehicle.telemetry \
  --property print.key=true --property print.partition=true --max-messages 5
make logs S=producer      # JSON stats lines: sent / failed counts
```
**Expect:** `PartitionCount: 3`. Each message has a `V###` key, and a vehicle always lands in the same partition.

## 2. Speed layer: live metrics (Spark Structured Streaming)

```bash
make psql
```
```sql
SELECT window_start, vehicles_reporting, active_vehicles, idle_ratio,
       trips_completed, total_earnings
FROM realtime_vehicle_metrics ORDER BY window_start DESC LIMIT 5;
-- run again after ~30 s: new windows appear, the newest one's numbers change
SELECT reason, count(*) FROM rejected_events GROUP BY 1;   -- quarantined records
```
```bash
curl -s localhost:8000/api/v1/fleet/summary | python3 -m json.tool
curl -s localhost:8000/api/v1/fleet/zones   | python3 -m json.tool
```
**Expect:**
- A new 1-hour (simulated) window about every 12 real seconds.
- A few rejected records. The producer injects about 0.5% invalid records on purpose, and the stream keeps running.

## 3. Batch layer: daily file → Airflow → Spark → PostgreSQL

After the first simulated day completes (≈ 5 min after start):

```bash
ls data/landing/expenses/                     # one folder per completed business date
head -3 data/landing/expenses/2026-01-01/vehicle_expenses.csv
```
Open the **Airflow UI** at http://localhost:8080 and go to `fleet_daily_batch`. Show a green run with its six tasks, and the log of `reconcile`.

```sql
SELECT pipeline_name, business_date, status, rows_read, rows_written
FROM pipeline_runs ORDER BY run_id DESC LIMIT 6;
```

## 4. Reconciliation and profitability

```bash
curl -s localhost:8000/api/v1/reports/daily/2026-01-01 | python3 -m json.tool | head -40
curl -s localhost:8000/api/v1/vehicles/V009 | python3 -m json.tool | head -40
```
**Expect:**
- A mix of `profitable`, `watch` and `unprofitable` vehicles.
- The low-utilisation vehicles (V003, V013) sit near the bottom on utilisation.
- The high-cost ones (V006, V016) are unprofitable.
- The maintenance-heavy ones (V009, V019) lose money on service days.

## 5. Idempotency

```bash
docker compose exec airflow airflow dags trigger fleet_daily_batch --conf '{"business_date": "2026-01-01"}'
```
```sql
SELECT count(*), sum(estimated_profit) FROM daily_vehicle_profitability WHERE business_date = '2026-01-01';
```
**Expect:** the same count (20) and sum before and after, plus one more `success` row in `pipeline_runs`.

## 6. Data quality: an invalid batch file fails clearly

```bash
docker compose exec expense-generator python -m fleet.batch.generator --business-date 2099-01-05 --corrupt
docker compose exec airflow airflow dags trigger fleet_daily_batch --conf '{"business_date": "2099-01-05"}'
```
**Expect:**
- `validate_expenses` is red and the downstream tasks are `upstream_failed`.
- The log lists the rule failures (duplicate id, negative value, bad date, …).
- `pipeline_runs` has a `failed` row, and no rows are loaded.

Clean up afterwards:
```bash
rm -r data/landing/expenses/2099-01-05
```
```sql
DELETE FROM data_quality_stats WHERE business_date = '2099-01-05';
DELETE FROM pipeline_runs WHERE business_date = '2099-01-05';
```

## 7. Alerts

```bash
curl -s 'localhost:8000/api/v1/alerts?status=open&limit=10' | python3 -m json.tool
docker compose stop producer        # wait ~90 s
curl -s 'localhost:8000/api/v1/alerts?alert_type=no_stream_data' | python3 -m json.tool
docker compose --profile app up -d producer   # alert auto-resolves within ~1 min
```
**Expect:**
- `low_profitability` alerts for the unprofitable vehicles.
- A `no_stream_data` alert (critical) while the producer is stopped, which becomes `resolved` once events flow again.

## 8. Tests and code quality

```bash
make test     # full suite inside the runner container
make lint
```
**Expect:** all tests pass and Ruff is clean.
