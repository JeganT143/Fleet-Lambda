# Fleet Lambda - 10-Minute Recorded Demo Script

## Purpose

This script is for a two-student video demonstration of the Fleet Lambda project.
The target duration is approximately 9 to 10 minutes:

| Part | Speaker | Duration |
|---|---|---:|
| Introduction and architecture | Student 1 | 2:30 |
| Live project demonstration | Student 2 | 6:00 |
| Conclusion | Both students | 1:00 |

The project is a simulated ride-hailing fleet platform for Colombo, Sri Lanka.
It answers this business question:

> Which vehicles are active, idle, under-utilised, or unprofitable, and where is the fleet earning money?

All demo data is simulated. One simulated business day lasts five real minutes.

---

## Before Recording

Complete these steps before starting the recording. The goal is to have data already
available so the demonstration is continuous.

```powershell
cd D:\Fleet-Lambda
docker info
docker compose --profile app --profile tools build
docker compose up -d --wait
docker compose --profile app up -d
docker compose --profile app ps
```

Wait until the dashboard, API, producer, Spark streaming, Airflow, and PostgreSQL
services are running. Start the recording after at least one simulated day has
completed, normally 6 to 8 minutes after `up -d`.

Open these browser tabs before recording:

1. Dashboard: <http://localhost:8501>
2. API documentation: <http://localhost:8000/docs>
3. Airflow: <http://localhost:8080>

Use the Airflow username and password from `.env`.

Recommended pages and data to prepare:

- Dashboard `Overview` page
- Dashboard `Live Fleet` page
- Dashboard `Daily Report` page
- Dashboard `Alerts` page
- Airflow DAG `fleet_daily_batch`
- API endpoint `/api/v1/reports/daily/2026-01-01`

If the daily report is not available yet, keep the application running until the
first report for `2026-01-01` appears.

---

# Part 1 - Introduction

## Student 1 - About 2 minutes 30 seconds

### 0:00-0:30 - Opening

**Show:** The generated architecture image as the first slide or background visual.

**Say:**

"Hello everyone. Our project is called Fleet Lambda. It is an end-to-end data
engineering platform for a simulated ride-hailing fleet operating in Colombo,
Sri Lanka.

The system continuously receives vehicle telemetry and also receives daily vehicle
expense files. It combines both types of data to answer an operational question:
which vehicles are being used efficiently, which vehicles are idle or under-utilised,
and which vehicles are not profitable after fuel and maintenance costs?"

### 0:30-1:25 - Explain the architecture image

**Point to the left side of the image and say:**

"The project has two data paths. The first is the speed layer. A Python simulator
generates vehicle events and sends them to Apache Kafka. Spark Structured Streaming
reads those events, validates them, enriches them with geographic zones, and creates
real-time fleet and zone metrics."

**Point to the lower or second path and say:**

"The second is the batch layer. At the end of each simulated business day, a Python
expense generator creates a CSV file. Apache Airflow detects and validates that file,
then starts Spark batch processing. The batch process combines the expense data with
the complete streaming history for that day."

**Point to PostgreSQL and the right side and say:**

"Both paths write to PostgreSQL. PostgreSQL is the serving layer for the system.
FastAPI exposes the stored results as REST endpoints, and the Streamlit dashboard
uses the API to display live fleet status, daily reports, vehicle profitability,
alerts, and data quality information."

### 1:25-2:10 - Explain why Lambda Architecture is used

**Say:**

"This is called Lambda Architecture because it has separate streaming and batch
layers. The streaming layer gives us fast operational information, such as active
vehicles, idle ratio, trips, and earnings by zone.

The batch layer gives us a more complete and auditable daily result. It uses the full
event history and the daily expense file, so it can calculate fuel cost, maintenance
cost, utilization, and estimated profit accurately. This is useful because a real-time
window can contain late events or approximate calculations, while the daily report
can recompute the complete day."

### 2:10-2:30 - Transition

**Say:**

"The main technologies are Python, Kafka, Spark Structured Streaming, Spark Batch,
Airflow, PostgreSQL, FastAPI, Streamlit, Docker Compose, pytest, and Ruff. Student 2
will now show the running system and the complete flow from live events to business
reports."

---

# Part 2 - Live Demonstration

## Student 2 - About 6 minutes

### 2:30-3:10 - Show the running system

**Show:** Dashboard `Overview` page.

**Say:**

"This is the Fleet Lambda dashboard. The Overview page shows the health of the main
services. PostgreSQL is the database, Airflow is the workflow scheduler, and the
stream status shows whether events are currently moving through Kafka and Spark."

"The system is running inside Docker Compose, so Kafka, Spark, Airflow, PostgreSQL,
the API, and the dashboard can be started as one local environment."

**Point to:** API, PostgreSQL, Airflow, stream status, simulated time, and number of
reconciled business days.

### 3:10-4:00 - Demonstrate the speed layer

**Show:** Dashboard `Live Fleet` page.

**Say:**

"This is the speed layer. The producer continuously generates telemetry for the
simulated vehicles. Kafka transports the JSON events, and Spark Structured Streaming
processes them in real time."

"The dashboard shows the latest one-hour simulated event-time window, including the
number of vehicles reporting, active vehicles, idle vehicles, idle ratio, trips per
hour, events per hour, and earnings."

**Point to:**

- Active vehicles
- Idle vehicles
- Idle ratio
- Trips per hour
- Earnings
- Zone metrics or charts

**Say:**

"The timestamps are simulated timestamps. One simulated day lasts five real minutes,
which allows us to demonstrate daily processing quickly."

### 4:00-4:35 - Show Kafka and data quality

**Show:** Dashboard `Pipeline & Quality` page or a terminal with producer logs.

Optional PowerShell command:

```powershell
docker compose --profile app logs --tail=20 producer
```

**Say:**

"The producer uses vehicle_id as the Kafka message key, so events from the same
vehicle stay in the same Kafka partition. The Kafka topic has three partitions."

"The producer also injects a small number of invalid events deliberately. Spark
does not stop when it sees them. Invalid records are placed in a rejected-events
quarantine table, while valid events continue through the pipeline. This demonstrates
basic data quality and fault-handling behavior."

### 4:35-5:25 - Demonstrate Airflow and the batch layer

**Show:** Airflow UI, DAG `fleet_daily_batch`.

**Say:**

"After a simulated day completes, the expense generator creates a daily CSV file.
Airflow detects that file and runs the batch workflow."

"This DAG resolves the business date, waits for the expense file, validates the
file, loads the expenses, runs Spark reconciliation, and raises profitability alerts."

**Point to:** The successful DAG run and its tasks.

"Airflow is useful here because it schedules the workflow, records task status,
provides logs, and can retry or rerun a failed task."

### 5:25-6:20 - Show the daily profitability report

**Show:** Dashboard `Daily Report` page.

**Say:**

"This is the batch result for the completed business day. The report combines the
full telemetry history with fuel and maintenance expenses for each vehicle."

"The estimated profit is calculated as earnings minus fuel cost minus maintenance
cost. The report also shows trips, distance, utilization, service status, and the
profitability category."

**Point to:** A few different vehicles and their statuses.

"The result contains profitable, watch, and unprofitable vehicles. This allows an
operator to identify vehicles that need attention rather than looking only at live
event counts."

Optional API demonstration:

```text
http://localhost:8000/api/v1/reports/daily/2026-01-01
```

**Say:**

"The same information is available through the FastAPI serving layer as structured
JSON. The dashboard reads this API instead of directly implementing the business
logic."

### 6:20-7:15 - Demonstrate alerts

**Show:** Dashboard `Alerts` page.

**Say:**

"The system also creates database-backed alerts. Examples include no streaming data,
a vehicle remaining idle for too long, and low daily profitability."

"These alerts are generated by the Airflow alert workflow and the batch workflow.
They are stored in PostgreSQL and exposed through the API, so the dashboard can show
open and resolved alerts."

If an alert is already visible, point to its type, severity, vehicle, message, and
status.

### 7:15-8:30 - Show reliability and idempotency

**Show:** Dashboard `Pipeline & Quality` page or Airflow run history.

**Say:**

"The project also includes reliability controls. Streaming data is checkpointed, so
Spark can resume after a restart. Database keys prevent duplicate events and duplicate
vehicle-day records."

"The daily batch is idempotent. If we run the same business date again, the report
rows for that date are replaced consistently instead of being duplicated. This is
important in production because scheduled workflows may be retried or manually
re-run."

Point to pipeline runs, quality statistics, or the rerun control if visible.

### 8:30-8:45 - Transition to conclusion

**Say:**

"We have now followed one complete flow: simulated telemetry moved through Kafka and
Spark for real-time monitoring, then daily expenses were reconciled through Airflow
and Spark batch to produce profitability and alert information."

---

# Part 3 - Conclusion

## Both Students - About 1 minute

### Student 1

"To summarize, the speed layer provides near real-time fleet visibility, while the
batch layer provides a complete daily financial and operational view."

### Student 2

"The final serving layer makes these results available through FastAPI and the
Streamlit dashboard. The project also demonstrates validation, rejected records,
checkpointing, idempotent processing, alerts, and workflow monitoring."

### Student 1

"This architecture is useful for fleet operators because it connects live operations
with daily business decisions. Thank you."

---

# Image Generation Prompt for the Introduction

Copy the following prompt into an image-generation tool. Generate a clean, professional
16:9 architecture illustration for a university data-engineering presentation.

```text
Create a clean professional 16:9 isometric data engineering architecture illustration
for a project called "Fleet Lambda: Ride-Hailing Fleet Operations".

The setting is a modern Colombo, Sri Lanka ride-hailing fleet with small city roads,
20 connected vehicles, subtle location pins, and a warm daytime city atmosphere.
The image must communicate a Lambda Architecture with two clearly separated horizontal
data paths that converge into one serving layer.

Top path, labeled "SPEED LAYER":
Python telemetry simulator -> Apache Kafka, 3 partitions -> Spark Structured Streaming
-> PostgreSQL.
Show small JSON vehicle telemetry events flowing along this path. Include labels for
"live vehicle events", "validation", "zone enrichment", and "real-time windows".

Bottom path, labeled "BATCH LAYER":
Daily expense CSV -> Apache Airflow -> Spark Batch -> PostgreSQL.
Show a CSV file, a scheduler clock, and batch records flowing along this path. Include
labels for "daily expenses", "validation", "reconciliation", and "profitability".

On the right side, show the shared PostgreSQL serving layer containing four simple
labeled data areas: "stream history", "real-time metrics", "daily profitability",
and "alerts". From PostgreSQL, draw arrows to "FastAPI" and then to a "Streamlit
Fleet Dashboard" containing small visual hints of active vehicles, idle ratio, zones,
profitability, and alerts.

Use a restrained professional palette: teal and blue for streaming, green for batch,
warm amber for PostgreSQL and business results, and neutral gray for infrastructure.
Use high contrast, readable English labels, simple flat icons, consistent arrows, and
clear spacing. Make the two paths visually distinct but show that they converge in the
same PostgreSQL serving layer.

Do not use a photorealistic style, excessive decoration, fantasy elements, random
unreadable text, extra technologies, Kubernetes, cloud provider logos, ZooKeeper, or
any brand logos. Do not add a title paragraph outside the diagram. The result should
look like a polished architecture slide that students can explain in under three
minutes.
```

## Image Presentation Notes

When Student 1 explains the image, reveal it from left to right:

1. Start with the simulated vehicles and telemetry source.
2. Explain the speed layer and Kafka.
3. Explain Spark Structured Streaming and real-time metrics.
4. Explain the daily expense file and Airflow batch layer.
5. Explain PostgreSQL as the shared serving layer.
6. Finish with FastAPI and the dashboard.

The image is a visual aid. The actual project implementation remains the source of
truth for the technologies and data flow.

---

# Backup Demonstration Commands

Use these commands only if a dashboard page does not load during the recording.

```powershell
# Check service status
docker compose --profile app ps

# Check API health
Invoke-WebRequest http://localhost:8000/health

# Show real-time summary
Invoke-RestMethod http://localhost:8000/api/v1/fleet/summary | ConvertTo-Json -Depth 5

# Show zone metrics
Invoke-RestMethod http://localhost:8000/api/v1/fleet/zones | ConvertTo-Json -Depth 5

# Show the daily report
Invoke-RestMethod http://localhost:8000/api/v1/reports/daily/2026-01-01 | ConvertTo-Json -Depth 5

# Show recent logs
docker compose --profile app logs --tail=30 producer
docker compose --profile app logs --tail=30 spark-streaming
```

Do not spend recording time building images or waiting for the first business day.
Prepare the environment first and record the dashboard results when they are already
available.