"""fleet_stream_alerts: every minute, check the speed layer for alert conditions.

    check_no_stream_data   no event ingested for ALERT_NO_DATA_SECONDS (real time);
                           auto-resolved once events arrive again
    check_idle_vehicles    vehicles idle for >= ALERT_IDLE_MINUTES (simulated time),
                           measured against the latest fleet event time;
                           auto-resolved when the vehicle moves again

Both tasks are plain SQL through psycopg (no Spark) and are idempotent thanks to the
alerts.dedup_key unique constraint. Results are written to the task logs.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)


def check_no_stream_data() -> dict:
    from fleet.alerts.evaluator import check_no_stream_data as check

    result = check()
    log.info("no-data check: %s", result)
    return result


def check_idle_vehicles() -> dict:
    from fleet.alerts.evaluator import check_idle_vehicles as check

    result = check()
    log.info("idle check: %s", result)
    return result


with DAG(
    dag_id="fleet_stream_alerts",
    description="No-data and idle-vehicle alerts on the streaming data",
    schedule="* * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "alerts", "retries": 1, "retry_delay": timedelta(seconds=15)},
    dagrun_timeout=timedelta(minutes=5),
    tags=["fleet", "alerts"],
    doc_md=__doc__,
) as dag:
    PythonOperator(task_id="check_no_stream_data", python_callable=check_no_stream_data)
    PythonOperator(task_id="check_idle_vehicles", python_callable=check_idle_vehicles)
