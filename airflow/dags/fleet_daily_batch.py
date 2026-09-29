"""fleet_daily_batch: expense file -> validation -> Spark load -> Spark reconciliation -> alerts.

    resolve_business_date -> wait_for_expense_file -> validate_expenses
        -> load_expenses (spark-submit) -> reconcile (spark-submit) -> raise_profitability_alerts

Which business date?
- Manual trigger with conf {"business_date": "YYYY-MM-DD"}: that date (re-processing an
  already reconciled date is allowed and idempotent).
- Scheduled run (every 2 real minutes, ~0.4 simulated days): the OLDEST business date
  that is finished in the stream, has a landing file, and has no successful
  'reconciliation' row in pipeline_runs. If there is none, the run is skipped.
  One date per run (max_active_runs=1) keeps memory use predictable; a backlog is
  worked off one date every 2 minutes, faster than the stream produces days (5 min).

Failure handling:
- An invalid file fails validate_expenses with AirflowFailException (no retries: the
  same file would fail again). Its rule failures are in the task log, data_quality_stats
  and a 'failed' pipeline_runs row. Nothing is loaded.
- Spark tasks retry (transient errors, e.g. PostgreSQL restarting); every attempt is
  idempotent because each job replaces its business date in one transaction.

Memory: Spark runs in this container with local[1] and a 512 MB driver (docker-compose
gives Airflow 2 GB; webserver + scheduler use ~1 GB).
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

log = logging.getLogger(__name__)

PROJECT_DIR = os.getenv("PYTHONPATH", "/opt/project").split(":")[0]
JOBS_SCRIPT = f"{PROJECT_DIR}/fleet/batch/jobs.py"
DATE_XCOM = "{{ ti.xcom_pull(task_ids='resolve_business_date') }}"


def spark_submit(job: str) -> str:
    """Bash command for one Spark batch job (Jinja fills in the date and Airflow run id)."""
    return (
        "spark-submit --master local[1] --driver-memory 512m"
        " --conf spark.ui.enabled=false --conf spark.sql.shuffle.partitions=1"
        f" {JOBS_SCRIPT} {job} --business-date {DATE_XCOM}"
        " --airflow-run-id '{{ run_id }}'"
    )


def resolve_business_date(**context) -> str:
    """Return the business date for this run (pushed to XCom) or skip the run."""
    from datetime import date

    from fleet.batch.dates import completed_business_dates, pending_business_dates
    from fleet.common.config import load_settings
    from fleet.common.db import connect

    conf = (context["dag_run"].conf or {}) if context.get("dag_run") else {}
    if conf.get("business_date"):
        business_date = date.fromisoformat(str(conf["business_date"])).isoformat()
        log.info("business date from dag_run.conf: %s", business_date)
        return business_date

    settings = load_settings()
    with connect(settings) as conn:
        completed = completed_business_dates(conn)
        pending = pending_business_dates(conn, settings.landing_dir)
    log.info(
        "completed business dates: %d, pending (file present, not reconciled): %s",
        len(completed),
        [d.isoformat() for d in pending],
    )
    if not pending:
        raise AirflowSkipException("no completed business date waiting for reconciliation")
    business_date = pending[0].isoformat()
    log.info("processing oldest pending business date %s", business_date)
    return business_date


def expense_file_exists(business_date: str) -> bool:
    from fleet.common.config import load_settings
    from fleet.common.contracts import expense_file_path

    path = expense_file_path(load_settings().landing_dir, business_date)
    found = os.path.isfile(path)
    log.info("expense file %s: %s", path, "found" if found else "not yet")
    return found


def validate_expenses(business_date: str, **context) -> dict:
    from fleet.batch.runs import validate_and_record
    from fleet.batch.validation import ExpenseValidationError
    from fleet.common.config import load_settings

    try:
        result = validate_and_record(load_settings(), business_date, context["run_id"])
    except ExpenseValidationError as exc:
        # no retry: the same file would fail the same way
        log.error("%s", exc)
        raise AirflowFailException(str(exc)) from exc
    log.info("expense file valid: %s rows, file %s", result.records_total, result.file_path)
    return {"rows": result.records_total, "file": result.file_path}


def raise_profitability_alerts(business_date: str) -> dict:
    from fleet.alerts.evaluator import raise_low_profitability_alerts

    result = raise_low_profitability_alerts(business_date)
    log.info("low-profitability alerts: %s", result)
    return result


default_args = {
    "owner": "batch",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="fleet_daily_batch",
    description="Daily expenses + stream history -> vehicle profitability (Spark batch)",
    schedule="*/2 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=20),
    tags=["fleet", "batch"],
    doc_md=__doc__,
) as dag:
    resolve = PythonOperator(
        task_id="resolve_business_date",
        python_callable=resolve_business_date,
        retries=0,
    )
    wait_for_file = PythonSensor(
        task_id="wait_for_expense_file",
        python_callable=expense_file_exists,
        op_kwargs={"business_date": DATE_XCOM},
        mode="reschedule",  # frees the worker slot between pokes
        poke_interval=20,
        timeout=15 * 60,
        retries=0,
    )
    validate = PythonOperator(
        task_id="validate_expenses",
        python_callable=validate_expenses,
        op_kwargs={"business_date": DATE_XCOM},
    )
    load = BashOperator(
        task_id="load_expenses",
        bash_command=spark_submit("load"),
        execution_timeout=timedelta(minutes=10),
    )
    reconcile = BashOperator(
        task_id="reconcile",
        bash_command=spark_submit("reconcile"),
        execution_timeout=timedelta(minutes=10),
    )
    alerts = PythonOperator(
        task_id="raise_profitability_alerts",
        python_callable=raise_profitability_alerts,
        op_kwargs={"business_date": DATE_XCOM},
    )

    resolve >> wait_for_file >> validate >> load >> reconcile >> alerts
