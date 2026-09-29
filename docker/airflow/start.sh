#!/usr/bin/env bash
# Single-container Airflow: metadata migration, admin user, scheduler + webserver.
# If either process exits, the container exits so Docker restarts it.
set -euo pipefail

: "${AIRFLOW_ADMIN_USER:?AIRFLOW_ADMIN_USER must be set}"
: "${AIRFLOW_ADMIN_PASSWORD:?AIRFLOW_ADMIN_PASSWORD must be set}"

airflow db migrate

if airflow users list --output plain 2>/dev/null | awk '{print $2}' | grep -qx "${AIRFLOW_ADMIN_USER}"; then
    echo "admin user '${AIRFLOW_ADMIN_USER}' already exists"
else
    airflow users create \
        --username "${AIRFLOW_ADMIN_USER}" \
        --password "${AIRFLOW_ADMIN_PASSWORD}" \
        --firstname Fleet --lastname Admin \
        --role Admin --email admin@example.com
fi

airflow scheduler &
airflow webserver &

wait -n
echo "an Airflow process exited; stopping container" >&2
exit 1
