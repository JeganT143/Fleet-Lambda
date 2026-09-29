#!/usr/bin/env bash
# Runs once, on first start of an empty PostgreSQL volume, BEFORE 001_schema.sql
# (files in /docker-entrypoint-initdb.d run in alphabetical order).
# Creates the Airflow metadata database and its owner on the same server.
set -euo pipefail

: "${AIRFLOW_DB:?AIRFLOW_DB must be set}"
: "${AIRFLOW_DB_USER:?AIRFLOW_DB_USER must be set}"
: "${AIRFLOW_DB_PASSWORD:?AIRFLOW_DB_PASSWORD must be set}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    -v db="$AIRFLOW_DB" -v user="$AIRFLOW_DB_USER" -v pass="$AIRFLOW_DB_PASSWORD" <<'SQL'
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'user', :'pass')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'user') \gexec
SELECT format('CREATE DATABASE %I OWNER %I', :'db', :'user')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = :'db') \gexec
SQL

echo "000_create_airflow_db: database '$AIRFLOW_DB' owned by '$AIRFLOW_DB_USER' is ready"
