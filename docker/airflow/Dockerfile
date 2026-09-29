# Airflow 2.10.5 + Java 17 + the same pyspark/jars as fleet-runtime.
# Build context: repository root.
ARG AIRFLOW_VERSION=2.10.5
FROM apache/airflow:${AIRFLOW_VERSION}-python3.11

ARG AIRFLOW_VERSION=2.10.5
ARG PYSPARK_VERSION=3.5.4
ARG POSTGRES_JDBC_VERSION=42.7.13

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /usr/lib/jvm/java-17
ENV JAVA_HOME=/usr/lib/jvm/java-17

COPY --chmod=0755 docker/airflow/start.sh /opt/fleet/start.sh

USER airflow
# Pinning apache-airflow and using the official constraints keeps Airflow's own
# dependency set intact. pyspark 3.5.4 is the version those constraints pin.
# psycopg (v3) is not in the constraints file; it is used by fleet/common/db.py.
RUN pip install --no-cache-dir \
        "apache-airflow==${AIRFLOW_VERSION}" \
        "pyspark==${PYSPARK_VERSION}" \
        "psycopg[binary]>=3.2,<4" \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt" \
    && pip show pyspark | grep -E '^(Name|Version):'

COPY --chown=airflow:root docker/spark/install_spark_jars.py /tmp/install_spark_jars.py
RUN POSTGRES_JDBC_VERSION="${POSTGRES_JDBC_VERSION}" python /tmp/install_spark_jars.py

# Project code (fleet/) is bind-mounted here; DAGs import it via PYTHONPATH.
ENV PYTHONPATH=/opt/project
# The image entrypoint runs "bash ..." commands as-is (anything else is passed to `airflow`)
CMD ["bash", "/opt/fleet/start.sh"]
