# fleet-runtime: Python 3.11 + Java 17 + pyspark with Kafka/JDBC jars baked in.
# Used by producer, spark-streaming, api, expense-generator and runner.
# Build context: repository root.
FROM python:3.11-slim-bookworm

ARG PYSPARK_VERSION=3.5.4
ARG POSTGRES_JDBC_VERSION=42.7.13
ARG APP_UID=1000

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Java for Spark (procps: Spark's launcher scripts call `ps`)
RUN mkdir -p /usr/share/man/man1 \
    && apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /usr/lib/jvm/java-17
ENV JAVA_HOME=/usr/lib/jvm/java-17

WORKDIR /app

# Dependencies only (layer cached until pyproject changes). The project itself
# is uninstalled again: code is used from /app via PYTHONPATH (bind-mounted in compose).
COPY pyproject.toml README.md ./
RUN mkdir fleet && touch fleet/__init__.py \
    && pip install ".[producer,api,spark,dev]" "pyspark==${PYSPARK_VERSION}" \
    && pip uninstall -y fleet-lambda \
    && rm -rf fleet build ./*.egg-info \
    && pip show pyspark | grep -E '^(Name|Version):'

COPY docker/spark/install_spark_jars.py /tmp/install_spark_jars.py
RUN POSTGRES_JDBC_VERSION="${POSTGRES_JDBC_VERSION}" python /tmp/install_spark_jars.py \
    && rm /tmp/install_spark_jars.py

RUN useradd --uid "${APP_UID}" --create-home --shell /bin/bash fleet \
    && mkdir -p /checkpoints /data/landing \
    && chown fleet /checkpoints /data/landing /app

COPY --chown=fleet fleet ./fleet
COPY --chown=fleet docker/smoke ./docker/smoke

ENV PYTHONPATH=/app
USER fleet
CMD ["python", "-c", "import pyspark, fleet; print('fleet-runtime', pyspark.__version__)"]
