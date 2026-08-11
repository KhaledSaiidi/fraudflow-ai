# syntax=docker/dockerfile:1

ARG AIRFLOW_VERSION=3.3.0
ARG PYTHON_VERSION=3.13

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG AIRFLOW_VERSION

LABEL org.opencontainers.image.title="FraudFlow Airflow"
LABEL org.opencontainers.image.description="FraudFlow orchestration runtime"

USER airflow

COPY --chown=airflow:0 requirements.txt /tmp/requirements.txt

RUN umask 0002 \
    && pip install --no-cache-dir \
        "apache-airflow==${AIRFLOW_VERSION}" \
        --constraint "${HOME}/constraints.txt" \
        --requirement /tmp/requirements.txt \
    && pip check \
    && rm /tmp/requirements.txt
