FROM apache/airflow:3.3.0

LABEL maintainer="FraudFlow AI"
LABEL description="Apache Airflow with custom dependencies for FraudFlow"

USER root
RUN mkdir -p /app/models && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        build-essential \
        libssl-dev \
        libffi-dev \
        python3-dev \
        gcc \
        g++ \
        git && \
    rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt && \
    rm /tmp/requirements.txt

HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8080/api/v2/monitor/health || exit 1