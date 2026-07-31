FROM python:3.11-slim

LABEL maintainer="FraudFlow AI"
LABEL description="MLflow server for FraudFlow experiment tracking and model registry"

WORKDIR /app

# Create non-root user for security
RUN groupadd -r mlflow && useradd -r -g mlflow mlflow && \
    apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

COPY --chown=mlflow:mlflow requirements.txt /app/

RUN pip install --no-cache-dir -r /app/requirements.txt && \
    rm /app/requirements.txt

# Switch to non-root user
USER mlflow

EXPOSE 5500

HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:5500 || exit 1

ENTRYPOINT ["mlflow"]
CMD ["server", "--port", "5500", "--host", "0.0.0.0"]