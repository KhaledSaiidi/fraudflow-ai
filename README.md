# FraudFlow AI

FraudFlow AI is a containerized fraud-detection platform that simulates end-to-end transaction ingestion, feature generation, and model-training orchestration.

It is built to demonstrate production-oriented data and ML engineering patterns with Kafka, Airflow, MinIO, PostgreSQL, Redis, and MLflow.

## Current Status

Implemented now:

- Kafka producer that generates synthetic transactions with fraud patterns
- Kafka ingestion pipeline with validation, partition-aware consumption, deduplication, and Parquet output
- Airflow DAG that orchestrates ingestion, dataset building, and training task execution
- Training dataset builder with feature engineering and MinIO persistence
- Full local infrastructure in Docker Compose (Airflow + Kafka + MinIO + MLflow)

Not implemented yet:

- Real model training logic in `dags/fraud_detection_training.py` (`train_model` is still a stub)
- Real-time inference consumer for `fraud_predictions`
- Monitoring dashboards and drift detection

## Repository Layout

```text
.
├── airflow/
├── config/
├── config.yaml
├── dags/
├── docker-compose.yaml
├── logs/
├── mlflow/
├── models/
├── plugins/
├── producer/
├── scripts/
├── .env
├── .env.example
└── README.md
```

## Architecture

```text
Transaction Producer (2 replicas)
        |
        v
Kafka Cluster (3 brokers, KRaft, 6 partitions, SASL_PLAINTEXT)
        |
        v
Airflow DAG: fraud_detection_training (daily)
  1) validate_environment
  2) ingest_transactions_0..2 (parallel)
  3) build_training_dataset
  4) execute_training (currently stubbed)
  5) cleanup
        |
        v
MinIO buckets
  - transactions     (raw deduplicated parquet batches)
  - fraud-features   (training dataset parquet)
  - mlflow           (MLflow artifacts)
        |
        v
MLflow Tracking Server
```

## Data Flow Details

### 1) Producer

`producer/main.py` continuously emits synthetic transactions and applies fraud simulation rules.

Fraud simulation patterns include:

- Account takeover behavior
- Card testing behavior
- Merchant collusion behavior
- Geographic anomaly behavior

Producer behavior also includes JSON schema validation and Kafka topic bootstrap/partition management.

### 2) Ingestion

`dags/ingestor.py` consumes from Kafka with worker-aware partition assignment.

Key ingestion settings from `config.yaml`:

- `worker_count: 3`
- `max_messages_per_batch: 1000`
- `max_wait_seconds: 5`
- `max_run_seconds: 1800`
- `dedup_shard_count: 3`

Data is validated, deduplicated, and written as Snappy-compressed Parquet under:

`transactions/event_date=YYYY-MM-DD/dedup_shard=N/...parquet`

### 3) Training Dataset Build

`dags/training_dataset.py` loads shard data for a configurable lookback window (`training_data.lookback_days`, default 30), deduplicates, creates features, and writes:

`fraud-features/v1/features-YYYY-MM-DD.parquet`

Implemented engineered features include:

- `hour_of_day`
- `day_of_week`
- `is_weekend`
- `log_amount`
- `is_card_testing`
- `is_large_amount`
- `is_very_large_amount`
- `is_high_risk_merchant`
- `is_suspicious_location`

The target label is `is_fraud`.

### 4) Model Training

`dags/fraud_detection_training.py` has infrastructure scaffolding (MinIO checks, MLflow setup) but `train_model` currently returns hardcoded values.

## Airflow DAG

`dags/fraud_detection_training_dag.py` defines a daily DAG (`0 4 * * *`) with:

- environment validation task
- three parallel ingestion tasks
- one dataset build task
- one training task
- cleanup task (`trigger_rule='all_done'`)

## Configuration

Non-secret settings live in `config.yaml`:

- Kafka connectivity and topic parameters
- MinIO endpoint and bucket names
- Ingestion worker/batch behavior
- Training dataset lookback/minimum rows
- Airflow DAG scheduling and retry policy
- MLflow tracking and registry names

Secrets and credentials live in `.env` (template: `.env.example`).

Required credential groups:

- MinIO/S3 credentials
- Airflow DB/admin/JWT/Fernet credentials
- MLflow DB credentials
- Kafka SASL credentials

## Quick Start

### 1) Prepare environment

```bash
cp .env.example .env
# Edit .env and set all required values
```

### 2) Start the full stack

```bash
docker-compose up -d
docker-compose ps
```

### 3) Open service UIs

- Airflow: `http://localhost:8080`
- MLflow: `http://localhost:5500`
- MinIO Console: `http://localhost:9001`
- Kafka UI: `http://localhost:8085`
- Flower (optional profile): `http://localhost:5555`

### 4) Trigger and monitor DAG

Use Airflow UI, or:

```bash
curl -X POST http://localhost:8080/api/v2/dags/fraud_detection_training/dagRuns \
  -u "$AIRFLOW_ADMIN_USERNAME:$AIRFLOW_ADMIN_PASSWORD" \
  -H "Content-Type: application/json" \
  -d '{"note":"manual run"}'
```

Check logs:

```bash
docker logs -f airflow-scheduler
docker logs -f airflow-worker
```

## Technology Stack

- Python (runtime for producer, DAG tasks, and training components)
- Apache Kafka 4.x in KRaft mode
- Apache Airflow 3.3 with CeleryExecutor
- Redis + PostgreSQL for Airflow backend and broker/results
- MinIO for object storage
- MLflow for experiment tracking/artifacts
- PyArrow/Pandas for dataset processing
- Docker Compose for local orchestration

## Roadmap

- Implement `train_model` end-to-end (load dataset, train, evaluate, log to MLflow, register model)
- Add inference service consuming `fraud_predictions`
- Add metrics/observability and drift detection
- Add automated retraining strategy

## Disclaimer

This project uses synthetic/sample-like data and is intended for learning and experimentation only. It is not suitable for real financial decision-making without substantial additional controls.
