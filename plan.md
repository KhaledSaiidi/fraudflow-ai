# FraudFlow AI Continuation Plan

This project already has a useful foundation from Part 1:

- Local infrastructure with Docker Compose.
- A 3-node Kafka cluster with Kafka UI.
- A Python transaction producer that generates synthetic fraud labels.
- Airflow with CeleryExecutor, Postgres, and Redis.
- MLflow backed by Postgres and MinIO.
- A first Airflow DAG skeleton for model training.

The missing "Part 2" is the path from infrastructure to a complete real-time fraud detection system. The best next step is to make the current foundation stable, then add batch training, model registration, streaming inference, monitoring, and finally production-style hardening.

## Target Architecture

```text
Synthetic Transaction Producer
        |
        v
Kafka topic: transactions
        |
        +-----------------------------+
        |                             |
        v                             v
Offline training data sink       Real-time inference service
        |                             |
        v                             v
Airflow training DAG             Kafka topic: fraud_predictions
        |                             |
        v                             v
MLflow experiment + model        Dashboard / alerts / storage
registry
```

## Phase 0: Stabilize The Current Project

Goal: make sure the existing stack starts reliably before adding new services.

Tasks:

- Add or document the required `src/.env` variables.
- Fix typos and config drift in `src/config.yaml`.
- Confirm Kafka brokers start and Kafka UI can connect.
- Confirm the producer creates the `transactions` topic and publishes events.
- Confirm Airflow can load the `fraud_detection_training` DAG.
- Confirm MLflow starts and can write artifacts to MinIO.

Known issues to check first:

- `src/config.yaml` has `bnootstrap_servers`; this should be `bootstrap_servers`.
- `src/dags/fraud_detection_training.py` imports `cadwyn.endpoint`, which appears unrelated and likely unnecessary.
- `FraudDetectionTraining` validates Kafka username/password from environment, but local Docker config may rely on `.env`; make sure these match.
- The training class initializes config, MLflow, and MinIO, but does not actually load data, train a model, evaluate it, or register it.
- The producer fraud-generation logic is currently nested under the compromised-user branch, so many normal transactions may return `None`. Review the indentation before relying on the generated data.

Acceptance checks:

```bash
cd src
docker compose config
docker compose up -d
docker compose ps
```

Open:

- Kafka UI: `http://localhost:8085`
- Airflow: `http://localhost:8080`
- MLflow: `http://localhost:5500`
- MinIO: `http://localhost:9001`

## Phase 1: Create A Training Dataset

Goal: persist enough transaction history to train and evaluate a supervised fraud model.

Recommended approach for learning:

- Add a Kafka consumer or Spark batch job that reads from the `transactions` topic.
- Save transactions to local Parquet files under a mounted data volume, for example `src/data/transactions`.
- Partition the data by date or ingestion hour.
- Keep the original `is_fraud` label for supervised training.

Minimum fields to keep:

- `transaction_id`
- `user_id`
- `amount`
- `currency`
- `merchant`
- `timestamp`
- `location`
- `is_fraud`

Useful derived fields:

- Hour of day.
- Day of week.
- Amount bucket.
- Merchant frequency.
- User transaction count.
- User average amount.
- Time since user's previous transaction.
- Count of transactions per user in the last 5, 15, and 60 minutes.

Acceptance checks:

- You can produce at least 50,000 transactions.
- You can load the saved data with pandas or Spark.
- Fraud rate is visible and reasonable, ideally around 1-3 percent for this synthetic project.
- Duplicate `transaction_id` handling is defined.

## Phase 2: Build The First Real Training Pipeline

Goal: make the Airflow DAG train a real model and log results to MLflow.

Recommended first model:

- Start with `RandomForestClassifier`, `LogisticRegression`, or `XGBoost`.
- Optimize for fraud detection metrics, not plain accuracy.
- Track precision, recall, F1, ROC AUC, and PR AUC.
- Pay special attention to recall and precision because fraud data is imbalanced.

Training pipeline tasks:

- Load historical transactions from Parquet.
- Validate schema and required columns.
- Split train/test by time, not random split.
- Build preprocessing for categorical fields like `merchant`, `currency`, and `location`.
- Handle class imbalance with class weights or resampling.
- Train a baseline model.
- Log parameters, metrics, confusion matrix, and model artifact to MLflow.
- Register the best model as `fraud_detection_xgboost` or a similarly named model.

Acceptance checks:

- Airflow DAG completes successfully.
- MLflow shows a run with metrics and artifacts.
- A registered model version exists.
- The model can be loaded back and used for prediction.

## Phase 3: Add Real-Time Inference

Goal: consume live transactions, score them with the registered model, and publish fraud predictions.

Create a new service, for example:

```text
src/inference/
  Dockerfile
  main.py
  requirements.txt
```

Responsibilities:

- Connect to Kafka.
- Consume from `transactions`.
- Load the current production model from MLflow.
- Apply the same feature engineering used during training.
- Produce predictions to `fraud_predictions`.
- Include fields such as:
  - `transaction_id`
  - `fraud_probability`
  - `prediction`
  - `model_name`
  - `model_version`
  - `scored_at`

Important design rule:

- Training and inference must share feature logic. Do not duplicate slightly different feature code in two places.

Acceptance checks:

- New Kafka topic `fraud_predictions` receives messages.
- Every prediction references the source `transaction_id`.
- The inference service survives bad messages without crashing.
- The model version is visible in each prediction.

## Phase 4: Add Stream Processing With Spark

Goal: use Spark Structured Streaming for realistic streaming feature engineering.

Options:

- Simple path: keep Python inference service and use Spark only for aggregation features.
- More advanced path: make Spark read `transactions`, engineer features, load the MLflow model, and write predictions.

Recommended learning path:

1. Add Spark master and worker services to Docker Compose.
2. Create `src/spark/streaming_job.py`.
3. Read from Kafka using Spark Structured Streaming.
4. Parse JSON transaction events with an explicit schema.
5. Add watermarking and windowed features.
6. Write enriched events or predictions to Kafka.

Acceptance checks:

- Spark job reads from Kafka continuously.
- Late events are handled with a defined watermark.
- Aggregated features are reproducible between training and inference.

## Phase 5: Store Predictions And Build Visibility

Goal: make predictions inspectable after they are emitted.

Storage options:

- Postgres table for predictions.
- Parquet files for analytics.
- Elasticsearch/OpenSearch if you want search and dashboarding later.

Recommended tables:

- `transactions`
- `fraud_predictions`
- `model_versions`
- `prediction_feedback`

Dashboard ideas:

- Transactions per minute.
- Fraud rate over time.
- Top risky merchants.
- Top risky locations.
- Model version currently serving.
- Prediction latency.
- Kafka consumer lag.

Acceptance checks:

- Predictions are queryable after services restart.
- You can trace a prediction back to the original transaction.
- You can inspect fraud rate trends over time.

## Phase 6: Monitoring And Model Quality

Goal: detect when the system or model is getting worse.

System metrics:

- Producer throughput.
- Consumer lag.
- Inference latency.
- Failed message count.
- Kafka topic sizes.

Model metrics:

- Prediction distribution.
- Fraud probability distribution.
- Feature drift.
- Label drift, if feedback labels are available.
- Precision and recall on delayed ground truth.

Recommended tools:

- Prometheus and Grafana for infrastructure metrics.
- Evidently or custom Python reports for drift checks.
- Airflow scheduled DAG for daily model-quality reports.

Acceptance checks:

- You can see whether the inference service is alive.
- You can detect if Kafka lag is growing.
- You can compare today's transaction distribution against training data.

## Phase 7: Feedback Loop And Retraining

Goal: simulate how real fraud systems improve over time.

Tasks:

- Add a fake feedback generator that confirms or rejects fraud predictions after a delay.
- Store feedback labels in `prediction_feedback`.
- Update the Airflow DAG to train on confirmed labels.
- Add model promotion rules.
- Only promote a model if it beats the current production model on agreed metrics.

Example promotion rules:

- Recall must be above a minimum threshold.
- Precision must not fall below a minimum threshold.
- PR AUC must improve over the current production model.
- Model must pass inference smoke tests.

Acceptance checks:

- New models are trained from accumulated data.
- Bad models are not promoted automatically.
- The inference service can switch to a promoted model version.

## Phase 8: Testing And Developer Workflow

Goal: make the project easier to change without breaking the pipeline.

Tests to add:

- Producer schema validation tests.
- Feature engineering unit tests.
- Training smoke test on a tiny dataset.
- Inference test with a known model artifact.
- Docker Compose health checks.
- DAG import test.

Useful commands to document:

```bash
cd src
docker compose up -d
docker compose logs -f producer
docker compose logs -f airflow-scheduler
docker compose logs -f mlflow-server
docker compose down
```

Acceptance checks:

- A new developer can start the stack from README instructions.
- Tests can run locally.
- The DAG can be imported without starting the entire stack.

## Phase 9: Production-Style Hardening

Goal: make the project look like a real deployable data/ML system.

Hardening tasks:

- Move secrets out of committed files.
- Add `.env.example`.
- Add structured logging.
- Add retry and dead-letter-topic handling.
- Add message schema versioning.
- Add model input schema validation.
- Add Docker health checks for custom services.
- Add CI checks for formatting, tests, and Docker Compose config.
- Add resource limits for all services.

Optional advanced topics:

- Schema Registry.
- Feast feature store.
- Redis online feature cache.
- Kubernetes deployment.
- Terraform infrastructure.
- Canary model deployment.
- A/B model comparison.

## Suggested Implementation Order

Follow this sequence to avoid getting stuck:

1. Fix current config and producer issues.
2. Verify Docker Compose stack health.
3. Save Kafka transactions to Parquet.
4. Implement real Airflow training.
5. Log and register the model in MLflow.
6. Add inference service.
7. Publish to `fraud_predictions`.
8. Store predictions.
9. Add dashboard and monitoring.
10. Add feedback and retraining.

## Next Concrete Task

Start with Phase 0 and Phase 1:

- Fix `src/config.yaml`.
- Review producer fraud generation indentation.
- Add `.env.example`.
- Add a small consumer or Spark job that writes transactions to Parquet.
- Generate enough data for the first training run.

After that, the project will be ready for a real model-training DAG instead of the current training skeleton.
