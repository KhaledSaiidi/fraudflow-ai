import logging
import os
import pandas as pd
import boto3
import mlflow
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    average_precision_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from xgboost import XGBClassifier
from settings import load_config, minio_url, require_credential

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    level=logging.INFO,
    handlers=[
        logging.FileHandler('./fraud_detection_training.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class FraudDetectionTraining:
    def __init__(self, config_path='/app/config.yaml'):
        os.environ['GIT_PYTHON_REFRESH'] = 'quiet'
        os.environ['GIT_PYTHON_EXECUTABLE'] = '/usr/bin/git'

        self.config = load_config(config_path)

        access_key = require_credential("AWS_ACCESS_KEY_ID")
        secret_key = require_credential("AWS_SECRET_ACCESS_KEY")
        endpoint = minio_url(self.config)

        os.environ.update({
            "AWS_ACCESS_KEY_ID": access_key,
            "AWS_SECRET_ACCESS_KEY": secret_key,
            "AWS_ENDPOINT_URL_S3": endpoint,
            "MLFLOW_S3_ENDPOINT_URL": endpoint,
        })
        self._validate_environment()

        mlflow.set_tracking_uri(self.config['mlflow']['tracking_uri'])
        mlflow.set_experiment(self.config['mlflow']['experiment_name'])

    def _validate_environment(self):
        self._check_minio_connection()

    def _check_minio_connection(self):
        try:
            s3 = boto3.client(
                's3',
                endpoint_url=minio_url(self.config),
                aws_access_key_id=require_credential("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=require_credential("AWS_SECRET_ACCESS_KEY"),
            )
            buckets = s3.list_buckets()
            bucket_names = [b['Name'] for b in buckets.get('Buckets', [])]
            logger.info('Minio Connection successful. Buckets: %s...', bucket_names)
            mlflow_bucket = self.config["minio"]["buckets"]["mlflow"]
            if mlflow_bucket not in bucket_names:
                s3.create_bucket(Bucket=mlflow_bucket)
                logger.info('Created missing Minio bucket: %s...', mlflow_bucket)
        except Exception as e:
            logger.error('Minio Connection failed: %s...', str(e))
            raise

    def load_from_minio(self, object_name: str) -> str:
        try:
            s3 = boto3.client(
                's3',
                endpoint_url=minio_url(self.config),
                aws_access_key_id=require_credential("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=require_credential("AWS_SECRET_ACCESS_KEY"),
            )
            bucket_name = self.config["minio"]["buckets"]["fraud_features"]
            local_file_path = '/tmp/features.parquet'

            s3.download_file(bucket_name, object_name, local_file_path)
            logger.info('Downloaded feature parquet from MinIO: %s/%s', bucket_name, object_name)
            return local_file_path
        except Exception as e:
            logger.error('Failed to load feature parquet from MinIO: %s...', str(e))
            raise

    def _get_training_features(self,
                               df: pd.DataFrame,
                               label_column:str,
                               ) -> tuple[pd.DataFrame, pd.Series]:
        feature_columns = self.config.get("features")

        if not feature_columns:
            raise ValueError("Missing features list in config.yaml")
        missing_features = sorted(set(feature_columns) - set(df.columns))
        if missing_features:
            raise ValueError("Missing features list in config.yaml")

        invalid_features = [
            column
            for column in feature_columns
            if not pd.api.types.is_numeric_dtype(df[column])
        ]
        if invalid_features:
            raise ValueError(
                f"Feature columns must be numeric. Invalid columns: {invalid_features}"
            )

        x = df[feature_columns]
        y = df[label_column]

        return x, y


    def _validate_training_dataframe(self, df: pd.DataFrame) -> str:
        if df.empty:
            raise ValueError("Training dataset is empty")

        label_column = self.config["training_data"]["label_column"]

        if label_column not in df.columns:
            raise ValueError(
                f"Training dataset is missing label column: {label_column}"
            )

        if df[label_column].isna().any():
            raise ValueError(f"Label column {label_column} contains null values")

        labels = set(df[label_column].unique())
        invalid_labels = labels - {0, 1}

        if invalid_labels:
            raise ValueError(
                f"Label column {label_column} must contain only 0/1 values. "
                f"Found: {sorted(invalid_labels)}"
            )

        if len(labels) < 2:
            raise ValueError(
                f"Training dataset must contain both classes 0 and 1. "
                f"Found only: {sorted(labels)}"
            )

        return label_column

    def train_model(self, object_name: str) -> tuple:
        try:
            logger.info("Starting model training...")

            experiment_name = self.config['mlflow']['experiment_name']
            register_model_name = self.config['mlflow']['register_model_name']
            tracking_uri = self.config['mlflow']['tracking_uri']
            artifact_path = self.config['mlflow']['artifact_path']

            mlflow.set_tracking_uri(tracking_uri)
            mlflow.set_experiment(experiment_name)

            with mlflow.start_run() as run:
                model_config = self.config["model"]
                xgboost_config = dict(model_config["xgboost"])

                local_file_path = self.load_from_minio(object_name)
                df = pd.read_parquet(local_file_path)

                label_column = self._validate_training_dataframe(df)
                X, y = self._get_training_features(df, label_column)
                X_train, X_test, y_train, y_test = train_test_split(
                    X,
                    y,
                    test_size=model_config["test_size"],
                    random_state=model_config["random_state"],
                    stratify=y,
                    )
                negative_count = int((y_train == 0).sum())
                positive_count = int((y_train == 1).sum())
                scale_pos_weight = negative_count / positive_count

                hyperparameters = {
                    **xgboost_config,
                    "random_state": model_config["random_state"],
                    "scale_pos_weight": scale_pos_weight,
                }
                mlflow.log_metric("negative_count", negative_count)
                mlflow.log_metric("positive_count", positive_count)
                mlflow.log_metric("fraud_rate", positive_count / len(y_train))
                mlflow.log_param("test_size", model_config["test_size"])
                for param_name, param_value in hyperparameters.items():
                    mlflow.log_param(param_name, param_value)

                # Train the model
                model = XGBClassifier(**hyperparameters)
                model.fit(X_train, y_train)

                logger.info("Model training completed and logged to MLflow.")

                predictions = model.predict(X_test)
                prediction_scores = model.predict_proba(X_test)[:, 1]

                precision = float(precision_score(y_test, predictions, zero_division=0))
                recall = float(recall_score(y_test, predictions, zero_division=0))
                f1 = float(f1_score(y_test, predictions, zero_division=0))

                roc_auc = float(roc_auc_score(y_test, prediction_scores))
                average_precision = float(average_precision_score(y_test, prediction_scores))

                mlflow.log_metric("precision", precision)
                mlflow.log_metric("recall", recall)
                mlflow.log_metric("f1", f1)
                mlflow.log_metric("roc_auc", roc_auc)
                mlflow.log_metric("average_precision", average_precision)

                logger.info("Model prediction completed successfully.")

                return run.info.run_id, precision, experiment_name, register_model_name, artifact_path
            
        except Exception as e:
            logger.error("Model training failed: %s", str(e), exc_info=True)
            raise
