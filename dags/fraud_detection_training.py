import logging
import os
import mlflow.sklearn
import pandas as pd
import boto3
import mlflow
from sklearn.model_selection import train_test_split
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
                local_file_path = self.load_from_minio(object_name)
                df = pd.read_parquet(local_file_path)
                X = df.drop(columns=['is_fraud'])
                y = df['is_fraud']
                X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

                # Define hyperparameters
                hyperparameters = {
                    'learning_rate': 0.1,
                    'n_estimators': 100,
                    'max_depth': 5,
                    'random_state': 42
                }
                mlflow.log_param("n_estimators", hyperparameters['n_estimators'])
                mlflow.log_param("max_depth", hyperparameters['max_depth'])
                mlflow.log_param("learning_rate", hyperparameters['learning_rate'])
                mlflow.log_param("random_state", hyperparameters['random_state'])

                # Train the model
                model = XGBClassifier(**hyperparameters)
                model.fit(X_train, y_train)

                logger.info("Model training completed and logged to MLflow.")

                predictions = model.predict(X_test)
                if predictions.sum() == 0:
                    precision = 0.0
                else:
                    precision = (predictions & y_test).sum() / predictions.sum()
                mlflow.log_metric("precision", precision)

                logger.info("Model prediction completed successfully.")

                return run.info.run_id, precision, experiment_name, register_model_name, artifact_path
            
        except Exception as e:
            logger.error("Model training failed: %s", str(e), exc_info=True)
            raise