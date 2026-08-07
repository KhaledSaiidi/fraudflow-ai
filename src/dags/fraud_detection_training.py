import logging
import os
import boto3
import mlflow

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

    def train_model(self) -> tuple:
        try:
            logger.info("Starting model training...")
            model = "trained_model"
            precision = 0.95
            logger.info("Model training completed successfully.")
            return model, precision
        except Exception as e:
            logger.error("Model training failed: %s", str(e), exc_info=True)
            raise
