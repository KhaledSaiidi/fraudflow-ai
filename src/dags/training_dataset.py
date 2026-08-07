import logging
import os
from datetime import datetime, timedelta
from io import BytesIO

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from dotenv import load_dotenv
from minio import Minio

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    level=logging.INFO
)

logger = logging.getLogger(__name__)
load_dotenv(dotenv_path="/app/.env")


REQUIRED_TRANSACTION_FIELDS = {
    "transaction_id",
    "user_id",
    "amount",
    "currency",
    "timestamp",
    "is_fraud",
}

class TrainingDataset:
    def __init__(self, config_path='/app/config.yaml'):
        self.minio_endpoint = os.getenv('MINIO_ENDPOINT', 'minio:9000')
        self.minio_access_key = os.getenv('AWS_ACCESS_KEY_ID')
        self.minio_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.config = self._load_config(config_path)

        self.source_bucket = self.config["training_data"]["source_bucket"]
        self.destination_bucket = self.config["training_data"]["destination_bucket"]
        self.lookback_days = self.config["training_data"]["lookback_days"]
        self.minimum_rows = self.config["training_data"]["minimum_rows"]
        self.label_column = self.config["training_data"]["label_column"]
        self.dataset_version = self.config["training_data"]["dataset_version"]

    def _load_config(self, config_path: str) -> dict:
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            logger.info('Configuration loaded successfully...')
            return config
        except Exception as e:
            logger.error('Failed to load Config: %s...', str(e))
            raise

    def connect_to_minio(self) -> Minio:
        """Connect to Minio storage and return the client."""
        try:
            minio_client = Minio(
                self.minio_endpoint,
                access_key=self.minio_access_key,
                secret_key=self.minio_secret_key,
                secure=False
            )
            logger.info("Connected to Minio at %s", self.minio_endpoint)
            return minio_client
        except Exception as e:
            logger.error("Failed to connect to Minio: %s", str(e))
            raise

    @staticmethod
    def _validate_data(table: pa.Table) -> tuple[bool, str]:
        required_fields = {
            "transaction_id",
            "user_id",
            "amount",
            "currency",
            "timestamp",
            "is_fraud",
        }
        missing_fields = required_fields - set(table.column_names)
        if missing_fields:
            return False, f"Missing required fields: {missing_fields}"
        return True, "Data validation passed"
    
    @staticmethod
    def _deduplicate_data(table: pa.Table) -> tuple[pa.Table, int]:
        """Deduplicate the data based on transaction_id and timestamp."""
        frame = table.to_pandas()
        rows_before = len(frame)
        frame = frame.sort_values(
            [
                "transaction_id",
                "ingested_at",
                "kafka_partition",
                "kafka_offset",
            ],
            kind="mergesort",
        )
        frame = frame.drop_duplicates(
            subset=["transaction_id"],
            keep="first",
        )
        return (
            pa.Table.from_pandas(frame, preserve_index=False),
            rows_before - len(frame),
        )

    def _get_data(
        self,
        cutoff: datetime,
        lookback_days: int,
    ) -> tuple[pa.Table, list[str]]:
        """Get Data as a Parquet object in MinIO."""

        minio_client = self.connect_to_minio()
        start = cutoff - timedelta(days=lookback_days)
        object_names: list[str] = []
        current_date = start.date()
        while current_date <= cutoff.date():
            prefix = f"event_date={current_date.isoformat()}/"
            for obj in minio_client.list_objects(
                self.source_bucket,
                prefix=prefix,
                recursive=True,
            ):
                if obj.object_name and obj.object_name.endswith(".parquet"):
                    object_names.append(obj.object_name)
            current_date += timedelta(days=1)
        object_names.sort()
        tables: list[pa.Table] = []
        for object_name in object_names:
            response = minio_client.get_object(
                self.source_bucket,
                object_name,
            )

            try:
                payload = response.read()
                table = pq.read_table(BytesIO(payload))
                _validate_data, message = self._validate_data(table)
                if not _validate_data:
                    logger.warning(
                        "Data validation failed for %s: %s",
                        object_name,
                        message,
                    )
                    continue
                table, deduped_count = self._deduplicate_data(table)
                if deduped_count > 0:
                    logger.info(
                        "Deduplicated %d transactions from %s",
                        deduped_count,
                        object_name,
                    )
                tables.append(table)
            finally:
                response.close()
                response.release_conn()
        if not tables:
            raise ValueError(
                f"No transaction data found between {start} and {cutoff}"
            )

        combined = pa.concat_tables(
            tables,
            promote_options="default",
        )
        frame = combined.to_pandas()
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)

        frame = frame[
            (frame["timestamp"] >= start)
            & (frame["timestamp"] < cutoff)
        ]
        combined = pa.Table.from_pandas(frame, preserve_index=False)
        return combined, object_names

    def _write_dataset(self):
        pass

    def _create_features(self):
        pass
