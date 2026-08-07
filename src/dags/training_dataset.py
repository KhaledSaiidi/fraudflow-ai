import logging
from datetime import datetime, timedelta
from io import BytesIO

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio

from settings import load_config, require_credential

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    level=logging.INFO
)

logger = logging.getLogger(__name__)


REQUIRED_TRANSACTION_FIELDS = {
    "transaction_id",
    "user_id",
    "amount",
    "currency",
    "timestamp",
    "is_fraud",
    "ingested_at",
    "kafka_partition",
    "kafka_offset",
    "dedup_shard",
}

DEDUPLICATION_FIELDS = {
    "transaction_id",
    "ingested_at",
    "kafka_partition",
    "kafka_offset",
    "dedup_shard",
}

class TrainingDataset:
    def __init__(self, config_path='/app/config.yaml'):
        self.config = load_config(config_path)
        minio_config = self.config["minio"]

        self.minio_endpoint = minio_config["endpoint"]
        self.minio_secure = bool(minio_config["secure"])
        self.minio_access_key = require_credential("AWS_ACCESS_KEY_ID")
        self.minio_secret_key = require_credential("AWS_SECRET_ACCESS_KEY")
        self.dedup_shard_count = int(
            self.config["ingestion"]["dedup_shard_count"]
        )
        if self.dedup_shard_count < 1:
            raise ValueError("ingestion.dedup_shard_count must be at least 1")

        buckets = minio_config["buckets"]
        self.source_bucket = buckets["transactions"]
        self.destination_bucket = buckets["fraud_features"]
        self.lookback_days = self.config["training_data"]["lookback_days"]
        self.minimum_rows = self.config["training_data"]["minimum_rows"]
        self.label_column = self.config["training_data"]["label_column"]
        self.dataset_version = self.config["training_data"]["dataset_version"]

    def connect_to_minio(self) -> Minio:
        """Connect to Minio storage and return the client."""
        try:
            minio_client = Minio(
                self.minio_endpoint,
                access_key=self.minio_access_key,
                secret_key=self.minio_secret_key,
                secure=self.minio_secure,
            )
            logger.info("Connected to Minio at %s", self.minio_endpoint)
            return minio_client
        except Exception as e:
            logger.error("Failed to connect to Minio: %s", str(e))
            raise

    @staticmethod
    def _validate_data(table: pa.Table) -> tuple[bool, str]:
        missing_fields = REQUIRED_TRANSACTION_FIELDS - set(table.column_names)
        if missing_fields:
            return False, f"Missing required fields: {missing_fields}"

        null_deduplication_fields = sorted(
            field
            for field in DEDUPLICATION_FIELDS
            if table.column(field).null_count > 0
        )
        if null_deduplication_fields:
            return (
                False,
                "Null values in deduplication fields: "
                f"{null_deduplication_fields}",
            )
        return True, "Data validation passed"
    
    @staticmethod
    def _deduplicate_data(table: pa.Table) -> tuple[pa.Table, int]:
        """Keep the earliest ingested row for each transaction ID."""
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
        shard_index: int,
    ) -> tuple[pa.Table, list[str]]:
        """Load and globally deduplicate one transaction shard from MinIO."""

        if not 0 <= shard_index < self.dedup_shard_count:
            raise ValueError(
                "shard_index must be between 0 and "
                f"{self.dedup_shard_count - 1}"
            )

        minio_client = self.connect_to_minio()
        start = cutoff - timedelta(days=lookback_days)
        object_names: list[str] = []
        current_date = start.date()
        while current_date <= cutoff.date():
            prefix = (
                f"event_date={current_date.isoformat()}/"
                f"dedup_shard={shard_index}/"
            )
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
        loaded_object_names: list[str] = []
        for object_name in object_names:
            response = minio_client.get_object(
                self.source_bucket,
                object_name,
            )

            try:
                payload = response.read()
                table = pq.read_table(BytesIO(payload))
                is_valid, message = self._validate_data(table)
                if not is_valid:
                    logger.warning(
                        "Data validation failed for %s: %s",
                        object_name,
                        message,
                    )
                    continue

                stored_shards = set(table.column("dedup_shard").to_pylist())
                if stored_shards != {shard_index}:
                    raise ValueError(
                        f"Object {object_name} contains dedup shards "
                        f"{sorted(stored_shards)}, expected only {shard_index}"
                    )

                tables.append(table)
                loaded_object_names.append(object_name)
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
        combined, deduped_count = self._deduplicate_data(combined)
        if deduped_count > 0:
            logger.info(
                "Deduplicated %d transactions from shard %d",
                deduped_count,
                shard_index,
            )

        return combined, loaded_object_names

    def _write_dataset(self):
        pass

    def _create_features(self):
        pass
