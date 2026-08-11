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

    def build_training_dataset(self, cutoff: datetime) -> str:
        """Build the training dataset for fraud detection."""
        all_tables: list[pa.Table] = []
        for shard_index in range(self.dedup_shard_count):
            logger.info(
                "Loading data for deduplication shard %d/%d",
                shard_index + 1,
                self.dedup_shard_count,
            )
            table, _ = self._get_data(
                cutoff=cutoff,
                lookback_days=self.lookback_days,
                shard_index=shard_index,
            )
            all_tables.append(table)

        combined_table = pa.concat_tables(all_tables, promote_options="default")
        feature_table = self._create_features(combined_table)

        if feature_table.num_rows < self.minimum_rows:
            raise ValueError(
                f"Training dataset has only {feature_table.num_rows} rows, "
                f"which is less than the minimum required {self.minimum_rows}"
            )

        object_name = self._write_dataset(feature_table, cutoff)
        logger.info(
            "Training dataset built successfully with %d rows and %d columns. "
            "Stored at %s/%s",
            feature_table.num_rows,
            feature_table.num_columns,
            self.destination_bucket,
            object_name,
        )
        return object_name
    
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

        if len(frame) == 0:
            raise ValueError(
                f"No transactions remain for shard {shard_index} "
                f"after filtering to [{start}, {cutoff})"
            )
        combined = pa.Table.from_pandas(frame, preserve_index=False)
        combined, deduped_count = self._deduplicate_data(combined)
        if deduped_count > 0:
            logger.info(
                "Deduplicated %d transactions from shard %d",
                deduped_count,
                shard_index,
            )

        return combined, loaded_object_names

    def _write_dataset(self, feature_table: pa.Table, cutoff: datetime) -> str:
        buffer = BytesIO()
        pq.write_table(feature_table, buffer)
        data_len = buffer.tell()
        buffer.seek(0)
        object_name = (
            f"{self.dataset_version}/"
            f"features-{cutoff.date().isoformat()}.parquet"
        )
        minio_client = self.connect_to_minio()
        try:
            minio_client.put_object(
                self.destination_bucket,
                object_name,
                buffer,
                length=data_len,
                content_type="application/octet-stream",
            )
            logger.info("Wrote dataset to %s/%s", self.destination_bucket, object_name)
            return object_name
        except Exception as e:
            logger.error("Failed to write dataset to %s/%s: %s", self.destination_bucket, object_name, e)
            raise

    _HIGH_RISK_MERCHANTS = {"QuickCash", "GlobalDigital", "FastMoneyX"}
    _SUSPICIOUS_LOCATIONS = {"CN", "RU", "GB"}

    @staticmethod
    def _create_features(table: pa.Table) -> pa.Table:
        """Create features for fraud detection."""
        import math
        try:
            frame = table.to_pandas()

            frame["hour_of_day"] = frame["timestamp"].dt.hour
            frame["day_of_week"] = frame["timestamp"].dt.dayofweek
            frame["is_weekend"] = frame["day_of_week"].isin([5, 6]).astype(int)
            frame["log_amount"] = frame["amount"].clip(lower=0.01).apply(math.log)
            frame["is_card_testing"] = (frame["amount"] < 2.0).astype(int)
            frame["is_large_amount"] = (frame["amount"] > 500).astype(int)
            frame["is_very_large_amount"] = (frame["amount"] > 3000).astype(int)
            frame["is_high_risk_merchant"] = frame["merchant"].isin(
                TrainingDataset._HIGH_RISK_MERCHANTS
            ).astype(int)
            frame["is_suspicious_location"] = frame["location"].isin(
                TrainingDataset._SUSPICIOUS_LOCATIONS
            ).astype(int)
            frame["is_fraud"] = frame["is_fraud"].astype(int)

            # drop Kafka metadata — not predictive signals
            frame = frame.drop(
                columns=["transaction_id", "ingested_at", "kafka_partition", "kafka_offset", "dedup_shard"],
                errors="ignore",
            )
            return pa.Table.from_pandas(frame, preserve_index=False)
        except Exception as e:
            logger.error("Error creating features: %s", str(e))
            raise