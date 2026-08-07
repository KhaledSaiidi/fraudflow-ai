from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
from io import BytesIO
import json
import logging
import math
import time
from typing import Any

from confluent_kafka import Consumer, OFFSET_STORED, TopicPartition
from minio import Minio
import pyarrow as pa
import pyarrow.parquet as pq

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
}


@dataclass(frozen=True)
class ConsumedBatch:
    transactions: list[dict[str, Any]]
    offsets: list[TopicPartition]
    consumed_messages: int


class TransactionConsumer:
    def __init__(
        self,
        client_id: str | None = None,
        worker_index: int = 0,
        worker_count: int = 1,
        config_path: str = "/app/config.yaml",
    ):
        if worker_count < 1:
            raise ValueError("worker_count must be at least 1")
        if not 0 <= worker_index < worker_count:
            raise ValueError("worker_index must be between 0 and worker_count - 1")

        self.config = load_config(config_path)
        kafka_config = self.config["kafka"]
        minio_config = self.config["minio"]
        ingestion_config = self.config["ingestion"]

        self.bootstrap_servers = kafka_config["bootstrap_servers"]
        self.kafka_username = require_credential("KAFKA_USERNAME")
        self.kafka_password = require_credential("KAFKA_PASSWORD")
        self.kafka_security_protocol = kafka_config["security_protocol"]
        self.kafka_sasl_mechanism = kafka_config["sasl_mechanism"]
        self.topic = kafka_config["topic"]
        self.topic_partitions = int(kafka_config["topic_partitions"])
        self.minio_endpoint = minio_config["endpoint"]
        self.minio_secure = bool(minio_config["secure"])
        self.minio_access_key = require_credential("AWS_ACCESS_KEY_ID")
        self.minio_secret_key = require_credential("AWS_SECRET_ACCESS_KEY")
        self.transactions_bucket = minio_config["buckets"]["transactions"]
        self.max_messages_per_batch = int(
            ingestion_config["max_messages_per_batch"]
        )
        self.max_run_seconds = int(ingestion_config["max_run_seconds"])
        self.max_wait_seconds = int(ingestion_config["max_wait_seconds"])
        self.dedup_shard_count = int(ingestion_config["dedup_shard_count"])
        if self.dedup_shard_count < 1:
            raise ValueError("ingestion.dedup_shard_count must be at least 1")
        self.worker_index = worker_index
        self.worker_count = worker_count
        self.partition_ids = list(
            range(worker_index, self.topic_partitions, worker_count)
        )
        if not self.partition_ids:
            raise ValueError(
                f"worker {worker_index} has no partitions; "
                f"topic has {self.topic_partitions} partitions"
            )

        # Confluent Kafka consumer configuration
        self.consumer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': kafka_config["consumer"]["group_id"],
            "client.id": client_id or kafka_config["consumer"]["client_id"],
            'auto.offset.reset': kafka_config["consumer"]["auto_offset_reset"],
            'enable.auto.commit': False,
            'enable.auto.offset.store': False,
        }

        if self.kafka_username and self.kafka_password:
            self.consumer_config.update({
                'security.protocol': self.kafka_security_protocol,
                'sasl.mechanism': self.kafka_sasl_mechanism,
                'sasl.username': self.kafka_username,
                'sasl.password': self.kafka_password,
            })
        else: 
            self.consumer_config['security.protocol'] = 'PLAINTEXT'

        try:
            self.consumer = Consumer(self.consumer_config)
            self.assign_partitions()
        except Exception as e:
            logger.error("Failed to initialize Kafka consumer: %s", str(e))
            raise

    def assign_partitions(self) -> None:
        """Assign this worker a deterministic, non-overlapping partition set."""
        try:
            partitions = [
                TopicPartition(self.topic, partition_id, OFFSET_STORED)
                for partition_id in self.partition_ids
            ]
            self.consumer.assign(partitions)
            logger.info(
                "Assigned Kafka topic %s partitions %s to worker %d/%d",
                self.topic,
                self.partition_ids,
                self.worker_index + 1,
                self.worker_count,
            )
        except Exception as exc:
            logger.error(
                "Failed to assign Kafka topic %s partitions %s",
                self.topic,
                self.partition_ids,
                extra={
                    "event": "kafka_partition_assignment_failed",
                    "topic": self.topic,
                    "partitions": self.partition_ids,
                    "error": str(exc),
                },
                exc_info=True,
            )
            raise

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

    def consume_available(
        self,
        max_messages_per_batch: int | None = None,
        max_run_seconds: int | None = None,
    ) -> dict[str, int]:
        if max_messages_per_batch is None:
            max_messages_per_batch = self.max_messages_per_batch
        if max_run_seconds is None:
            max_run_seconds = self.max_run_seconds

        deadline = time.monotonic() + max_run_seconds
        consumed_message_count = 0
        transaction_count = 0
        object_count = 0

        while time.monotonic() < deadline:
            batch = self._consume_transactions(
                max_messages=max_messages_per_batch,
                max_wait_seconds=self.max_wait_seconds,
            )

            # Kafka has been quiet for five seconds: backlog is drained.
            if batch.consumed_messages == 0:
                break

            object_names: list[str] = []
            if batch.transactions:
                batch_id = self._build_batch_id(batch.transactions)
                object_names = self._persist_transactions(
                    batch.transactions,
                    batch_id=batch_id,
                )

            # Commit explicit offsets only after every Parquet object succeeds.
            self._commit_offsets(batch.offsets)

            consumed_message_count += batch.consumed_messages
            transaction_count += len(batch.transactions)
            object_count += len(object_names)

        return {
            "consumed_messages": consumed_message_count,
            "transactions": transaction_count,
            "rejected_messages": consumed_message_count - transaction_count,
            "objects": object_count,
        }

    def _consume_transactions(
        self,
        max_messages: int = 100,
        max_wait_seconds: int = 5,
    ) -> ConsumedBatch:
        transactions: list[dict[str, Any]] = []
        next_offsets: dict[tuple[str, int], int] = {}
        consumed_messages = 0
        deadline = time.monotonic() + max_wait_seconds

        while consumed_messages < max_messages:
            remaining_seconds = deadline - time.monotonic()
            if remaining_seconds <= 0:
                break

            msg = self.consumer.poll(timeout=min(1.0, remaining_seconds))
            if msg is None:
                continue
            if msg.error():
                logger.error("Consumer error: %s", msg.error())
                continue

            topic = msg.topic()
            partition = msg.partition()
            offset = msg.offset()
            if topic is None or partition is None or offset is None:
                raise RuntimeError(
                    "Kafka returned a message without complete topic, partition, "
                    "or offset metadata"
                )

            consumed_messages += 1
            offset_key = (topic, partition)
            next_offsets[offset_key] = max(
                next_offsets.get(offset_key, 0),
                offset + 1,
            )

            try:
                transaction = self._decode_and_validate_transaction(msg)
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
                logger.error(
                    "Skipping invalid transaction at %s[%s] offset %s: %s",
                    topic,
                    partition,
                    offset,
                    str(exc),
                )
                continue

            transactions.append(transaction)

        offsets = [
            TopicPartition(topic, partition, offset)
            for (topic, partition), offset in sorted(next_offsets.items())
        ]
        return ConsumedBatch(
            transactions=transactions,
            offsets=offsets,
            consumed_messages=consumed_messages,
        )

    def _commit_offsets(self, offsets: list[TopicPartition]) -> None:
        """Synchronously commit the next offset for each owned partition."""
        if not offsets:
            return

        committed_offsets = self.consumer.commit(
            offsets=offsets,
            asynchronous=False,
        )
        failures = [
            partition
            for partition in committed_offsets or []
            if partition.error is not None
        ]
        if failures:
            raise RuntimeError(f"Kafka offset commit failed: {failures}")

        logger.info(
            "Committed Kafka offsets: %s",
            [
                f"{partition.topic}[{partition.partition}]={partition.offset}"
                for partition in committed_offsets or offsets
            ],
        )

    def _decode_and_validate_transaction(self, msg: Any) -> dict[str, Any]:
        """Decode, validate, normalize, and enrich one Kafka transaction."""
        payload = msg.value()
        if payload is None:
            raise ValueError("message payload is empty")

        if isinstance(payload, bytes):
            decoded_payload = payload.decode("utf-8")
        elif isinstance(payload, str):
            decoded_payload = payload
        else:
            raise ValueError(
                f"unsupported payload type {type(payload).__name__}"
            )

        transaction = json.loads(decoded_payload)
        if not isinstance(transaction, dict):
            raise ValueError("transaction payload must be a JSON object")

        missing_fields = sorted(REQUIRED_TRANSACTION_FIELDS - transaction.keys())
        if missing_fields:
            raise ValueError(
                f"missing required fields: {', '.join(missing_fields)}"
            )

        transaction_id = transaction["transaction_id"]
        if not isinstance(transaction_id, str) or not transaction_id.strip():
            raise ValueError("transaction_id must be a non-empty string")

        user_id = transaction["user_id"]
        if (
            isinstance(user_id, bool)
            or not isinstance(user_id, (int, float))
            or not float(user_id).is_integer()
            or not 1000 <= user_id <= 9999
        ):
            raise ValueError("user_id must be an integer between 1000 and 9999")

        amount = transaction["amount"]
        if (
            isinstance(amount, bool)
            or not isinstance(amount, (int, float))
            or not math.isfinite(float(amount))
            or not 0.01 <= amount <= 100000
        ):
            raise ValueError("amount must be between 0.01 and 100000")

        currency = transaction["currency"]
        if (
            not isinstance(currency, str)
            or len(currency) != 3
            or not currency.isalpha()
            or not currency.isupper()
        ):
            raise ValueError("currency must contain three uppercase letters")

        fraud_label = transaction["is_fraud"]
        if type(fraud_label) is not int or fraud_label not in (0, 1):
            raise ValueError("is_fraud must be either 0 or 1")

        merchant = transaction.get("merchant")
        if merchant is not None and not isinstance(merchant, str):
            raise ValueError("merchant must be a string when provided")

        location = transaction.get("location")
        if location is not None and (
            not isinstance(location, str)
            or len(location) != 2
            or not location.isalpha()
            or not location.isupper()
        ):
            raise ValueError("location must contain two uppercase letters")

        timestamp_value = transaction["timestamp"]
        if not isinstance(timestamp_value, str):
            raise ValueError("timestamp must be an ISO-8601 string")
        try:
            event_timestamp = datetime.fromisoformat(
                timestamp_value.replace("Z", "+00:00")
            )
        except ValueError as exc:
            raise ValueError("timestamp must be a valid ISO-8601 value") from exc
        if event_timestamp.tzinfo is None or event_timestamp.utcoffset() is None:
            raise ValueError("timestamp must include a timezone")

        event_timestamp_utc = event_timestamp.astimezone(timezone.utc)
        ingested_at = datetime.now(timezone.utc)

        return {
            "transaction_id": transaction_id,
            "user_id": int(user_id),
            "amount": float(amount),
            "currency": currency,
            "merchant": merchant,
            "timestamp": event_timestamp_utc,
            "location": location,
            "is_fraud": int(fraud_label),
            "event_date": event_timestamp_utc.date().isoformat(),
            "ingested_at": ingested_at,
            "kafka_topic": msg.topic(),
            "kafka_partition": msg.partition(),
            "kafka_offset": msg.offset(),
        }

    @staticmethod
    def _build_batch_id(transactions: list[dict[str, Any]]) -> str:
        """Build an ID that remains stable when the same Kafka batch is retried."""
        offsets = sorted(
            (
                str(transaction["kafka_topic"]),
                int(transaction["kafka_partition"]),
                int(transaction["kafka_offset"]),
            )
            for transaction in transactions
        )
        identity = "|".join(
            f"{topic}:{partition}:{offset}"
            for topic, partition, offset in offsets
        )
        return hashlib.sha256(identity.encode("utf-8")).hexdigest()[:24]

    @staticmethod
    def get_dedup_shard(transaction_id: str, shard_count: int) -> int:
        if shard_count < 1:
            raise ValueError("shard_count must be at least 1")
        digest = hashlib.sha256(transaction_id.encode("utf-8")).digest()
        return int.from_bytes(digest[:8], byteorder="big") % shard_count

    def _persist_transactions(
        self,
        transactions: list[dict[str, Any]],
        batch_id: str,
    ) -> list[str]:
        """Write each event-date and dedup-shard group to MinIO."""
        if not transactions:
            logger.info("No transactions to persist.")
            return []

        minio_client = self.connect_to_minio()
        bucket_name = self.transactions_bucket
        transactions_by_partition: dict[
            tuple[str, int],
            list[dict[str, Any]],
        ] = defaultdict(list)
        for transaction in transactions:
            dedup_shard = self.get_dedup_shard(
                transaction["transaction_id"],
                self.dedup_shard_count,
            )
            transaction["dedup_shard"] = dedup_shard
            partition_key = (str(transaction["event_date"]), dedup_shard)
            transactions_by_partition[partition_key].append(transaction)

        try:
            if not minio_client.bucket_exists(bucket_name):
                minio_client.make_bucket(bucket_name)
                logger.info("Created Minio bucket: %s", bucket_name)

            object_names: list[str] = []
            for (event_date, dedup_shard), shard_transactions in sorted(
                transactions_by_partition.items()
            ):
                object_name = (
                    f"event_date={event_date}/"
                    f"dedup_shard={dedup_shard}/"
                    f"batch-{batch_id}.parquet"
                )
                parquet_stream = BytesIO()
                table = pa.Table.from_pylist(shard_transactions)
                pq.write_table(table, parquet_stream, compression="snappy")
                length = parquet_stream.tell()
                parquet_stream.seek(0)

                minio_client.put_object(
                    bucket_name=bucket_name,
                    object_name=object_name,
                    data=parquet_stream,
                    length=length,
                    content_type="application/vnd.apache.parquet",
                )
                object_names.append(object_name)
                logger.info(
                    "Persisted %d transactions to s3://%s/%s",
                    len(shard_transactions),
                    bucket_name,
                    object_name,
                )

            return object_names
        except Exception as exc:
            logger.error(
                "Failed to persist transactions to Minio: %s",
                str(exc),
                exc_info=True,
            )
            raise

    def close(self) -> None:
        """Stop polling and close the Kafka consumer."""
        self.consumer.close()
        logger.info("Kafka consumer closed")


if __name__ == "__main__":
    consumer = TransactionConsumer()
    try:
        consumer.consume_available()
    finally:
        consumer.close()
