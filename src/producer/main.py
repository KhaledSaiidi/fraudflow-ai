from importlib.metadata import metadata
import json
import os
import signal
from typing import Any, Dict, Optional
from confluent_kafka import Producer
from dotenv import load_dotenv
import logging
import random
from faker import Faker
import time
from datetime import datetime, timedelta, timezone

from jsonschema import ValidationError, validate, FormatChecker
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic
from confluent_kafka.cimpl import NewPartitions

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    level=logging.INFO
)

logger = logging.getLogger(__name__)

load_dotenv(dotenv_path="/app/.env")

fake = Faker()

TRANSACTION_SCHEMA = {
    "type": "object",
    "properties": {
        "transaction_id": {"type": "string"},
        "user_id": {"type": "number", "minimum": 1000, "maximum": 9999},
        "amount": {"type": "number", "minimum": 0.01, "maximum": 100000},
        "currency": {"type": "string", "pattern": "^[A-Z]{3}$"},
        "merchant": {"type": "string"},
        "timestamp": {"type": "string", "format": "date-time"},
        "location": {"type": "string","pattern": "^[A-Z]{2}$"},
        "is_fraud": {"type": "integer", "minimum": 0, "maximum": 1}
    },
    "required": ["transaction_id", "user_id", "amount", "currency", "timestamp", "is_fraud"]
}

class TransactionProducer():
    def __init__(self):
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.kafka_username = os.getenv('KAFKA_USERNAME')
        self.kafka_password = os.getenv('KAFKA_PASSWORD')
        self.topic = os.getenv('KAFKA_TOPIC', 'transactions')
        self.topic_partitions = int(os.getenv('KAFKA_TOPIC_PARTITIONS', 6))
        self.topic_replication_factor = int(os.getenv('KAFKA_TOPIC_REPLICATION_FACTOR', 3))
        self.running = False

        # confluent kafka producer configuration
        self.producer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': 'transaction-producer',
            'compression.type': 'gzip',
            'linger.ms': '5',
            'batch.size': 16384,
        }

        if self.kafka_username and self.kafka_password:
            self.producer_config.update({
                'security.protocol': 'SASL_SSL',
                'sasl.mechanism': 'PLAIN',
                'sasl.username': self.kafka_username,
                'sasl.password': self.kafka_password,
            })
        else: 
            self.producer_config['security.protocol'] = 'PLAINTEXT'

        try:
            self.producer = Producer(self.producer_config)
            self.ensure_topic_exists()
            logger.info(f"Connected to Kafka broker at {self.bootstrap_servers} and topic {self.topic} is ready")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka broker: {str(e)}")
            raise e

        self.compromised_users = set(random.sample(range(1000, 9999), 50)) #0.5 of the users are compromised
        self.high_risk_merchants = ['QuickCash', 'GlobalDigital', 'FastMoneyX']
        self.fraud_pattern_weights = {
            'account_takeover': 0.4,
            'card_testing': 0.3,
            'merchant_collusion': 0.2,
            'geo_anomaly': 0.1
        }

        # Configure graceful shutdown
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def ensure_topic_exists(self, retries: int = 30, delay: int = 5):
        startup_jitter = random.uniform(0, 6)
        logger.info("Waiting %.2fs before Kafka topic setup", startup_jitter)
        time.sleep(startup_jitter)
        last_error = None
        for attempt in range(1, retries + 1):
            try:
                self._ensure_topic_exists_once()
                return
            except Exception as e:
                last_error = e
                logger.warning(f"Attempt {attempt}/{retries} to ensure topic exists failed: {str(e)}")
                time.sleep(delay)
        logger.error(f"Failed to ensure topic exists after {retries} attempts: {str(last_error)}")
        raise RuntimeError(
            f"Failed to ensure topic {self.topic} exists after {retries} attempts"
        ) from last_error

    def _ensure_topic_exists_once(self):
        admin = AdminClient(self.producer_config)
        metadata = admin.list_topics(timeout=10)
        existing_topic = metadata.topics.get(self.topic)
        if existing_topic is None:
            topic = NewTopic(
                self.topic,
                self.topic_partitions,
                self.topic_replication_factor,
            )
            futures = admin.create_topics([topic])
            try:
                futures[self.topic].result()
                logger.info("Created Kafka topic %s with %d partitions",self.topic, self.topic_partitions)
                return
            except Exception as e:
                if "TOPIC_ALREADY_EXISTS" in str(e):
                    logger.info("Kafka topic %s was created by another producer", self.topic)
                    return
                raise
        current_partitions = len(existing_topic.partitions)
        if current_partitions == self.topic_partitions:
            logger.info("Kafka topic %s already exists with %d partitions", self.topic, current_partitions)
            return
        if current_partitions < self.topic_partitions:
            futures = admin.create_partitions([
                NewPartitions(self.topic, self.topic_partitions)
            ])
            futures[self.topic].result()
            logger.info("Increased Kafka topic %s partitions from %d to %d", self.topic, current_partitions, self.topic_partitions)
            return
        raise ValueError(
            f"Kafka topic {self.topic} has {current_partitions} partitions, "
            f"expected {self.topic_partitions}; Kafka cannot reduce partitions"
        )


    def delivery_report(self, err, msg):
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    def validate_transaction(self, transaction: Dict[str, Any]) -> bool:
        try:
            validate(
                instance=transaction,
                schema=TRANSACTION_SCHEMA,
                format_checker=FormatChecker()
            )
            return True
        except ValidationError as e:
            logger.error(f'Invalid  Transaction: {e.message}')
            return False

    def generate_transaction(self) -> Optional[Dict[str, Any]]:
        transaction = {
            'transaction_id': fake.uuid4(),
            'user_id': random.randint(1000, 9999),
            'amount': round(fake.pyfloat(min_value=0.01, max_value=10000.0), 2),
            'currency': 'USD',
            'merchant': fake.company(),
            'timestamp': (datetime.now(timezone.utc) + 
                          timedelta(seconds=random.randint(-300, 3000))).isoformat(),
            'location': fake.country_code(),
            'is_fraud': 0
        }
        is_fraud = 0
        amount = transaction['amount']
        user_id = transaction['user_id']
        merchant = transaction['merchant']
        # Account takeover
        if user_id in self.compromised_users and amount > 500:
            if random.random() < 0.3: # 30% chance of fraud if user is compromised and amount is high
                is_fraud = 1
                transaction['amount'] = random.uniform(500, 5000)
                transaction['merchant'] = random.choice(self.high_risk_merchants)
            # Card Testing
            if not is_fraud and amount < 2.0:
                # simulate rapid small txns
                if user_id % 1000 == 0 and random.random() < 0.25:
                    is_fraud = 1
                    transaction['amount'] = round(random.uniform(0.01, 2), 2)
                    transaction['location'] = 'US'

            # Merchant collusion
            if not is_fraud and merchant in self.high_risk_merchants:
                if amount > 3000 and random.random() < 0.15:
                    is_fraud = 1
                    transaction['amount'] = random.uniform(500, 5000)

            # Geo anomalies
            if not is_fraud:
                if user_id % 500 == 0 and random.random() < 0.1:
                    is_fraud = 1
                    transaction['location'] = random.choice(['CN', 'RU', 'GB'])

            # Baseline random fraud (0.1 - 0.3%)
            if not is_fraud and random.random() < 0.002:
                is_fraud = 1
                transaction['amount'] = random.uniform(100, 2000)

            # Ensure that final fraud rate is between 1-2%
            transaction['is_fraud'] = is_fraud if random.random() < 0.985 else 0

            # Validate modified transaction 
            if self.validate_transaction(transaction):
                return transaction

    def send_transaction(self) -> bool:
        try:
            transaction = self.generate_transaction()
            if not transaction:
                return False
            self.producer.produce(
                self.topic,
                key=transaction['transaction_id'],
                value=json.dumps(transaction),
                callback=self.delivery_report
            )
            self.producer.poll(0) # Trigger callbacks 
            return True
        except Exception as e:
            logger.error(f"Error producing message:: {str(e)}")
            return False

    def run_continuous_production(self, interval: float=0.0):
        """Run Continuous message Production with graceful shutdown"""
        self.running = True
        logger.info('Starting producer for topic %s...', self.topic)
        try:
            while self.running:
                if self.send_transaction():
                    time.sleep(interval)
        finally:
            self.shutdown()


    def shutdown(self, signum=None, frame=None):
        if self.running:
            logger.info("Initiating shutdown...")
            self.running = False

            if self.producer:
                self.producer.flush(timeout=30)
            logger.info('Producer stopped')

if __name__ == "__main__":
    producer = TransactionProducer()
    producer.run_continuous_production()
