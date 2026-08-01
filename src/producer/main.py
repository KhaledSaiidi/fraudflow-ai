import json
import os
import signal
from confluent_kafka import Producer
from dotenv import load_dotenv
import logging
import random
from faker import Faker
import time

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    level=logging.INFO
)

logger = logging.getLogger(__name__)

load_dotenv(dotenv_path="/app/.env")

fake = Faker()

class TransactionProducer():
    def __init__(self):
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.kafka_username = os.getenv('KAFKA_USERNAME')
        self.kafka_password = os.getenv('KAFKA_PASSWORD')
        self.topic = os.getenv('KAFKA_TOPIC', 'transactions')
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
            logger.info(f"Connected to Kafka broker at {self.bootstrap_servers}")
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
                self.producer.close()
            logger.info('Producer stopped')

if __name__ == "__main__":
    producer = TransactionProducer()
    producer.run_continuous_production()
