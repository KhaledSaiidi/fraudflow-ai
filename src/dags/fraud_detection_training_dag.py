from datetime import datetime, timedelta
import logging

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import AirflowException, BashOperator


logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    level=logging.INFO
)

logger = logging.getLogger(__name__)
INGESTION_WORKER_COUNT = 3

default_args = {
    'owner': 'fraud_detection_team.com',
    'depends_on_past': False,
    'start_date': datetime(2026, 8, 1),
    'email_on_failure': False,
    'execution_timeout': timedelta(minutes=120),
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
}

def _train_model(**context):
    """
    Function to train the fraud detection model.
    This function should contain the logic to train the model, including data loading,
    preprocessing, model training, and saving the trained model.
    """
    from fraud_detection_training import FraudDetectionTraining
    try:
        logger.info("Initializing model training...")
        trainer = FraudDetectionTraining()
        model, precision = trainer.train_model()

        return {'status': 'success', 'precision': precision}

    except Exception as e:
        logger.error("Model training failed: %s", str(e), exc_info=True)
        raise AirflowException(f'Model Training failed: {str(e)}') from e

def _ingest_transactions(
    consumer_index: int,
    consumer_count: int,
    **context,
):
    """
    Function to ingest transactions from Kafka.
    This function should contain the logic to consume messages from Kafka, process them,
    and store them in the appropriate data store for model training.
    """
    from ingestor import TransactionConsumer

    consumer = TransactionConsumer(
        client_id=f"airflow-ingestor-{consumer_index}",
        worker_index=consumer_index,
        worker_count=consumer_count,
    )

    try:
        result = consumer.consume_available(
            max_messages_per_batch=1000,
            max_run_seconds=1800,
        )
        logger.info("Kafka ingestion completed: %s", result)
        return result
    except Exception as exc:
        logger.error("Kafka ingestion failed", exc_info=True)
        raise AirflowException(
            f"Kafka ingestion failed: {exc}"
        ) from exc
    finally:
        consumer.close()

with DAG(
    'fraud_detection_training',
    default_args=default_args,
    description='A DAG for training the fraud detection model',
    schedule="0 4 * * *",  # Every day at 04:00 AM
    catchup=False,
    tags=['fraud', 'ML'],
    max_active_runs=1,
) as dag:

    validate_environment = BashOperator(
        task_id='validate_environment',
        bash_command='''
        echo "Validating environment..."
        test -f /app/config.yaml &&
        test -f /app/.env &&
        echo "Environment validation successful." ||
        (echo "Environment validation failed. Required files are missing." && exit 1)
        '''
    )

    ingestion_tasks = [
        PythonOperator(
            task_id=f"ingest_transactions_{index}",
            python_callable=_ingest_transactions,
            op_kwargs={
                "consumer_index": index,
                "consumer_count": INGESTION_WORKER_COUNT,
            },
        )
        for index in range(INGESTION_WORKER_COUNT)
    ]

    training_task = PythonOperator(
        task_id='execute_training',
        python_callable=_train_model,
    )

    cleanup_task = BashOperator(
        task_id='cleanup',
        bash_command='echo "Cleaning up temporary files..." && rm -rf /app/tmp/*',
        trigger_rule='all_done'  # Ensure cleanup runs regardless of previous task outcomes
    )

    validate_environment >> ingestion_tasks >> training_task >> cleanup_task

    # Documentation 
    dag.doc_md = """
    # Fraud Detection Model Training DAG
    This DAG is responsible for training the fraud detection model. It performs the following steps:
    1. **Validate Environment**: Checks for the presence of required configuration files.
    2. **Execute Training**: Runs the model training process.
    3. **Cleanup**: Cleans up temporary files after training.
    Daily Training of fraud detection use: 
    - Transactions data from Kafka
    - Classifier with precision optimisation
    - MLFLOW for experiment tracking and model versioning
    """
