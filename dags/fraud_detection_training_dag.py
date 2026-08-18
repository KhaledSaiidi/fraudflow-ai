from datetime import datetime, timedelta, timezone
import logging

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk.exceptions import AirflowException

from settings import load_config


logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    level=logging.INFO
)

logger = logging.getLogger(__name__)
CONFIG = load_config()
AIRFLOW_CONFIG = CONFIG["airflow"]
DAG_CONFIG = AIRFLOW_CONFIG["dag"]
INGESTION_CONFIG = CONFIG["ingestion"]
INGESTION_WORKER_COUNT = int(INGESTION_CONFIG["worker_count"])

default_args = {
    'owner': 'fraud_detection_team.com',
    'depends_on_past': False,
    'start_date': datetime.fromisoformat(DAG_CONFIG["start_date"]),
    'email_on_failure': False,
    'execution_timeout': timedelta(
        minutes=int(DAG_CONFIG["execution_timeout_minutes"])
    ),
    "retries": int(DAG_CONFIG["retries"]),
    "retry_delay": timedelta(
        seconds=int(DAG_CONFIG["retry_delay_seconds"])
    ),
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
        task_instance = context.get("ti")
        dataset_payload = (
            task_instance.xcom_pull(task_ids="build_training_dataset")
            if task_instance
            else None
        )
        object_name = (
            dataset_payload.get("object_name")
            if isinstance(dataset_payload, dict)
            else None
        )
        if not object_name:
            raise AirflowException(
                "Missing object_name from build_training_dataset XCom"
            )

        trainer = FraudDetectionTraining()
        training_results = trainer.train_model(object_name=object_name)
        (
            precision, 
            experiment_name, 
            register_model_name, 
            artifact_path, 
            logged_model_uri, 
            model_registered_uri, 
            model_alias_uri
        ) = training_results

        return {
            'status': 'success',
            'precision': precision,
            'experiment_name': experiment_name,
            'register_model_name': register_model_name,
            'artifact_path': artifact_path,
            'logged_model_uri': logged_model_uri,
            'model_registered_uri': model_registered_uri,
            'model_alias_uri': model_alias_uri
        }

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
        client_id=(
            f"{CONFIG['kafka']['consumer']['client_id']}-{consumer_index}"
        ),
        worker_index=consumer_index,
        worker_count=consumer_count,
    )

    try:
        result = consumer.consume_available()
        logger.info("Kafka ingestion completed: %s", result)
        return result
    except Exception as exc:
        logger.error("Kafka ingestion failed", exc_info=True)
        raise AirflowException(
            f"Kafka ingestion failed: {exc}"
        ) from exc
    finally:
        consumer.close()

def build_training_dataset(**context):
    """
    Function to build the training dataset for fraud detection.
    This function should contain the logic to process ingested transactions and create
    a dataset suitable for model training, including feature engineering and data cleaning.
    """
    
    from training_dataset import TrainingDataset
    try:
        logger.info("Build the training dataset...")
        builder = TrainingDataset()
        object_name = builder.build_training_dataset(cutoff=datetime.now(timezone.utc))
        return {'status': 'success', 'object_name': object_name}
    except Exception as e:
        logger.error("Dataset build failed: %s", str(e), exc_info=True)
        raise AirflowException(f'Dataset build failed: {str(e)}') from e
    
with DAG(
    'fraud_detection_training',
    default_args=default_args,
    description='A DAG for training the fraud detection model',
    schedule=DAG_CONFIG["schedule"],
    catchup=False,
    tags=['fraud', 'ML'],
    max_active_runs=int(DAG_CONFIG["max_active_runs"]),
) as dag:

    validate_environment = BashOperator(
        task_id='validate_environment',
        bash_command='''
        echo "Validating environment..."
        test -f /app/config.yaml &&
        test -n "$AWS_ACCESS_KEY_ID" &&
        test -n "$AWS_SECRET_ACCESS_KEY" &&
        test -n "$KAFKA_USERNAME" &&
        test -n "$KAFKA_PASSWORD" &&
        echo "Environment validation successful." ||
        (echo "Environment validation failed. Configuration or credentials are missing." && exit 1)
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

    build_training_dataset_task = PythonOperator(
        task_id="build_training_dataset",
        python_callable=build_training_dataset,
    )

    training_task = PythonOperator(
        task_id='execute_training',
        python_callable=_train_model,
    )

    cleanup_task = BashOperator(
        task_id='cleanup',
        bash_command='echo "Cleaning up temporary files..." && rm -rf /app/tmp/*',
        trigger_rule='all_done'  # Ensure cleanup runs regardless of previous task outcomes
    )

    validate_environment >> ingestion_tasks
    ingestion_tasks >> build_training_dataset_task
    build_training_dataset_task >> training_task
    training_task >> cleanup_task

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

