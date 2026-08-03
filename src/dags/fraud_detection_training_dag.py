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

default_args = {
    'owner': 'fraud_detection_team.com',
    'depends_on_past': False,
    'start_date': datetime(2026, 8, 1),
    'email_on_failure': False,
    'execution_timeout': timedelta(minutes=120),
    'max_active_runs': 1,
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
        return {'status': 'success'}

    except Exception as e:
        logger.error("Model training failed: %s", str(e), exc_info=True)
        raise AirflowException(f'Model Training failed: {str(e)}') from e
    
with DAG(
    'fraud_detection_training',
    default_args=default_args,
    description='A DAG for training the fraud detection model',
    schedule="0 3 * * *",  # Every day at 03:00
    catchup=False,
    tags=['fraud', 'ML']
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

    training_task = PythonOperator(
        task_id='execute_training',
        python_callable=_train_model,
    )

    cleanup_task = BashOperator(
        task_id='cleanup',
        bash_command='echo "Cleaning up temporary files..." && rm -rf /app/tmp/*',
        trigger_rule='all_done'  # Ensure cleanup runs regardless of previous task outcomes
    )

    validate_environment >> training_task >> cleanup_task

    # Documentation 
    dag.doc_md = """
    # Fraud Detection Model Training DAG
    This DAG is responsible for training the fraud detection model. It performs the following steps:
    1. **Validate Environment**: Checks for the presence of required configuration files.
    2. **Execute Training**: Runs the model training process.
    3. **Cleanup**: Cleans up temporary files after training.
    Daily Training of fraud detection use: 
    - Transactions data from Kafka
    - XGBoost classifier with precision optimisation
    - MLFLOW for experiment tracking and model versioning
    """