import sys
import os

# Add the scripts folder to Python path
SCRIPTS_PATH = os.path.join(os.path.dirname(__file__), '..', 'scripts')
if SCRIPTS_PATH not in sys.path:
    sys.path.append(SCRIPTS_PATH)

# Import your ETL functions
from ingest_minio import ingest_data
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="olist_pipeline",
    start_date=datetime(2025, 9, 4),
    schedule="@daily",  # <- changed from schedule_interval
    default_args=default_args,
    catchup=False,
    tags=["staging", "minio", "etl"]
) as dag:

    start = EmptyOperator(task_id="start")

    ingest_task = PythonOperator(
        task_id="ingest_data_to_minio",
        python_callable=ingest_data
    )

    end = EmptyOperator(task_id="end")

    start >> ingest_task >> end
