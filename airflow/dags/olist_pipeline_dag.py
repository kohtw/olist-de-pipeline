# Get path
import sys
import os
SCRIPTS_PATH = os.path.join(os.path.dirname(__file__), '..', 'scripts')
if SCRIPTS_PATH not in sys.path:
    sys.path.append(SCRIPTS_PATH)

# Imports
from ingest_minio import ingest_data
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
}

with DAG(
    dag_id="olist_pipeline",
    start_date=datetime(2025, 9, 4),
    schedule=None,
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
