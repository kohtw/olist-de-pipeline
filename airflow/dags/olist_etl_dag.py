import os
from datetime import datetime
import subprocess

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.decorators import task

from ingest_minio import ingest_ecommerce_data
from minio_to_postgres import load_ecommerce_data

TAGS = ['ecommerce-ingest']

with DAG(
    dag_id="olist_ETL",
    start_date=datetime(2025, 9, 4),
    schedule=None,
    default_args= {
        "owner": "airflow",
        "depends_on_past": False,
    },
    catchup=False,
    tags=TAGS
) as dag:

    start = EmptyOperator(task_id="start")

    @task(task_id="ingest_task")
    def ingest_task():
        ingest_ecommerce_data()

    @task(task_id="transform_task")
    def transform_task():
        subprocess.run([
            "spark-submit",
            "--master",
            "spark://spark-master:7077",
            "/path/to/airflow/scripts/transform_data.py"
        ], check=True)

    @task(task_id="load_task")
    def load_task():
        load_ecommerce_data()

    trigger_next = TriggerDagRunOperator(
        task_id="trigger_dbt",
        trigger_dag_id="dbt_build",
        wait_for_completion=False,
    )

    end = EmptyOperator(task_id="end")

    (start >> ingest_task >> transform_task >> end)
