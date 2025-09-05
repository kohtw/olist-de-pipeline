# dags/olist_pipeline_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

from scripts.ingest_minio import ingest_data
from scripts.load_warehouse import load_data

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="olist_pipeline",
    start_date=datetime(2025, 9, 4),
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    tags=["staging", "minio", "etl", "spark"]
) as dag:

    start = EmptyOperator(task_id="start")

    ingest_task = PythonOperator(
        task_id="ingest_data_to_minio",
        python_callable=ingest_data
    )
    
    transform_task = BashOperator(
        task_id="spark_transform",
        bash_command="docker exec spark-app-container spark-submit /app/spark_transform.py"
    )
    
    load_task = PythonOperator(
        task_id="load_to_warehouse",
        python_callable=load_data
    )
    
    end = EmptyOperator(task_id="end")
    
    start >> ingest_task >> transform_task >> load_task >> end
