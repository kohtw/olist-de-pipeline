import os
from datetime import datetime
import subprocess

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task


TAGS = ['ecommerce-dbt']

with DAG(
    dag_id="olist_dbt",
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

    end = EmptyOperator(task_id="end")

    (start >> end)
