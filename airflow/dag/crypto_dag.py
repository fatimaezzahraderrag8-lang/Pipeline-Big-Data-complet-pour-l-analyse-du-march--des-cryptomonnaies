from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess
import os

BASE_DIR = "/opt/airflow/project"

def ingest_bronze():
    subprocess.run(
        ["python", f"{BASE_DIR}/bronze/ingest_bronze.py"],
        check=True
    )

def transform_silver():
    subprocess.run(
        ["python", f"{BASE_DIR}/silver/transform_silver.py"],
        check=True
    )

def build_gold():
    subprocess.run(
        ["python", f"{BASE_DIR}/gold/build_gold.py"],
        check=True
    )

def load_snowflake():
    subprocess.run(
        ["python", f"{BASE_DIR}/snowflake/snowflake_loader.py"],
        check=True
    )

with DAG(
    dag_id="crypto_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    bronze_task = PythonOperator(
        task_id="ingest_bronze",
        python_callable=ingest_bronze,
    )

    silver_task = PythonOperator(
        task_id="transform_silver",
        python_callable=transform_silver,
    )

    gold_task = PythonOperator(
        task_id="build_gold",
        python_callable=build_gold,
    )

    snowflake_task = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_snowflake,
    )

    bronze_task >> silver_task >> gold_task >> snowflake_task