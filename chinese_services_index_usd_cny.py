from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

def run_script():
    subprocess.run(
        ["python", "/opt/airflow/dags/chinese_services_index_usd_cny.py"],
        check=True
    )

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='chinese_services_index_usd_cny',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval='@monthly',
    catchup=False,
    tags=['etl', 'usd', 'cny'],
) as dag:
    etl_task = PythonOperator(
        task_id='chinese_services_index_usd_cny_script',
        python_callable=run_script
    )
