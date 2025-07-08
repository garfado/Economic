from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
#from chinese_caixin_services_index_dag import main
from chinese_caixin_services_index_etl import main

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='chinese_caixin_services_index_etl',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval='@monthly',
    catchup=False,
    tags=['etl', 'caixin', 'services'],
) as dag:
    etl_task = PythonOperator(
    task_id='run_chinese_caixin_services_index_etl_script',
    python_callable=main,
    execution_timeout=timedelta(minutes=30)  # Ajuste conforme necessário
    )
