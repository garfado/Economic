from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from bloomberg_commodity_index_etl import etl  # Certifique-se que o arquivo ETL está presente

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='bloomberg_commodity_index_etl',
    default_args=default_args,
    start_date=datetime(2025, 7, 7),
    schedule='@monthly',
    catchup=False,
    tags=['etl', 'bloomberg', 'commodity'],
) as dag:
    etl_task = PythonOperator(
        task_id='run_bloomberg_commodity_index_script',
        python_callable=etl
    )
