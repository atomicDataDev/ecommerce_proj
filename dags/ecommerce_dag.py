from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ecommerce_daily_report',
    default_args=default_args,
    description='Daily ETL pipeline activation ao 02:00',
    schedule='0 2 * * *',
    start_date = datetime(2026, 5, 28),
    catchup=False,
    tags=['ecommerce', 'etl']
) as dag:
    run_etl_pipeline = BashOperator(
        task_id='execute_main_py',
        bash_command='cd /opt/airflow/project && python main.py'
    )

    run_etl_pipeline