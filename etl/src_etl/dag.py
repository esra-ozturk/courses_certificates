from datetime import timedelta,datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

from etl import etl_main

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 3, 14,8),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'certificates_dag',
    default_args=default_args,
    description='Certificates of courses!',
    schedule_interval=timedelta(days=1),
)

etl = PythonOperator(
    task_id='courses_certificates',
    python_callable=etl_main,
    dag=dag,
)
etl