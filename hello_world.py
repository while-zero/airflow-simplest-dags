from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# author - Piotrek Janeczek

with DAG(
    dag_id="jnk_hello_World",
    start_date=datetime(2022,7,25),
    schedule_interval='@hourly',
    catchup=False
) as dag:

    def say_hi() -> None:
        print('hello world from airflow dag')

    first_task = PythonOperator(
        python_callable=say_hi,
        task_id='print_hello'
    )

    first_task
