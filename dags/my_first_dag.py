from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='my_first_dag',
    default_args=default_args,
    description='DAG đầu tiên chạy trong Docker',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    task1 = BashOperator(
        task_id='print_date',
        bash_command='date'
    )

    task2 = BashOperator(
        task_id='say_hello',
        bash_command='echo "Hello from Cuong!"'
    )

    task1 >> task2
