# -*- coding: utf-8 -*-
import time
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime
import json

def send_to_kafka_for_test():
    from kafka import KafkaProducer
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    data = {
        'id': time.time(),
        'name': 'test',
        'chuyen_mon': 'test',
        'mo_ta_cong_viec': 'test',
        'yeu_cau_cong_viec': 'test',
        'quyen_loi': 'test',
        'dia_diem_lam_viec': 'test',
        'thoi_gian_lam_viec': 'test',
        'cach_thuc_ung_tuyen': 'test'
    }
    producer.send('recruitment_information', json.dumps(data).encode('utf-8'))
    time.sleep(3)

default_args = {
    'owner': 'vancuong',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025,5,12),
}

with DAG('stream_recruitment_information_test',
         default_args=default_args,
         description = "This is kafka stream task.",
         schedule_interval='@daily',
         catchup=False) as dag:

    send_to_kafka_for_test= PythonOperator(
        task_id='send_to_kafka',
        python_callable=send_to_kafka_for_test,
        provide_context=True
    )

    send_to_kafka_for_test