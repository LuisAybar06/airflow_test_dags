import os
import pytz
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Obtener la zona horaria deseada
TZ = os.getenv('TZ')
timezone = pytz.timezone(TZ)

default_args = {
    'owner': 'datapath',
    # 'retries': 5,
    # 'retry_delay': timedelta(minutes=5)
}

def get_name(ti):
    ti.xcom_push(key='first_name', value='Jerry')
    ti.xcom_push(key='last_name', value='Fridman')

def get_age(ti):
    ti.xcom_push(key='age', value=19)

def greet(some_dict, ti):
    print("some dict: ", some_dict)
    first_name = ti.xcom_pull(task_ids='get_name', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_name', key='last_name')
    age = ti.xcom_pull(task_ids='get_age', key='age')
    print(f"Hello World! My name is {first_name} {last_name}, "
          f"and I am {age} years old!")
    

with DAG(
    default_args=default_args,
    dag_id='our_second_dag_v1.2',
    description='Our first dag using python operator',
    start_date=datetime(2023, 6, 26, tzinfo=timezone),
    schedule_interval='@once'
) as dag:
    
    taskA = PythonOperator(
        task_id='get_name',
        python_callable=get_name
    )

    taskB = PythonOperator(
        task_id='get_age',
        python_callable=get_age
    )

    taskC = PythonOperator(
        task_id='greet',
        python_callable=greet
    )

    [taskA, taskB] >> taskC