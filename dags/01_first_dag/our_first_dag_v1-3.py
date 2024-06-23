import os, sys
import pytz
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models.param import Param

# Obtener la zona horaria deseada
TZ = os.getenv('TZ')
timezone = pytz.timezone(TZ)
 

default_args = {
    'owner': 'datapath'
}

with DAG(
    dag_id='our_first_dag_v1.3',
    default_args=default_args,
    description='DAG de prueba, obtenemos las argumentos de entrada',
    start_date=datetime(2023, 6, 25, tzinfo=timezone),
    schedule_interval='0 8 * * *', #'@daily'
    catchup=False,
    params={"name": "Data Path", "project": "Airflow Test Project"}

) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo hello world, this is the first task! [{{ dag_run.conf }}]"
    )

    task2 = BashOperator(
        task_id='second_task',
        bash_command="echo hey, I am task2 and will be running after task1! [$name]",
        env={
            "name": '{{ dag_run.conf["name"] if dag_run else "" }}'
        }
    )

    task3 = BashOperator(
        task_id='thrid_task',
        bash_command="echo hey, I am task3 and will be running after task1 at the same time as task2!"
    )

    # Task dependency method 1
    # task1.set_downstream(task2)
    # task1.set_downstream(task3)

    # Task dependency method 2
    # task1 >> task2
    # task1 >> task3

    # Task dependency method 3
    task1 >> [task2, task3]
