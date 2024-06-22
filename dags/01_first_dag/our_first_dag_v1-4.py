import os, sys
import pytz
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable

# Obtener la zona horaria deseada
# TZ = os.getenv('TZ')
# timezone = pytz.timezone(TZ)

timezone = pytz.timezone('America/Lima')

VAR1 = Variable.get("AIRFLOW_VAR_1")
VAR2 = Variable.get("AIRFLOW_VAR_2", deserialize_json=True)
VAR2_STRING = VAR2.get('data').get('string')

default_args = {
    'owner': 'datapath',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='our_first_dag_v1.4',
    default_args=default_args,
    description='DAG de prueba, obtenemos las variables de entorno',
    start_date=datetime(2023, 6, 25, tzinfo=timezone),
    schedule_interval='0 8 * * *', #'@daily'
    catchup=False
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo hello world, this is the first task! [{{ var.value.AIRFLOW_VAR_1 }}]"
    )

    task2 = BashOperator(
        task_id='second_task',
        bash_command="echo hey, I am task2 and will be running after task1! [{{ var.json.AIRFLOW_VAR_2.data.string }}]",
    )

    task3 = BashOperator(
        task_id='thrid_task',
        bash_command="echo hey, I am task3 and will be running after task1 at the same time as task2! [$AIRFLOW_HOME]",
        append_env=False
    )

    # Task dependency method 1
    # task1.set_downstream(task2)
    # task1.set_downstream(task3)

    # Task dependency method 2
    # task1 >> task2
    # task1 >> task3

    # Task dependency method 3
    task1 >> [task2, task3]
