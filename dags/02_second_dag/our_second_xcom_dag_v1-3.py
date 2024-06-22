import os
import pytz
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# Obtener la zona horaria deseada
TZ = os.getenv('TZ')
timezone = pytz.timezone(TZ)

default_args = {
    'owner': 'datapath',
    # 'retries': 5,
    # 'retry_delay': timedelta(minutes=5)
}

def get_name(ti, **kwargs):

    dag_run = kwargs.get('dag_run')
    dag_arg = dag_run.conf
    first_name = dag_arg.get('first_name', 'Rick')
    last_name = dag_arg.get('last_name', 'Hunter')

    ti.xcom_push(key='first_name', value=first_name)
    ti.xcom_push(key='last_name', value=last_name)

def get_age(ti, **kwargs):

    dag_run = kwargs.get('dag_run')
    dag_arg = dag_run.conf
    age = dag_arg.get('age', 18)

    var1 = Variable.get("AIRFLOW_VAR_1")
    var2_json = Variable.get("AIRFLOW_VAR_2", deserialize_json=True)
    var2 = var2_json.get('data').get('string')
    
    ti.xcom_push(key='var1', value=var1)
    ti.xcom_push(key='var2', value=var2)
    ti.xcom_push(key='age', value=age)
 
def greet(some_dict, ti):
    print("some dict: ", some_dict)
    first_name = ti.xcom_pull(task_ids='get_name', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_name', key='last_name')
    age = ti.xcom_pull(task_ids='get_age', key='age')
    print(f"Hello World! My name is {first_name} {last_name}, "
          f"and I am {age} years old!")
    

with DAG(
    default_args=default_args,
    dag_id='our_second_dag_v1.3',
    description='Python Operator - Airflow Variables & Dag Arguments',
    start_date=datetime(2023, 6, 27, tzinfo=timezone),
    schedule_interval='@daily'
) as dag:
    
    taskA = PythonOperator(
        task_id='get_name',
        python_callable=get_name,
        provide_context=True,
    )

    taskB = PythonOperator(
        task_id='get_age',
        python_callable=get_age,
        provide_context=True,
    )

    taskC = PythonOperator(
        task_id='greet',
        python_callable=greet,
        op_kwargs={'some_dict': {'a': 1, 'b': 2}}
    )

    [taskA, taskB] >> taskC