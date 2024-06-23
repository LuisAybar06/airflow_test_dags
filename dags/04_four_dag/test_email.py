from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow import DAG

default_args = {
    "owner": "Airflow",
    "start_date": datetime(2022, 8, 21),
    'email': ['admin@localmail.com'],
    'email_on_failure': True,
}

with DAG(
    dag_id="email_test_dag",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False
) as dag:

    test_email = EmailOperator(
       task_id='email_test',
       to='a.luisaybar@gmail.com',
       subject='Airflow Alert !!!',
       html_content="""<h1>Testing Email using Airflow</h1>""",
    )

test_email
