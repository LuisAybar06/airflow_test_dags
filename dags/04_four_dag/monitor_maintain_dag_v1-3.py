import sys
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.utils.email import send_email

PATH_COMMON = '../'
sys.path.append(PATH_COMMON)

from common.add_task import task_monitoringmodel, task_trainmodel

def success_email(context):
    task_instance = context['task_instance']
    task_status = 'Success' 
    subject = f'Airflow Task {task_instance.task_id} {task_status}'
    body = f'The task {task_instance.task_id} completed with status : {task_status}. \n\n'\
        f'The task execution date is: {context["execution_date"]}\n'\
        f'Log url: {task_instance.log_url}\n\n'
    to_email = 'a.luisaybar@gmail.com' #recepient mail
    send_email(to = to_email, subject = subject, html_content = body)

def failure_email(context):
    task_instance = context['task_instance']
    task_status = 'Failed'
    subject = f'Airflow Task {task_instance.task_id} {task_status}'
    body = f'The task {task_instance.task_id} completed with status : {task_status}. \n\n'\
        f'The task execution date is: {context["execution_date"]}\n'\
        f'Log url: {task_instance.log_url}\n\n'
    to_email = 'a.luisaybar@gmail.com' #recepient mail
    send_email(to = to_email, subject = subject, html_content = body)

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2024, 3, 17),
    'schedule_interval' : 'None',
    'email_on_failure': True,
    'email_on_success': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}


@dag(
    dag_id='monitor_maintain_dag_v1-3',
    default_args = default_args,
    start_date=datetime(2022, 1, 1),
    on_failure_callback = lambda context: failure_email(context),
    on_success_callback = lambda context: success_email(context),
    schedule=None
)
def mydag():
    train_task = task_trainmodel()
    monitoring_task = task_monitoringmodel()

    train_task >> monitoring_task


first_dag = mydag()