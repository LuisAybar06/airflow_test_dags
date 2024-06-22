import sys
from airflow.decorators import dag, task
from datetime import datetime

PATH_COMMON = '../'
sys.path.append(PATH_COMMON)

from common.add_task import task_monitoringmodel


@dag(
    dag_id='monitor_maintain_dag_v1-2',
    start_date=datetime(2022, 1, 1),
    schedule=None
)
def mydag():
    
    task_monitoringmodel()


first_dag = mydag()