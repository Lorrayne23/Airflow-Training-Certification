from airflow.operators.dummy import DummyOperator
from airflow import DAG

from datetime import datetime

default_args = {
    "start_date": datetime(2021,1,1)
}

with DAG('dependency', schedule_interval='@daily',
default_args=default_args,catchup=False) as dag:
    t1 = DummyOperator(task_id="t1")
    t2 = DummyOperator(task_id="t2")
    t3 = DummyOperator(task_id="t3")
    t4 = DummyOperator(task_id="t4")
    t5 = DummyOperator(task_id="t5")
    t6 = DummyOperator(task_id="t6")

    t2.set_upstream(t1)
    t1.set_downstream(t2)