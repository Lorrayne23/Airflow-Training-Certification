from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import task , dag
#from typing import Dict
from airflow.operators.subdag import SubDagOperator
#from subdags.subdag_factory import subdag_factory
from datetime import datetime , timedelta
from airflow.utils.task_group import TaskGroup

"""class CustomPostgresOperator(PostgresOperator):
    template_fields = ('sql', 'parameters')

    def _extract(partner_name):
    partner_settings = Variable.get("my_dag_partner", deserialize_json=True)
    name = partner_settings["name"]
    api_key = partner_settings["api_key"]
    #ath = partner_settings["path"]
    print(partner_name)"""

@task.python(task_id="extract_partners",do_xcom_push=False,multiple_outputs=True)
def extract():
    partner_name = "netflix"
    partner_path = "/partners/netflix"
    #ti.xcom_push(key="partner_name", value=partner_name)
    return {"partner_name": partner_name,"partner_path": partner_path}
    #return partner_name

@task.python
def process_a(partner_name,partner_path ):
    print(partner_name)
    print(partner_path)

@task.python
def process_b(partner_name,partner_path):
    print(partner_name)
    print(partner_path)

@task.python
def process_c(partner_name,partner_path):
    print(partner_name)
    print(partner_path)

default_args = {
    "start_date": datetime(2021,1,1)
}
@dag(description= "DAG in charge of processing customer data",
        default_args=default_args, schedule_interval="@daily",
        dagrun_timeout=timedelta(minutes=10), tags=["data_science"],
        catchup=False, max_active_runs=1)
def my_dag():
    #extract() >> process()
    #process(extract())
    partner_settings = extract()
    
    with TaskGroup(group_id='process_tasks') as process_tasks:
        process_a(partner_settings['partner_name'], partner_settings['partner_path'])
        process_b(partner_settings['partner_name'], partner_settings['partner_path'])
        process_c(partner_settings['partner_name'], partner_settings['partner_path'])

    """process_tasks = SubDagOperator(
        task_id="process_tasks",
        subdag=subdag_factory("my_dag", "process_tasks" , default_args) 
    )"""

    #extract()>> process_tasks
    
    """extract = PythonOperator(
            task_id = "extract",
            python_callable = _extract,
            #op_args = ["{{var.json.my_dag_partner.name }}"]
        )

        process = PythonOperator(
            task_id = "process",
            python_callable = _process,
        )

       

        featching_data = CustomPostgresOperator (
            task_id="featching_data",
            sql = "sql/MY_REQUEST.sql",
            parameters = {
                'next_ds': '{{next_ds}}',
                'prev_ds':  '{{prev_ds}}',
                'partner_name': '{{var.json.my_dag_partner.name}}',
            }

        )  
        """
my_dag()

    

        
