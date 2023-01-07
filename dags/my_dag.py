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
from groups.process_tasks import process_tasks

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


default_args = {
    "start_date": datetime(2021,1,1)
}
@dag(description= "DAG in charge of processing customer data",
        default_args=default_args, schedule_interval="@daily",
        dagrun_timeout=timedelta(minutes=10), tags=["data_science"],
        catchup=False, max_active_runs=1)
def my_dag():
    
    partner_settings = extract()
    process_tasks(partner_settings)
     
my_dag()

    

        
