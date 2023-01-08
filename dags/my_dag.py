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




@task.python(task_id="extract_partners",do_xcom_push=False,multiple_outputs=True)
def extract(partner_name,partner_path):
    return {"partner_name": partner_name,"partner_path": partner_path}
 


default_args = {
    "start_date": datetime(2021,1,1)
}
@dag(description= "DAG in charge of processing customer data",
        default_args=default_args, schedule_interval="@daily",
        dagrun_timeout=timedelta(minutes=10), tags=["data_science"],
        catchup=False, max_active_runs=1)


def my_dag():

    partners = {

    "partner_snowflake":
    {
       "name": "snowflake",
       "path": "/partners/snowflake"
    },
    "partner_netflix":
    {
       "name": "netflix",
       "path": "/partners/netflix"
    },
    "partner_astronomer":
    {
       "name": "snowflake",
       "path": "/partners/astronomer"
    }

}

    for partners, details in partners.items():
        process_tasks(extract(details['name'], details['path']))


   
my_dag()

    

        
