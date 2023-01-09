from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import task , dag
#from typing import Dict
from airflow.operators.subdag import SubDagOperator
#from subdags.subdag_factory import subdag_factory
from datetime import datetime , timedelta
from airflow.utils.task_group import TaskGroup
from groups.process_tasks import process_tasks
from airflow.operators.dummy import DummyOperator
import time


default_args = {
    "start_date": datetime(2021,1,1)
}
@dag(description= "DAG in charge of processing customer data",
        default_args=default_args, schedule_interval="@daily",
        dagrun_timeout=timedelta(minutes=10), tags=["data_science"],
        catchup=False, max_active_runs=1)


#def _choosing_partner_based_on_day(execution_date):
        #day = execution_date.day_of_week
        #if (day == 1):
            #return 'extract_partner_snowflake'
        #if (day == 3):
            #return 'extract_partner_netflix'
        #if (day == 5):
            #return 'extract_partner_astronomer'
        #return stop


def my_dag():

    partners = {

    "partner_snowflake":
    {
       "name": "snowflake",
       "path": "/partners/snowflake",
       "priority": 2
    },
    "partner_netflix":
    {
       "name": "netflix",
       "path": "/partners/netflix",
       "priority": 3
    },
    "partner_astronomer":
    {
       "name": "snowflake",
       "path": "/partners/astronomer",
       "priority": 1
    }

    




}
    start = DummyOperator(task_id="start", trigger_rule='dummy' , pool='default_pool')

    """choosing_partner_based_on_day = BrachPythonOperator(
        task_id = 'choosing_partner_based_on_day',
        python_callable = _choosing_partner_based_on_day
    )

    stop = DummyOperator(task_id='stop')"""

    storing = DummyOperator(task_id='storing', trigger_rule='none_failed_or_skipped')
    
    #choosing_partner_based_on_day >> stop

    for partners, details in partners.items():

        @task.python(task_id=f"extract_{partners}", priority_weight=details['priority'],do_xcom_push=False, pool='partner_pool',multiple_outputs=True)
        def extract(partner_name,partner_path):
            time.sleep(3)
            return {"partner_name": partner_name,"partner_path": partner_path}
        extracted_values = extract(details['name'], details['path'])
        start >> extracted_values 
        process_tasks(extracted_values ) >> storing


   
my_dag()

    

        
