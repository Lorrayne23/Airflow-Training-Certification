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
from airflow.sensors.date_time import DateTimeSensor
from airflow.exceptions import AirflowTaskTimeout, AirflowSensorTimeout


default_args = {
    "start_date": datetime(2021,1,1),
    "retries": 0
}


def _success_callback(context):
    print(context)

def _failure_callback(context):
    print(context)

def _extract_callback_sucess(context):
    print('SUCESS CALLBACK')

def _extract_callback_failure(context):
    """if(context['exception']):
        if(isinstance (context['exception'], AirflowTaskTimeout)):
        if(isinstance (context['exception'], AirflowTSensorTimeout)):"""
    print('FAILURE CALLBACK')

def _extract_callback_retry(context):
    #if(context['ti'].try_number() > 2 ):
    print(' RETRY CALLBACK')


@dag(description= "DAG in charge of processing customer data",
        default_args=default_args, schedule_interval="@daily",
        dagrun_timeout=timedelta(minutes=10), tags=["data_science"],
        catchup=False,on_success_callback=_success_callback, on_failure_callback=_failure_callback,max_active_runs=1)


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
       "priority": 2,
       "pool": "snowflake"
    },
    "partner_netflix":
    {
       "name": "netflix",
       "path": "/partners/netflix",
       "priority": 3,
       "pool": "netflix"
    },
    "partner_astronomer":
    {
       "name": "snowflake",
       "path": "/partners/astronomer",
       "priority": 1,
       "pool": "astronomer"
    }    

}

    start = DummyOperator(task_id="start", trigger_rule='dummy' , pool='default_pool', execution_timeout=timedelta(minutes=10))

    delay = DateTimeSensor(
        task_id='delay',
        target_time="{{execution_date.add(hours=9)}}",
        poke_interval=60 * 60,
        mode='reschedule',
        timeout=60* 60 * 10,
        #execution_timeout=
        soft_fail=True,
        exponential_backoff=True

    )

    """choosing_partner_based_on_day = BrachPythonOperator(
        task_id = 'choosing_partner_based_on_day',
        python_callable = _choosing_partner_based_on_day
    )

    stop = DummyOperator(task_id='stop')"""

    storing = DummyOperator(task_id='storing', trigger_rule='none_failed_or_skipped')
    
    #choosing_partner_based_on_day >> stop

    for partners, details in partners.items():

        @task.python(task_id=f"extract_{partners}",retries=3,retry_delay=timedelta(minutes=5 ),
                     retry_exponential_backoff=True,max_retry_delay=timedelta(minutes=15),
                     on_success_callback=_extract_callback_sucess,on_failure_callback=_extract_callback_failure, on_retry_callback=_extract_callback_retry,depends_on_past=True, priority_weight=details['priority'], do_xcom_push=False, pool=details['pool'],multiple_outputs=True)
        def extract(partner_name,partner_path):
            time.sleep(3)
            raise ValueError("failed")
            return {"partner_name": partner_name,"partner_path": partner_path}
        extracted_values = extract(details['name'], details['path'])
        start >> extracted_values 
        process_tasks(extracted_values ) >> storing


   
my_dag()

    

        
