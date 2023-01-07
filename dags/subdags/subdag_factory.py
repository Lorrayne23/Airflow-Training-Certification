"""from airflow.models import DAG
from airflow.decorators import task 
from airflow.operators.python import get_current_context


@task.python
def process_a():
    ti = get_current_context()['ti']
    partner_name = ti.xcom_pull(key='partner_name', task_ids='extract_partners', dag_id='my_dag')
    partner_path = ti.xcom_pull(key='partner_path', task_ids='extract_partners', dag_id='my_dag')
    #partner_settings= ti.xcom_pull(task_ids="extract")
    #print(partner_settings['partner_name'])
    print(partner_name)
    print(partner_path)

@task.python
def process_b():
    ti = get_current_context()['ti']
    partner_name = ti.xcom_pull(key='partner_name', task_ids='extract_partners', dag_id='my_dag')
    partner_path = ti.xcom_pull(key='partner_path', task_ids='extract_partners', dag_id='my_dag')
    #partner_settings= ti.xcom_pull(task_ids="extract")
    #print(partner_settings['partner_name'])
    print(partner_name)
    print(partner_path)

@task.python
def process_c():
    ti = get_current_context()['ti']
    partner_name = ti.xcom_pull(key='partner_name', task_ids='extract_partners', dag_id='my_dag')
    partner_path = ti.xcom_pull(key='partner_path', task_ids='extract_partners', dag_id='my_dag')
    #partner_settings= ti.xcom_pull(task_ids="extract")
    #print(partner_settings['partner_name'])
    print(partner_name)
    print(partner_path)

def subdag_factory(parent_dag_id, subdag_dag_id, default_args):
    with DAG(f"{parent_dag_id}.{subdag_dag_id}", default_args=default_args) as dag:
        process_a()
        process_b()
        process_c()
    
    return dag
"""