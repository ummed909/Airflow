# brach example dag

from airflow import DAG
from airflow.api_connexion.schemas.provider_schema import provider_collection_schema
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import random


default_args = {
    'owner':"Ummed Choudhary",
    'depends_on_past':False,
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay': timedelta(minutes=3),
}

with DAG (
    dag_id='BranchPythonOperator',
    default_args=default_args,
    description="This is the example dag to learn about the branching",
    schedule_interval=None,
    start_date=datetime(year=2024, month=10, day=1),
    catchup=False,
    tags=['branching', 'BranchPythonOperator', 'conditional_execution'],
) as dag:

    # step:1 defile the branching logic function
    def _choose_path(**kwargs):
        """
        This function define the branching logic.
        It must return the task_id of the task to execute next
        """
        decision = random.choice(['path_a', 'path_b'])
        print(f'Decided to go down with :{decision}')
        return decision


    # step:2 BranchPythonOperator
    branch_taks = BranchPythonOperator(
        task_id='chosse_the_path',
        python_callable=_choose_path,
        provide_context = True
    )

    # step : 3 define the normal tasks,
    path_a_task = BashOperator(
        task_id='path_a',
        bash_command='echo "Executing the path a, {{ ds }}"'
    )

    path_b_task = BashOperator(
        task_id='path_b',
        bash_command='echo "Executing the path b, {{ ds }}"'
    )

    # steps: 4 Define the end task regardless of condiiton
    final_task = BashOperator(
        task_id='Final_task',
        bash_command='echo "this is the end the the dag"',
        trigger_rule='one_success'
    )

    # steps 5 : define dependencies
    # only one task is executed from the list , and one is skipped, and then the final task will run
    branch_taks >> [ path_a_task, path_b_task ]
    [path_a_task, path_b_task] >> final_task


