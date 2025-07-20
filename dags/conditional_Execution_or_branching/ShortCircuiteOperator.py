from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator
from datetime import datetime, timedelta
import random


default_args = {
    'owner': 'ummed_choudhary',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    default_args=default_args,
    dag_id='ShortCircuiteOperator',
    schedule_interval=None,
    start_date=datetime(2023,1,1),
    catchup=False,
    tags=["ummed", "ShortCircuiteOperator", 'Example'],
) as dag:

    # logic for running downstream or not
    def _run_downstream_or_not():
        """
        this function will decide weather to run the downstram or not
        :return: True/ False
        """

        res = random.choice([True, False])
        print(f"Decission of downstrem to run or not : {res}")
        return res

    #implement the short circuite

    decide_to_run_task = ShortCircuitOperator(
        task_id='ShortCircuiteOperator',
        python_callable=_run_downstream_or_not,
    )

    # define the first task

    task_1=BashOperator(
        task_id="Task_1",
        bash_command='echo "This is the First task of this dag"',
    )

    task_2=BashOperator(
        task_id="Task_2",
        bash_command='echo "This is the secound task : down stream"'
    )

    task_3 = BashOperator(
        task_id="Task_3",
        bash_command='echo "This is the third task : down stream"'
    )

    task_4 = BashOperator(
        task_id="Task_4",
        bash_command='echo "This is the final task"'
    )

    task_1 >> decide_to_run_task >> task_2 >> task_3 >> task_4