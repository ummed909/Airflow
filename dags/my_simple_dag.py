from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner':'Ummed Choudhary',
    'depends_on_past': False,
    'email' : ['ummedchoudhary93275@gmail.com'],
    'email_on_failure':True,
    'email_on_retry':True,
    'retries':1,
    'retry_delary':timedelta(minutes=5),
}

with DAG (
    default_args=default_args,
    dag_id='my_simple_dag',
    description='This is simple dag for just learning purpose',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1,1),
    catchup=False,
    tags=['Learning', 'simple', 'ummed'],
):
    # task 1 : print a greeting:
    start_task = BashOperator(
        task_id='start_task',
        bash_command='echo "I am Ummed , i am learning about the bash commands" ',
    )

    # task 2 : create a dumy file
    create_file_task = BashOperator(
        task_id='file_task',
        bash_command='touch /tmp/airflow_dumy_file.txt && echo "Dumy file created"',
    )

    # task 3: list all the files
    list_file_task = BashOperator(
        task_id='list_file_task',
        bash_command='ls -ls /tmp',
    )

    # task 4 : clean up the dumy files
    clean_up_task = BashOperator(
        task_id='clean_up_task',
        bash_command='rm /tmp/airflow_dumy_file.txt || echo "File is removed or not found"',
    )

    # create flow
    start_task >> create_file_task >> list_file_task >> clean_up_task
