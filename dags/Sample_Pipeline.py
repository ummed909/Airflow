from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_argumets = {
    'owner':'Ummed Choudhary',
    'start_date':days_ago(1),
    'depends_on_the_past':False,
    'email':['ummedchoudhary93275@gmail.com'],
    'email_on_failure':True,
    'email_on_retry':True,
    'retries':1
}

with DAG(
    dag_id='Hello_dag',
    default_args=default_argumets,
    description='This is very simple Dag to practice and understand the flow',
    schedule_interval=None,
    tags=['sample', 'practice', 'ummed']
) as dag:

    # task 1 : say Hello using the bashOperator
    say_hello_task = BashOperator(
        task_id='say_hello',
        bash_command='echo "Hello Airflow, I am Ummed ------------------"'
    )

    # task 2 : get the current date using bashOperator
    get_date = BashOperator(
        task_id='get_date',
        bash_command='date',
    )

    # task 3 : confirmation
    def last_func():
        print('All task is completed, you get it ------------------>>>>>>>>')
    everything_good = PythonOperator(
        task_id='everything_good',
        python_callable=last_func,
    )

    # dependecies :
    [say_hello_task, get_date] >> everything_good