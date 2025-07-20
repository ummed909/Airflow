from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

# default_arguments of dag
default_args = {
    'owner':'ummedchoudhary',
    'depends_on_past':False,
    'email_on_failure':True,
    'email_on_retry':True,
    'retries':1,
}

# Dag
with DAG(
    dag_id='Schedule_date_pipeline',
    description='Learning about date and time in pipeline and how to schedule pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=days_ago(5),
    catchup=False,
    tags = ['date and time', 'scheduling', 'ummed'],
):
    # Task 1 : check schedule and logical dates
    check_schedule_task = BashOperator(
        task_id='check_schedule_task',
        bash_command='''
        echo "---------------- DAG run details----------------------"
        echo "Logical Date (ds) : {{ds}}"
        echo "logical date (date_interval_end) : {{date_interval_end}}"
        echo "Execution Date (deprecated name for logical date): {{ execution_date }}"
        echo "Run ID: {{ run_id }}"
        echo "Data Interval Start: {{ data_interval_start }}"
        echo "Current Time (task run time): $(date '+%Y-%m-%d %H:%M:%S')"
        '''
    )
