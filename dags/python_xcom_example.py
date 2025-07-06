from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner':'Ummed Choudhary',
    'depends_on_past':False,
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay':timedelta(minutes=5),
}

with DAG(
    dag_id= 'Python_xcom_example',
    default_args=default_args,
    description='A example to learn about the x_com',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['x_com', 'learning', 'data_passing', 'ummed'],
):
    # function : generate data
    def _generate_data(ti):
        numbers = [10,20,30,40,50]
        print(f'generated data :{numbers}')
        ti.xcom_push(key='generated_data', value=numbers )
        print('data pushed to xcom with the key : generated_data')

    # task 1 :
    generate_data_task = PythonOperator(
        task_id='generated_data_task',
        python_callable=_generate_data,
        provide_context=True,
    )

    def process_data(ti):
        numbers = ti.xcom_pull(task_ids='generated_data_task', key = 'generated_data')
        print(f'data is pulled sucessfully, {numbers}')
        processed_data = sum(numbers)
        print(f"data is processed sucessfully {processed_data}")

        ti.xcom_push(key='proccesed_data', value=processed_data)
        print('processed data is pushed sucessfully')

    # task 2
    processed_data_task = PythonOperator(
        task_id = 'processed_data_task',
        python_callable=process_data,
        provide_context=True,
    )

    # task 3
    save_processed_data = BashOperator(
        task_id='save_procssed_data',
        bash_command='echo "Done ---------- >>>>>>>>>>>>>."'
    )

    generate_data_task >> processed_data_task >> save_processed_data




