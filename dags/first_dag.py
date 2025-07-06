from airflow import DAG
from airflow.decorators import python_task
from airflow.example_dags.example_latest_only import task1
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests

source_url = "https://jsonplaceholder.typicode.com/todos/1"


def demo_func():
    print('Hello World, I am Ummed')
    print('First Pipeline')
    print('-----------------------------------------')
    try:
        response = requests.get(source_url)
        data = response.json()
        print(f"Fetched {len(data)} items from API.")
        print(type(data))
    except Exception as e:
        print(f"Error during extraction: {e}")
        raise
    print('---------------------------------------')

with DAG(
    dag_id='first_dag',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False ,
    tags=["example"]
) as dag:
    task1 = PythonOperator(
        task_id = 'demo_func',
        python_callable=demo_func
    )





