from airflow import DAG
from airflow.example_dags.tutorial_dag import transform_task, load_task
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import sqlalchemy

# db_url
database_url = DB_URI = "postgresql://postgres:ummed@localhost:5432/DataEng"

# data source url
source_url = "https://jsonplaceholder.typicode.com/todos/1"

# ETL functoins

def extract():
    try:
        response = requests.get(source_url, timeout=10)  # ⏱ Timeout added
        data = response.json()
        print(f"Fetched {len(data)} items from API.")
        df = pd.DataFrame(data)
        df.to_csv('/tmp/raw_products.csv', index=False)
    except Exception as e:
        print(f"Error during extraction: {e}")
        raise

def transform():
    clean_dataFrame = pd.read_csv('tmp/raw_data.csv')
    clean_dataFrame = clean_dataFrame[['id', 'title', 'price', 'category']].dropna()
    clean_dataFrame.to_csv('tmp/clean_data.csv', index=False)

def load_data():
    data = pd.read_csv('tmp/clean_data.csv')
    engine = sqlalchemy.create_engine(database_url)
    data.to_sql('Products', engine, if_exists='replace', index=False)


# ---------------------- Air Flow ----------------------------------

with DAG(
    dag_id='ETL_Product_Pipeline',
    start_date=datetime(2024,1,1),
    schedule_interval= '@daily',
    catchup=False,
    tags = ['elt', 'api'],
) as dag:

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id='Load_task',
        python_callable = load_data
    )

    extract_task >> transform_task >> load_task # set dependencies





