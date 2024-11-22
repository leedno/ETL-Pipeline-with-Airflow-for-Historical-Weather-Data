from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from extract import extract_data
from transform import transform_data
from load import load_data  # This is where we'll load data into SQLite

# Define the DAG
dag = DAG(
    'weather_etl_pipeline',
    description='ETL pipeline for historical weather data',
    schedule_interval=None,  # Change this based on when you want it to run
    start_date=datetime(2024, 11, 22),
    catchup=False,
)

# Define the extraction task
extract_data_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag
)

# Define the transformation task
transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag
)

# Define the data loading task
load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag
)

# Set task dependencies
extract_data_task >> transform_data_task >> load_data_task
