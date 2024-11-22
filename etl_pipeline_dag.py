from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from extract import extract_data
from transform import transform_data
from validate import validate_data
from load_data import load_data  # Import the load_data function

# Define the DAG
dag = DAG(
    'weather_etl_pipeline',
    description='ETL pipeline for historical weather data',
    schedule_interval=None,  # Change as needed
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

# Define the validation task
validate_data_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    provide_context=True,
    dag=dag
)

# Define the load data task
load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag
)

# Set task dependencies
extract_data_task >> transform_data_task >> validate_data_task >> load_data_task
