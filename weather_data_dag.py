# Import the libraries
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'Team 5',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'weather_history_etl',
    default_args=default_args,
    description='ETL pipeline for weatherHistory data',
    schedule='@daily',
)

# File paths
csv_file_path = '/home/ubuntu/airflow/datasets/weatherHistory.csv'
db_path = '/home/ubuntu/airflow/databases/weather_data.db'

# Task 1: Extract data
def extract_data(**kwargs):
    kwargs['ti'].xcom_push(key='csv_file_path', value=csv_file_path)

extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_data,
    dag=dag,
)

# Task 2: Transform data
def transform_data(**kwargs):
    file_path = kwargs['ti'].xcom_pull(key='csv_file_path')
    df = pd.read_csv(file_path)

    # Data Cleaning - Convert Formatted Date to a proper date format.
    df['Formatted Date'] = pd.to_datetime(df['Formatted Date'])

     # Handle missing values in critical columns
    df['Temperature (C)'].fillna(df['Temperature (C)'].mean(), inplace=True)
    df['Humidity'].fillna(df['Humidity'].mean(), inplace=True)
    df['Wind Speed (km/h)'].fillna(df['Wind Speed (km/h)'].mean(), inplace=True)

    # Check for duplicates and remove them if necessary
    df.drop_duplicates(inplace=True)    


