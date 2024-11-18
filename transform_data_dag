# Import libraries
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

# Task 2: Transform data 
def transform_data(**kwargs):
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info('Starting data transformation process')
    
    # Pull the file path from XCom
    file_path = kwargs['ti'].xcom_pull(key='csv_file_path')
    logging.info(f'Loaded data from file: {file_path}')
    
    # Load the CSV file into a DataFrame
    df = pd.read_csv(file_path)

    # Data Cleaning - Convert Formatted Date to a proper date format
    df['Formatted Date'] = pd.to_datetime(df['Formatted Date'])

    # Handle missing values in critical columns using the mean of the column
    df['Temperature (C)'].fillna(df['Temperature (C)'].mean(), inplace=True)
    df['Humidity'].fillna(df['Humidity'].mean(), inplace=True)
    df['Wind Speed (km/h)'].fillna(df['Wind Speed (km/h)'].mean(), inplace=True)
    logging.info('Completed missing value handling')

    # Handle erroneous values using IQR and replacing with mean (without restricting negative values)
    for col in ['Temperature (C)', 'Humidity', 'Wind Speed (km/h)']:
        # Calculate IQR (Interquartile Range)
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR

        # Replace outliers with column mean
        col_mean = df[col].mean()
        df[col] = df[col].apply(lambda x: col_mean if x < lower_bound or x > upper_bound else x)
    
    logging.info('Completed outlier handling')

    # Add a new column for wind strength categorization based on wind speed
    def categorize_wind_speed(wind_speed):
        if wind_speed < 10:
            return 'Light'
        elif wind_speed < 30:
            return 'Moderate'
        else:
            return 'Strong'
    
    df['wind_strength'] = df['Wind Speed (km/h)'].apply(categorize_wind_speed)
    logging.info('Created wind strength categorization')

    # Calculate daily averages for temperature, humidity, and wind speed
    df['Date'] = df['Formatted Date'].dt.date
    daily_averages = df.groupby('Date')[['Temperature (C)', 'Humidity', 'Wind Speed (km/h)']].mean().reset_index()

    # Group data by month and calculate the mode of Precip Type for each month
    df['Month'] = df['Formatted Date'].dt.month
    mode_precip = df.groupby('Month')['Precip Type'].agg(lambda x: x.mode()[0] if not x.mode().empty else np.nan).reset_index()
    mode_precip.rename(columns={'Precip Type': 'Mode'}, inplace=True)

    # Merge the mode data with the daily averages (based on month)
    daily_averages['Month'] = pd.to_datetime(daily_averages['Date']).dt.month
    daily_averages = daily_averages.merge(mode_precip, on='Month', how='left')
    logging.info('Completed data transformations')

    # Save the transformed data to a new CSV file
    daily_averages.to_csv('./airflow/datasets/transformed_weather_data.csv', index=False)
    logging.info('Saved transformed data to CSV')

    # Push transformed data to XCom for validation step
    kwargs['ti'].xcom_push(key='transformed_data', value=daily_averages)
    logging.info('Pushed transformed data to XCom')

# Define the DAG
default_args = {
    'owner': 'Team 5',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'transform_data_etl',
    default_args=default_args,
    description='ETL pipeline for windstrength and cleaning',
    schedule='@daily',
)

# Task to transform data
transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)
