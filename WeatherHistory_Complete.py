from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import numpy as np
from airflow.models import TaskInstance
import sqlite3
import pyarrow
#from data_extract import extract_data
#from data_transform import transform_data
#from data_validate import validate_data
#from data_loading import load_data 


# Define the DAG
dag = DAG(
    'weather_history_pipeline',
    description='ETL pipeline for historical weather data',
    schedule_interval=None, 
    start_date=datetime(2024, 11, 22),
    catchup=False,
)

# Define the extraction task
# Task 1: Extract data
def extract_data(**kwargs):
    """
    Extracts the weatherHistory.csv dataset.
    Pushes the file path to XCom.
    """
    csv_file_path = '/home/ubuntu/airflow/datasets/weatherHistory.csv'
    kwargs['ti'].xcom_push(key='csv_file_path', value=csv_file_path)


extract_data_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag
)

# Define the transformation task
def transform_data(task_instance):
    # Step 1: Load the dataset
    file_path = task_instance.xcom_pull(key='csv_file_path')
    df = pd.read_csv(file_path)


    # Step 2: Convert 'Formatted Date' to datetime, accounting for timezone information
    print("Starting data transformation process")
    try:
        # Attempt to parse the 'Formatted Date' column into a proper datetime format (including timezone)
        df['Formatted Date'] = pd.to_datetime(df['Formatted Date'], errors='coerce', utc=True)

        # Step 3: Check for invalid datetime rows
        invalid_rows = df[df['Formatted Date'].isna()]
        if not invalid_rows.empty:
            print(f"Found {len(invalid_rows)} invalid rows in 'Formatted Date':")
            print(invalid_rows)
        else:
            print("No invalid date rows found.")

        # Step 4: Drop rows with invalid 'Formatted Date' values (NaT)
        df = df.dropna(subset=['Formatted Date'])

        # Step 5: Ensure that the 'Formatted Date' column is now in the correct datetime type
        print("Data type of 'Formatted Date' after conversion:", df['Formatted Date'].dtype)

        # Step 6: Extract date (without time) from 'Formatted Date' column
        df['Date'] = df['Formatted Date'].dt.date

        # Extract additional date components for grouping
        df['Year'] = df['Formatted Date'].dt.year
        df['Month'] = df['Formatted Date'].dt.month
        df['Day'] = df['Formatted Date'].dt.day
        df['Hour'] = df['Formatted Date'].dt.hour

        # Step 7: Handle missing data (if needed), example: fill missing 'Temperature (C)' with the column mean
        df['Temperature (C)'].fillna(df['Temperature (C)'].mean(), inplace=True)
        df['Humidity'].fillna(df['Humidity'].mean(), inplace=True)
        df['Wind Speed (km/h)'].fillna(df['Wind Speed (km/h)'].mean(), inplace=True)
        df['Visibility (km)'].fillna(df['Visibility (km)'].mean(), inplace=True)
        df['Pressure (millibars)'].fillna(df['Pressure (millibars)'].mean(), inplace=True)

        # Step 8: Remove duplicates
        df.drop_duplicates(inplace=True)

        # ----------------------------------------------------
        # 1. Daily Aggregation (Average values per day)
        # ----------------------------------------------------
        daily_averages = df.groupby(['Year', 'Month', 'Day']).agg({
            'Temperature (C)': 'mean',
            'Apparent Temperature (C)': 'mean',
            'Humidity': 'mean',
            'Wind Speed (km/h)': 'mean',
            'Visibility (km)': 'mean',
            'Pressure (millibars)': 'mean'
        }).reset_index()

        # Add wind strength categorization
        bins = [0, 1.5, 3.3, 5.4, 7.9, 10.7, 13.8, 17.1, 20.7, 24.4, 28.4, 32.6, np.inf]
        labels = ['Calm', 'Light Air', 'Light Breeze', 'Gentle Breeze', 'Moderate Breeze', 
                  'Fresh Breeze', 'Strong Breeze', 'Near Gale', 'Gale', 'Strong Gale', 
                  'Storm', 'Violent Storm']
        daily_averages['Wind Strength'] = pd.cut(daily_averages['Wind Speed (km/h)'], bins=bins, labels=labels, right=False)

        # ----------------------------------------------------
        # 2. Monthly Aggregation (Average values per month)
        # ----------------------------------------------------
        monthly_aggregates = df.groupby(['Year', 'Month']).agg({
            'Temperature (C)': 'mean',
            'Apparent Temperature (C)': 'mean',
            'Humidity': 'mean',
            'Visibility (km)': 'mean',
            'Pressure (millibars)': 'mean'
        }).reset_index()

        # Calculate mode of Precip Type per month (only if there is a clear mode)
        monthly_mode = df.groupby(['Year', 'Month'])['Precip Type'].agg(
            lambda x: x.mode()[0] if len(x.mode()) == 1 else np.nan).reset_index()
        monthly_mode.rename(columns={'Precip Type': 'Mode'}, inplace=True)

        # Merge the mode precip type into the monthly aggregates
        monthly_weather = pd.merge(monthly_aggregates, monthly_mode, on=['Year', 'Month'], how='left')
        print(type(monthly_weather))

        # ----------------------------------------------------
        # 3. Save to new CSV files
        # ----------------------------------------------------
        daily_averages['formatted_date'] = pd.to_datetime(daily_averages[['Year', 'Month', 'Day']].astype(str).agg('-'.join, axis=1))
        daily_averages.drop(columns=['Year', 'Month', 'Day'], inplace=True)  # Drop the individual date components

        # Save the transformed data to new CSV files (optional)
        daily_averages.to_csv('/home/ubuntu/airflow/datasets/daily_weather.csv', index=False)
        monthly_weather.to_csv('/home/ubuntu/airflow/datasets/monthly_weather.csv', index=False)

        print("Transformation complete. Daily and monthly data saved.")
        
        # ----------------------------------------------------
        # Ensure no non-serializable types (like Timestamp) are included before pushing to XCom
        # ----------------------------------------------------
        # Convert 'Formatted Date' or any other Timestamp columns to strings
        daily_averages['formatted_date'] = daily_averages['formatted_date'].astype(str)

        # Convert the pandas DataFrame to a dictionary and push to XCom
        # daily_averages_dict = daily_averages.to_dict(orient='records')
        
        # Push to XCom (ensure that no Timestamp remains in the DataFrame)
        task_instance.xcom_push(key='daily_weather', value=daily_averages)
        task_instance.xcom_push(key='monthly_weather', value=monthly_weather)

        # Return the transformed data (for further processing or testing)
        return daily_averages, monthly_weather

    except Exception as e:
        print(f"Error during transformation: {e}")
        raise


transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag
)

# Define the validation task
def validate_data(task_instance):
    # Pull the daily and monthly data from XCom (already passed in previous steps)
    daily_data = task_instance.xcom_pull(key='daily_weather')
    monthly_data = task_instance.xcom_pull(key='monthly_weather')


    #print(type(daily_data))
    #print(type(monthly_data))

    # Check for missing values in critical fields
    missing_values_daily = daily_data.isna().sum()
    missing_values_monthly = monthly_data.isna().sum()

    
    # If any missing values in critical fields, raise an error
    if missing_values_daily.any() or missing_values_monthly.any():
        raise ValueError(f"Missing values detected: {missing_values_daily} / {missing_values_monthly}")
    
    # Perform range checks
    # Temperature: reasonable weather range
    temperature_range_check_daily = daily_data['Temperature (C)'].between(-50, 50)
    temperature_range_check_monthly = monthly_data['Temperature (C)'].between(-50, 50)
    
    # Humidity: should be between 0 and 1
    humidity_range_check_daily = daily_data['Humidity'].between(0, 1)
    humidity_range_check_monthly = monthly_data['Humidity'].between(0, 1)
    
    # Wind Speed: should be non-negative
    wind_speed_range_check_daily = daily_data['Wind Speed (km/h)'] >= 0
    # There's no monthly wind speed in the monthly data
    # wind_speed_range_check_monthly = monthly_data['Wind Speed (km/h)'] >= 0
    
    # Identify outliers based on extreme values (e.g., unusually high temperatures)
    daily_data['Temperature Outliers'] = daily_data['Temperature (C)'].apply(lambda x: x < -30 or x > 40)
    monthly_data['Temperature Outliers'] = monthly_data['Temperature (C)'].apply(lambda x: x < -30 or x > 40)
    
    # Check that no outliers are flagged
    if daily_data['Temperature Outliers'].any() or monthly_data['Temperature Outliers'].any():
        raise ValueError("Outliers detected in temperature data!")
    
    # Check all range validations
    if not (temperature_range_check_daily.all() and temperature_range_check_monthly.all()):
        raise ValueError("Temperature values out of expected range (-50°C to 50°C).")
    if not (humidity_range_check_daily.all() and humidity_range_check_monthly.all()):
        raise ValueError("Humidity values out of expected range (0 to 1).")
    # There's no monthly wind speed in the monthly data (removed the monthly part)
    if not (wind_speed_range_check_daily.all()):
        raise ValueError("Wind Speed values should be >= 0.")

    # Save the validated data for the load step
    daily_validated_path = '/tmp/historical_daily_data.csv'
    monthly_validated_path = '/tmp/historical_monthly_data.csv'

    daily_data.to_csv(daily_validated_path, index=False)
    monthly_data.to_csv(monthly_validated_path, index=False)
        
    
    task_instance.xcom_push(key='daily_weather_path', value=daily_validated_path)
    task_instance.xcom_push(key='monthly_weather_path',value=monthly_validated_path)

    # If everything is fine, return True
    print("Validation passed successfully!")
    return True


validate_data_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    provide_context=True,
    dag=dag
)

# Define the load data task


def load_data(task_instance):
    # Pull the paths of the daily and monthly data CSV files from XCom
    daily_weather_file = task_instance.xcom_pull(key='daily_weather_path')
    monthly_weather_file = task_instance.xcom_pull(key='monthly_weather_path')

    print("1",type(daily_weather_file))
    print("2",type(monthly_weather_file))


    # Load the CSV files into the respective SQLite tables
    conn = sqlite3.connect('/home/ubuntu/airflow/databases/weather_data.db')
    cursor = conn.cursor()

    # Create daily_weather table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS daily_weather (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        formatted_date TEXT,
        temperature_c REAL,
        apparent_temperature_c REAL,
        humidity REAL,
        wind_speed_kmh REAL,
        visibility_km REAL,
        pressure_millibars REAL,
        wind_strength TEXT
    )
    ''')

    # Create monthly_weather table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS monthly_weather (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        year INTEGER,
        month INTEGER,
        avg_temperature_c REAL,
        avg_apparent_temperature_c REAL,
        avg_humidity REAL,
        avg_visibility_km REAL,
        avg_pressure_millibars REAL,
        mode_precip_type TEXT
    )
    ''')


    print("before daily")

    # Insert daily data into the daily_weather table
    daily_df = pd.read_csv(daily_weather_file)
    for _, row in daily_df.iterrows():
        cursor.execute('''
        INSERT INTO daily_weather (formatted_date, temperature_c, apparent_temperature_c, humidity, wind_speed_kmh, visibility_km, pressure_millibars, wind_strength)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (row['formatted_date'], row['Temperature (C)'], row['Apparent Temperature (C)'], row['Humidity'],
              row['Wind Speed (km/h)'], row['Visibility (km)'], row['Pressure (millibars)'], row['Wind Strength']))


    print("after daily")

    # Insert monthly data into the monthly_weather table
    monthly_df = pd.read_csv(monthly_weather_file)
    for _, row in monthly_df.iterrows():
        cursor.execute('''
        INSERT INTO monthly_weather (year, month, avg_temperature_c, avg_apparent_temperature_c, avg_humidity, avg_visibility_km, avg_pressure_millibars, mode_precip_type)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (row['Year'], row['Month'], row['Temperature (C)'], row['Apparent Temperature (C)'],
              row['Humidity'], row['Visibility (km)'], row['Pressure (millibars)'], row.get('Mode', None)))

    conn.commit()
    conn.close()

    print("Data loading complete.")


load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag
)

# Set task dependencies
extract_data_task >> transform_data_task >> validate_data_task >> load_data_task
