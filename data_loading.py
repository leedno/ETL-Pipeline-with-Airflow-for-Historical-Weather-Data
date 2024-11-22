import sqlite3

def load_data(task_instance):
    # Pull the paths of the daily and monthly data CSV files from XCom
    daily_weather_file = task_instance.xcom_pull(key='daily_weather_path', task_ids='transform_data')
    monthly_weather_file = task_instance.xcom_pull(key='monthly_weather_path', task_ids='transform_data')

    # Load the CSV files into the respective SQLite tables
    conn = sqlite3.connect('/home/core/airflow/datasets/weather_data.db')
    cursor = conn.cursor()

    # Insert daily data into the daily_weather table
    daily_df = pd.read_csv(daily_weather_file)
    for _, row in daily_df.iterrows():
        cursor.execute('''
        INSERT INTO daily_weather (formatted_date, temperature_c, apparent_temperature_c, humidity, wind_speed_kmh, visibility_km, pressure_millibars, wind_strength)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (row['formatted_date'], row['Temperature (C)'], row['Apparent Temperature (C)'], row['Humidity'],
              row['Wind Speed (km/h)'], row['Visibility (km)'], row['Pressure (millibars)'], row['Wind Strength']))

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
