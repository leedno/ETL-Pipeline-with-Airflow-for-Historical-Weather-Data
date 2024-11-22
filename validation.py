import pandas as pd
from datetime import datetime

def validate_data(task_instance):
    # Pull the daily and monthly data from XCom (already passed in previous steps)
    daily_data = task_instance.xcom_pull(key='daily_weather', task_ids='transform_task')
    monthly_data = task_instance.xcom_pull(key='monthly_weather', task_ids='transform_task')

    # Check for missing values in critical fields
    missing_values_daily = daily_data.isna().sum()
    missing_values_monthly = monthly_data.isna().sum()
    
    # If any missing values in critical fields, raise an error
    if missing_values_daily.any() or missing_values_monthly.any():
        raise ValueError(f"Missing values detected: {missing_values_daily} / {missing_values_monthly}")
    
    # Perform range checks
    # Temperature: reasonable weather range
    temperature_range_check_daily = daily_data['Temperature (C)'].between(-50, 50)
    temperature_range_check_monthly = monthly_data['avg_temperature_c'].between(-50, 50)
    
    # Humidity: should be between 0 and 1
    humidity_range_check_daily = daily_data['Humidity'].between(0, 1)
    humidity_range_check_monthly = monthly_data['avg_humidity'].between(0, 1)
    
    # Wind Speed: should be non-negative
    wind_speed_range_check_daily = daily_data['Wind Speed (km/h)'] >= 0
    wind_speed_range_check_monthly = monthly_data['avg_wind_speed_kmh'] >= 0
    
    # Identify outliers based on extreme values (e.g., unusually high temperatures)
    daily_data['Temperature Outliers'] = daily_data['Temperature (C)'].apply(lambda x: x < -30 or x > 40)
    monthly_data['Temperature Outliers'] = monthly_data['avg_temperature_c'].apply(lambda x: x < -30 or x > 40)
    
    # Check that no outliers are flagged
    if daily_data['Temperature Outliers'].any() or monthly_data['Temperature Outliers'].any():
        raise ValueError("Outliers detected in temperature data!")
    
    # Check all range validations
    if not (temperature_range_check_daily.all() and temperature_range_check_monthly.all()):
        raise ValueError("Temperature values out of expected range (-50°C to 50°C).")
    if not (humidity_range_check_daily.all() and humidity_range_check_monthly.all()):
        raise ValueError("Humidity values out of expected range (0 to 1).")
    if not (wind_speed_range_check_daily.all() and wind_speed_range_check_monthly.all()):
        raise ValueError("Wind Speed values should be >= 0.")

    # If everything is fine, return True
    print("Validation passed successfully!")
    return True
