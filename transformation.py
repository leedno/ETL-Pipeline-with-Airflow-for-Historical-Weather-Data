import pandas as pd
import numpy as np
from airflow.models import TaskInstance

def transform_data(task_instance):
    # Step 1: Load the dataset
    df = pd.read_csv('/home/core/airflow/datasets/weatherHistory.csv')

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

        # ----------------------------------------------------
        # 3. Save to new CSV files
        # ----------------------------------------------------
        daily_averages['formatted_date'] = pd.to_datetime(daily_averages[['Year', 'Month', 'Day']].astype(str).agg('-'.join, axis=1))
        daily_averages.drop(columns=['Year', 'Month', 'Day'], inplace=True)  # Drop the individual date components

        # Save the transformed data to new CSV files (optional)
        daily_averages.to_csv('/home/core/airflow/datasets/daily_weather.csv', index=False)
        monthly_weather.to_csv('/home/core/airflow/datasets/monthly_weather.csv', index=False)

        print("Transformation complete. Daily and monthly data saved.")
        
        # ----------------------------------------------------
        # Ensure no non-serializable types (like Timestamp) are included before pushing to XCom
        # ----------------------------------------------------
        # Convert 'Formatted Date' or any other Timestamp columns to strings
        daily_averages['formatted_date'] = daily_averages['formatted_date'].astype(str)

        # Convert the pandas DataFrame to a dictionary and push to XCom
        daily_averages_dict = daily_averages.to_dict(orient='records')
        
        # Push to XCom (ensure that no Timestamp remains in the DataFrame)
        task_instance.xcom_push(key='daily_weather', value=daily_averages_dict)

        # Return the transformed data (for further processing or testing)
        return daily_averages, monthly_weather

    except Exception as e:
        print(f"Error during transformation: {e}")
        raise

# If this script is run directly, execute the transformation (you can comment this out in production if needed)
if __name__ == "__main__":
    # Make sure you pass `task_instance` if calling this in testing directly (though normally Airflow handles this)
    daily_weather, monthly_weather = transform_data(task_instance)
    print("Sample daily weather data:")
    print(daily_weather.head())  # Just to verify the output in the console (optional)
    print("Sample monthly weather data:")
    print(monthly_weather.head())  # Just to verify the output in the console (optional)
