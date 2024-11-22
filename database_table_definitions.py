import sqlite3

def create_tables():
    # Connect to SQLite database (will create the DB if it doesn't exist)
    conn = sqlite3.connect('/home/core/airflow/datasets/weather_data.db')
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

    conn.commit()
    conn.close()

# Call this function to create tables
create_tables()
