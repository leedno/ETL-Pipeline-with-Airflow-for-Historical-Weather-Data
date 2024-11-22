# ETL Pipeline Tasks and Responsibilities

## **Person 1: Extraction and Initial Database Setup (Total: 7 Points)**

### **Dataset Download (1 point):**
- Download the dataset from Kaggle and place it in the `airflow/datasets/` directory.

### **Extraction Task (2 points):**
- Read the `weatherHistory.csv` file into a DataFrame as the initial extraction step.
- Use XCom to pass the DataFrame or file path to the next step in the pipeline.

### **Data Cleaning - Part 1 (2 points):**
- Convert `Formatted Date` to a proper date format.
- Check for duplicates and remove them if necessary.

### **Database Setup (2 points):**
- Create an SQLite database named `weather_data.db` in the `airflow/db/` directory.
- Set up initial table structures for `daily_weather` and `monthly_weather` (schema can be refined later by Person 4).

---

## **Person 2: Data Cleaning and Transformation - Part 1 (Total: 7 Points)**

### **Data Cleaning - Part 2 (1 point):**
- Handle any missing or erroneous data in key columns like `Temperature (C)`, `Humidity`, and `Wind Speed (km/h)`.

### **Feature Engineering - Daily Averages (1 point):**
- Calculate daily averages for temperature, humidity, and wind speed.

### **Feature Engineering - Wind Strength Categorization (1 point):**
- Add a new feature called `wind_strength`, categorizing wind speeds as specified.

### **Feature Engineering - Monthly Mode for Precipitation Type (2 points):**
- Group data by month and calculate the mode of `Precip Type` for each month.
- Save this information in a `Mode` column.

### **Save Transformed Daily Data (1 point):**
- Save the transformed daily data to a new CSV file for use in the next steps.

### **XCom for Transformation (1 point):**
- Use XCom to pass transformed daily data to the validation step.

---

## **Person 3: Transformation - Part 2 and Validation (Total: 7 Points)**

### **Monthly Aggregates (2 points):**
- Calculate monthly averages for temperature, humidity, wind speed, visibility, and pressure.
- Save the monthly transformed data to a new CSV file.

### **XCom for Transformation (1 point):**
- Use XCom to pass the transformed monthly data to the validation step.

### **Validation (3 points):**
- **Missing Values Check:** Ensure there are no missing values in critical fields.
- **Range Check:** Verify that values fall within expected ranges:
  - Temperature: -50 to 50°C
  - Humidity: 0 to 1
  - Wind Speed: 0 and above.
- **Outlier Detection:** Identify and log any extreme outliers in the data (e.g., unusually high temperatures).

### **Trigger Rules (1 point):**
- Set an `all_success` rule so that the pipeline only continues to the load step if validation passes successfully.

---

## **Person 4: Load Data and Airflow DAG Definition (Total: 7 Points)**

### **Refine Database Table Definitions (1 point):**
- Define final schema for the `daily_weather` and `monthly_weather` tables in SQLite.

### **Data Loading (3 points):**
- Use XCom to pull the paths for the daily and monthly transformed data files and load them into the respective SQLite tables.

### **Airflow DAG Definition (3 points):**
- Define the Airflow DAG for the ETL pipeline, including:
  - Tasks for each ETL step.
  - XCom for data passing.
  - Dependencies.
  - Trigger rules.

---

## **Final Overview**
- **Person 1:** Extraction, initial data cleaning, and initial database setup — 7 points.
- **Person 2:** Data cleaning continuation, first part of transformation, and feature engineering — 7 points.
- **Person 3:** Monthly aggregation, validation, and setting trigger rules — 7 points.
- **Person 4:** Database table refinement, data loading, and Airflow DAG configuration — 7 points.
