# Import necessary libraries
from airflow.models import Variable

# Define file paths
csv_file_path = Variable.get('csv_file_path', default_var='/home/core/airflow/datasets/weatherHistory.csv')

# Task 1: Extract data
def extract_data(**kwargs):
    """
    Extracts the weatherHistory.csv dataset.
    Pushes the file path to XCom.
    """
    kwargs['ti'].xcom_push(key='csv_file_path', value=csv_file_path)
