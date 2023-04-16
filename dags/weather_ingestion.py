import os
import requests
from datetime import datetime, timedelta
from pymongo import MongoClient
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator

# # Get API key and MongoDB credentials from environment variables
# API_KEY = os.environ['OPENWEATHER_API_KEY']
MONGO_USER = os.environ['MONGO_INITDB_ROOT_USERNAME']
MONGO_PASSWORD = os.environ['MONGO_INITDB_ROOT_PASSWORD']
MONGO_DB = os.environ['MONGO_INITDB_DATABASE']
MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@mongodb:27017/{MONGO_DB}"

# Function to convert Fahrenheit to Celsius
def fahrenheit_to_celsius(fahrenheit_temp):
    return (fahrenheit_temp - 32) * 5 / 9

def mph_to_mps(miles_per_hour):
    meters_per_second = miles_per_hour * 0.44704
    return meters_per_second

# Function to process ingested weather data
def process_data():
    # Read the csv file
    current_date = datetime.now().strftime("%Y-%m-%d_%H")
    input_file_path = f"/opt/airflow/data/{current_date}_weather_data.csv"
    output_file_path = f"/opt/airflow/data/processed_{current_date}_weather_data.csv"
    df = pd.read_csv(input_file_path)

    # Process the data 
    df["temperature"] = df["temperature"].apply(fahrenheit_to_celsius)
    df["wind_speed"] = df["wind_speed"].apply(mph_to_mps)

    # Save the processed data to a new JSON file
    df.to_csv(output_file_path, index=False)


# Function to store weather data in MongoDB
def store_weather_data():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db['weather']

    current_date = datetime.now().strftime("%Y-%m-%d_%H")
    input_file_path = f"/opt/airflow/data/processed_{current_date}_weather_data.csv"
    df = pd.read_csv(input_file_path)
    records = df.to_dict(orient='records')

    collection.insert_many(records)
    client.close()

# Define DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'weather_ingestion',
    default_args=default_args,
    description='Ingest weather data from OpenWeather API and store in MongoDB',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2023, 4, 16),
    catchup=False
)

# Define task to download weather data 
get_weather_data_task = BashOperator(
    task_id='download_weather_data',
    bash_command='python /opt/airflow/dags/get_weather_data.py',
    dag=dag
)

# Define task to process weather data
process_data_with_spark_task = PythonOperator(
    task_id="process_data_with_spark",
    python_callable=process_data,
    dag=dag
)

# Define task to store weather data in MongoDB
store_weather_data_task = PythonOperator(
    task_id='store_weather_data',
    python_callable=store_weather_data,
    dag=dag
)

get_weather_data_task >> process_data_with_spark_task >> store_weather_data_task
