import os
import requests
from datetime import datetime, timedelta
from pymongo import MongoClient
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator

# # Get API key and MongoDB credentials from environment variables
# API_KEY = os.environ['OPENWEATHER_API_KEY']
# MONGO_USER = os.environ['MONGO_INITDB_ROOT_USERNAME']
# MONGO_PASSWORD = os.environ['MONGO_INITDB_ROOT_PASSWORD']
# MONGO_DB = os.environ['MONGO_INITDB_DATABASE']
# MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@mongodb:27017/{MONGO_DB}"

# Get API key and MongoDB credentials from environment variables
API_KEY = 1
MONGO_USER = 2
MONGO_PASSWORD = 3
MONGO_DB = 4
MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@mongodb:27017/{MONGO_DB}"

# Function to fetch weather data
def fetch_weather_data():
    url = f"http://api.openweathermap.org/data/2.5/weather?q=Ho%20Chi%20Minh%20City&appid={API_KEY}"
    response = requests.get(url)
    data = response.json()
    return data

# Function to store weather data in MongoDB
def store_weather_data():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db['weather']

    weather_data = fetch_weather_data()
    weather_data['timestamp'] = datetime.utcnow()

    collection.insert_one(weather_data)
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
    start_date=datetime(2023, 4, 4),
    catchup=False
)

# Define task to download weather data 
get_weather_data_task = BashOperator(
    task_id='download_weather_data',
    bash_command='python /opt/airflow/dags/get_weather_data.py',
    dag=dag
)

get_weather_data_task
