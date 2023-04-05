import os
import requests
import pandas as pd
from datetime import datetime, timedelta

API_KEY = os.environ['OPENWEATHER_API_KEY']
LOCATION_DICT = {"1566083": "Ho Chi Minh", "1581130": "Ha Noi", "1580240": "Hue"}

# The OpenWeather API endpoint for historical data
API_URL = "https://api.openweathermap.org/data/2.5/weather"

def fetch_weather_data(id_location, api_key):
    response = requests.get(
        API_URL,
        params={
            "id": id_location,
            "appid": api_key,
            "units": "metric",
            },
        )
    data = response.json()
    return data


def process_weather_data(data, location):
    processed_data = {"timestamp": datetime.fromtimestamp(data["dt"]),
                        "city": location,
                        "temperature": data["main"]["temp"],
                        "humidity": data["main"]["humidity"],
                        "pressure": data["main"]["pressure"],
                        "wind_speed": data["wind"]["speed"],
                    }

    return processed_data

def save_weather_data(df, output_file):
    df.to_csv(output_file, index=False)

def main():
    current_date = datetime.now().strftime("%Y-%m-%d_%H")
    file_path = f"/opt/airflow/data/{current_date}_weather_data.csv"


    print(f"Fetching data for {current_date}")

    weather_data = []
    for id, location in LOCATION_DICT.items():
        raw_data = fetch_weather_data(id, API_KEY)
        processed_data = process_weather_data(raw_data, location)
        weather_data.append(processed_data)

    df = pd.DataFrame(weather_data)
    save_weather_data(df, file_path)
    print(os.getcwd())

if __name__ == "__main__":
    main()