import requests
import datetime
from datetime import timedelta
import os
import json
import re
import pyarrow as pa
import pyarrow.parquet as pq
import glob
import pandas as pd

def get_data_from_home_assistant(first_day, last_day, sensors):
    # convert the dates to datetime objects
    first_day = datetime.datetime.strptime(first_day, "%Y-%m-%d")
    last_day = datetime.datetime.strptime(last_day, "%Y-%m-%d")
    data = []
    print(f"Retrieving data for sensors {sensors} from {first_day} to {last_day}")
    for sensor in sensors:
        for day in daterange(first_day, last_day):
            data.extend(get_data_for_sensor(sensor, day))

    print(f"Retrieved {len(data)} data points")
    convert_data_to_performant_format(first_day, last_day, sensors)
    return data

def get_data_for_sensor(sensor, day):
    # Sanitize the file name
    file_name = f"{day}-{sensor}"
    file_name = re.sub(r"[^\w\d_.-]", "_", file_name)

    # Check if the data is already cached
    cache_folder = "cache"
    cache_file = f"{cache_folder}/{file_name}.json"
    if os.path.exists(cache_file):
        with open(cache_file, "r") as f:
            return json.load(f)

    # If the data is not cached, make an API request to get it
    # Set up the API endpoint and API key
    endpoint = "http://homeassistant.local:8123/api/history/period"
    api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI1Mzc4YWUzMWFlMTI0MTZmOTYzNTNlNDM1ZWZjNzhkNSIsImlhdCI6MTY2NTMzODYwMywiZXhwIjoxOTgwNjk4NjAzfQ.UDjuxN_JZb5ddIA34ZY2hOOfdDWlskvpXrb6AeY4qIk"

    # Set up the headers for the API request
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    # Set up the parameters for the API request
    params = {
        "start": day,
        "end": day,
        "filter_entity_id": sensor,
    }

    # Send the API request and get the response
    response = requests.get(endpoint, headers=headers, params=params)

    # If the request was successful, cache the data and return it
    if response.status_code == 200:
        if not os.path.exists(cache_folder):
            os.makedirs(cache_folder)
        with open(cache_file, "w") as f:
            json.dump(response.json(), f)
        return response.json()
    else:
        # If the request was not successful, print an error message
        print(f"Error getting data for sensor {sensor} on day {day}: {response.status_code}")
        return []

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

def convert_data_to_performant_format(first_day, last_day, sensors):
    for sensor in sensors:
        # Create a Pandas DataFrame from the sensor data
        df = pd.DataFrame()
        cache_folder = "cache"
        cache_file_pattern = f"{cache_folder}/*-{sensor}.json"
        for cache_file in glob.glob(cache_file_pattern):
            with open(cache_file, "r") as f:
                data = json.load(f)
                for item in data:
                    df = df.append(item, ignore_index=True)

        # Save the Pandas DataFrame as a Parquet file
        file_name = f"{first_day}_{last_day}_{sensor}"
        file_name = re.sub(r"[^\w\d_.-]", "_", file_name)
        file_name += ".parquet"
        df.to_parquet(file_name)
