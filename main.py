import requests
import json
import re
import os
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm

# Define Home Assistant credentials
url = "http://homeassistant.local:8123/api/history/period"
url = "http://homeassistant.local:8123"
token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI1Mzc4YWUzMWFlMTI0MTZmOTYzNTNlNDM1ZWZjNzhkNSIsImlhdCI6MTY2NTMzODYwMywiZXhwIjoxOTgwNjk4NjAzfQ.UDjuxN_JZb5ddIA34ZY2hOOfdDWlskvpXrb6AeY4qIk"

# Define sensor names
sensor_names = ["sensor.g2h_v6_power", "sensor.h2g_v6_power", "sensor.shelly1pm_244cab441f01_power"]

# Define date range
first_date = "2023-01-01"
last_date = "2023-03-16"

def download_sensor_data(url, token, sensor_names, first_date, last_date):
    # Initialize empty DataFrame to store data
    df = pd.DataFrame()

    # Iterate over sensor names
    for sensor_name in sensor_names:
        # Build URL to download data
        data_url = f"{url}/api/history/period/{first_date}T00:00:00?filter_entity_id={sensor_name}"
        headers = {"Authorization": f"Bearer {token}"}

        # Send request and receive data in JSON format
        response = requests.get(data_url, headers=headers)
        data = response.json()

        # Convert data to pandas DataFrame and append to df
        temp_df = pd.DataFrame(data)
        temp_df = temp_df.set_index("last_changed")
        temp_df = temp_df[["state"]]
        temp_df.columns = [sensor_name]
        df = pd.concat([df, temp_df], axis=1)

    # Filter data to desired date range
    df = df.loc[first_date:last_date]

    # Write data to CSV file
    df.to_csv("sensor_data.csv")

data = download_sensor_data(url, token, sensor_names, first_date, last_date)
print(data)