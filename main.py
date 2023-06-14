import requests
import json
import re
import os
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm
import sqlite3

# Define DB Stuff
database_name = "home-assistant_v2.db"

# Define sensor names
sensor_names = ["sensor.g2h_v6_power", "sensor.h2g_v6_power", "sensor.shelly1pm_244cab441f01_power"]

# Define date range
first_date = "2022-10-18 00:00:00"
last_date = "2023-06-10 23:59:59"

def get_sensor_data(database_name, sensor_names, first_date, last_date):
    connection = sqlite3.connect(database_name)
    dfs = []

    for sensor_name in sensor_names:
        query = f"""
        SELECT states_meta.entity_id,
               strftime('%Y-%m-%d %H:%M:%S', datetime((states.last_updated_ts / 300) * 300 - ((states.last_updated_ts / 300) * 300 % 300), 'unixepoch')) AS timestamp,
               states.state
        FROM states
        LEFT JOIN states_meta ON (states.metadata_id = states_meta.metadata_id)
        WHERE states_meta.entity_id = '{sensor_name}'
          AND states.last_updated_ts >= strftime('%s', '{first_date}')
          AND states.last_updated_ts <= strftime('%s', '{last_date}')
        GROUP BY states_meta.entity_id, timestamp
        ORDER BY timestamp DESC
        """

        df = pd.read_sql_query(query, connection)
        dfs.append(df)

    connection.close()
    combined_df = pd.concat(dfs, ignore_index=True)

    # Reshape the DataFrame
    reshaped_df = combined_df.pivot_table(index='timestamp', columns='entity_id', values='state', aggfunc='first')
    reshaped_df.columns.name = None
    reshaped_df.reset_index(inplace=True)

    # make sure that the timestamp column is of type datetime
    reshaped_df['timestamp'] = pd.to_datetime(reshaped_df['timestamp'])

    # replace every 'unknown' and 'unavailable' value with 0
    reshaped_df = reshaped_df.replace(['unknown', 'unavailable'], 0)

    # fill missing values with 0
    reshaped_df = reshaped_df.fillna(0)

    # make sure that the columns are of type float
    try:
        reshaped_df['sensor.g2h_v6_power'] = reshaped_df['sensor.g2h_v6_power'].astype(float)
        reshaped_df['sensor.h2g_v6_power'] = reshaped_df['sensor.h2g_v6_power'].astype(float)
        reshaped_df['sensor.shelly1pm_244cab441f01_power'] = reshaped_df['sensor.shelly1pm_244cab441f01_power'].astype(float)
    except:
        pass

    return reshaped_df

# Function which calculates the total power consumption for each timestamp: ((g2h_v6_power + shelly1pm_244cab441f01_power) - h2g_v6_power)
def calculate_total_power_consumption(df):
    df['total_power_consumption'] = df.apply(lambda row: (float(row['sensor.g2h_v6_power']) + float(row['sensor.shelly1pm_244cab441f01_power'])) - float(row['sensor.h2g_v6_power']), axis=1)
    return df

# Python Main function
if __name__ == "__main__":
    df = get_sensor_data(database_name, sensor_names, first_date, last_date)
    print(df)

    #save the df to a csv file
    df.to_csv("sensor_data_raw.csv", index=False)

    df_calc = calculate_total_power_consumption(df)
    print(df_calc)
    df_calc.to_csv("sensor_data_calc.csv", index=False)
