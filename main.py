# like main but with multiprocessing

# fetch data from "https://homeassistant.local:8123/" via the RESTful API
import client_mp as client
from collections import defaultdict
import functions_mp as functions
from multiprocessing import Pool
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import datetime
import math
import os

# ----------------- CONFIGURATION -----------------

# Home Assistant Token and URL
TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI1Mzc4YWUzMWFlMTI0MTZmOTYzNTNlNDM1ZWZjNzhkNSIsImlhdCI6MTY2NTMzODYwMywiZXhwIjoxOTgwNjk4NjAzfQ.UDjuxN_JZb5ddIA34ZY2hOOfdDWlskvpXrb6AeY4qIk"
URL = "http://homeassistant.local:8123/"

# Retrieve data since...
FIRST_DATE = "2022-10-09"
FIRST_DATE = "2022-10-16"

# Sensors to retrieve - Grid2Home, Home2Grid, Solarproduction (Watt)
SENSOR_NAMES = ["sensor.g2h_v6_power", "sensor.h2g_v6_power", "sensor.shelly1pm_244cab441f01_power"]

# installed and max possible PV capacity
CURRENT_PV_SIZE = 1.5
MAX_PV_CAPACITY = 15.0
MAX_BATTERY_SIZE = 15.0

# do to a bug in multiprocessing, functions need to be defined in a separate file

# ----------------- MAIN -----------------
if __name__ == '__main__':
    # crate a 3D dictionary to store the data
    # the first key is the date
    # the second key is the pv size
    # the third key is the battery size
    df_dict = defaultdict(lambda: defaultdict(dict))


    # connect to Home Assistant
    print ("Connecting to Home Assistant...")
    hass = client.client(URL, TOKEN, SENSOR_NAMES, CURRENT_PV_SIZE)

    # make a list of all days since FIRST_DATE
    dates = []
    date = datetime.datetime.strptime(FIRST_DATE, "%Y-%m-%d")
    while date < datetime.datetime.now():
        dates.append(date)
        date += datetime.timedelta(days=1)
    print("Fetching data for " + str(len(dates)) + " days...")

    # convert dates to strings
    dates = [date.strftime("%Y-%m-%d") for date in dates]

    # actually fetch the data for each day
    with Pool(1) as p:
        #data = p.map(hass.fetch_data, (date for date in dates))
        data = p.map(hass.get_data, (date for date in dates))
        p.close()
        p.join()

    # put the data into the dictionary
    for df in data:
        df_dict[df.columns.name.split(" ")[0]][df.columns.name.split(" ")[1]][df.columns.name.split(" ")[2]] = df
        print(df.columns.name.split(" ")[0], df.columns.name.split(" ")[1], df.columns.name.split(" ")[2])

    # print the data for debugging: 2022-10-16 01.5kWp 00kWh
    print(df_dict["2022-10-16"]["1.5kWp"]["0.0kWh"])

    exit()

    # calculate the power consumption per time step
    with Pool(1) as p:
        data = p.map(functions.calculate_consumption, (df for df in data))
        p.close()
        p.join()

    # put the df back into the dictionary

    # simulate the solar production for each day
    with Pool(1) as p:
        data = p.map(functions.simulate_solar, (df for df in data))
        p.close()
        p.join()



    print("Finished.")






    # fetch data from Home Assistant
    # create a seperate dataframe for each day
