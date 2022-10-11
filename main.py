# fetch data from "https://homeassistant.local:8123/" via the RESTful API
import client
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import datetime

# ----------------- CONFIGURATION -----------------

# Home Assistant Token and URL
TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI1Mzc4YWUzMWFlMTI0MTZmOTYzNTNlNDM1ZWZjNzhkNSIsImlhdCI6MTY2NTMzODYwMywiZXhwIjoxOTgwNjk4NjAzfQ.UDjuxN_JZb5ddIA34ZY2hOOfdDWlskvpXrb6AeY4qIk"
URL = "https://homeassistant.local:8123/"

# Retrieve data since...
FIRST_DATE = "2022-10-09"

# Sensors to retrieve - Grid2Home, Home2Grid, Solarproduction (Watt)
SENSOR_NAMES = ["sensor.g2h_v6_power", "sensor.h2g_v6_power", "sensor.shelly1pm_244cab441f01_power"]

# installed and max possible PV capacity
CURRENT_PV_CAPACITY = 1.0
MAX_PV_CAPACITY = 15.0



if __name__ == '__main__':
    # get the data from the API
    # convert FIRST_DATE to ISO format
    start_date = datetime.datetime.strptime(FIRST_DATE, "%Y-%m-%d").isoformat() + "Z"
    hass = client.client(URL, TOKEN, start_date)

    # for every day since FIRST_DATE
    first_date = datetime.date.fromisoformat(start_date[:10])
    today = datetime.date.today()

    # convert today to string "YYYY-MM-DDT00:00:00.000Z"
    today_str = today.isoformat() + "T00:00:00.000Z"

    # get the data from the API for every day and combine it into one dataframe
    real_data = pd.DataFrame()
    for day in range((today - first_date).days + 1):
        date = (first_date + datetime.timedelta(days=day)).isoformat()
        print("Fetching data for " + date)
        # convert date to string "YYYY-MM-DDT00:00:00.000Z"
        date_str = date + "T00:00:00.000Z"
        hass = client.client(URL, TOKEN, date_str)
        real_data = real_data.append(hass.get_data(SENSOR_NAMES))

    '''
    real_data = hass.get_data(SENSOR_NAMES)
    '''
    print(real_data)

    solar_selfconsumption = []
    net_neutrality = []

    for pv_size in range(int(CURRENT_PV_CAPACITY), int(MAX_PV_CAPACITY)):
        simulated_data = hass.simulate_data_solar_only(real_data, CURRENT_PV_CAPACITY, pv_size)

        solar_selfconsumption.append(hass.calculate_solar_selfconsumption(simulated_data, pv_size))
        net_neutrality.append(hass.calculate_net_neutrality(simulated_data, pv_size))

    #hass.simulate_data_battery(real_data,CURRENT_PV_CAPACITY, 8.0, 12.5)


    # plot the results
    plt.plot(range(int(CURRENT_PV_CAPACITY), int(MAX_PV_CAPACITY)), solar_selfconsumption)
    plt.plot(range(int(CURRENT_PV_CAPACITY), int(MAX_PV_CAPACITY)), net_neutrality)
    plt.legend(["Solar Selfconsumption", "Net Neutrality"])
    plt.xlabel('PV size (kWp)')
    plt.ylabel('percent')
    plt.ylim(0, 1.5)
    plt.show()
    plt.savefig("graph.png")

    solar_selfconsumption = pd.DataFrame(solar_selfconsumption)
    solar_selfconsumption.to_csv("solar_selfconsumption.csv")

    simulated_data.to_csv("simulated_data.csv", encoding='utf-8', sep=';')



