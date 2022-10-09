# fetch data from "https://homeassistant.local:8123/" via the RESTful API
import client

# Home Assistant Token and URL
TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI1Mzc4YWUzMWFlMTI0MTZmOTYzNTNlNDM1ZWZjNzhkNSIsImlhdCI6MTY2NTMzODYwMywiZXhwIjoxOTgwNjk4NjAzfQ.UDjuxN_JZb5ddIA34ZY2hOOfdDWlskvpXrb6AeY4qIk"
URL = "https://homeassistant.local:8123/"

# Retrieve data since...
FIRST_DATE = "2022-10-09T00:00:00.000Z"

# Sensors to retrieve - Grid2Home, Home2Grid, Solarproduction (Watt)
SENSOR_NAMES = ["sensor.g2h_v6_power", "sensor.h2g_v6_power", "sensor.shelly1pm_244cab441f01_power"]

# installed and max possible PV capacity
CURRENT_PV_CAPACITY = 1.5
MAX_PV_CAPACITY = 15.0

if __name__ == '__main__':
    # get the data from the API
    hass = client.client(URL, TOKEN, FIRST_DATE)
    real_data = hass.get_data(SENSOR_NAMES)

    for pv_size in range(int(CURRENT_PV_CAPACITY), int(MAX_PV_CAPACITY)):
        simulated_data = hass.simulate_data(real_data, CURRENT_PV_CAPACITY, pv_size)

        print("solar_selfconsumption at " + str(pv_size) + "kWp :" + str(round(hass.calculate_solar_selfconsumption(simulated_data, pv_size), 4)))

    simulated_data.to_csv("simulated_data.csv", encoding='utf-8', sep=';')



