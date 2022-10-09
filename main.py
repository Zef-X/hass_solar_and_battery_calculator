# fetch data from "https://homeassistant.local:8123/" via the RESTful API

import client

token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI1Mzc4YWUzMWFlMTI0MTZmOTYzNTNlNDM1ZWZjNzhkNSIsImlhdCI6MTY2NTMzODYwMywiZXhwIjoxOTgwNjk4NjAzfQ.UDjuxN_JZb5ddIA34ZY2hOOfdDWlskvpXrb6AeY4qIk"
url = "https://homeassistant.local:8123/"
first_date = "2022-10-09T00:00:00.000Z"
sensors = ["sensor.g2h_v6_power", "sensor.h2g_v6_power", "sensor.shelly1pm_244cab441f01_power"]

actual_pv_size = 1.5
max_pv_size = 15.0

hass = client.client(url, token, first_date)

if __name__ == '__main__':
    # get the data from the API
    real_data = hass.get_data(sensors)
    print(real_data)

