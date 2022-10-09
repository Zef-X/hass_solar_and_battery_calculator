import pandas as pd
import requests

class client:
    def __init__(self, url, token, first_date):
        self.url = url
        self.token = token
        self.first_date = first_date
        self.headers = {
            "Authorization": "Bearer " + self.token,
            "content-type": "application/json",
        }

    def get_data(self, sensors):
        real_data = pd.DataFrame(columns=sensors)
        for sensor in sensors:
            url = "http://homeassistant.local:8123/api/history/period/" + self.first_date + "?filter_entity_id=" + sensor
            response = requests.get(url, headers=self.headers, verify=False)
            data = response.json()

            # parse the data into a pandas dataframe, state in column of sensor name
            for item in data:
                real_data = real_data.append(item, ignore_index=True)

        # sort state into column of sensor name
        real_data = real_data.pivot(index="last_changed", columns="entity_id", values="state")

        # index the dataframe by date
        real_data.index = pd.to_datetime(real_data.index)
        real_data = real_data.sort_index()

        # cleanup file and remove all rows with value "unknown"
        real_data = real_data[real_data != "unknown"]

        # resample the data to 15 seconds
        real_data = real_data.ffill()
        real_data = real_data.astype(float)
        real_data = real_data.resample("15S").mean()
        real_data = real_data.round(2)

        # rename the columns "Grid2Home", "Home2Grid" and "SolarProduction"
        real_data = real_data.rename(columns={"sensor.g2h_v6_power": "Grid2Home", "sensor.h2g_v6_power": "Home2Grid", "sensor.shelly1pm_244cab441f01_power": "SolarProduction"})

        real_data = self.get_consumption(real_data)

        return real_data

    def get_consumption(self, data):
        # calculate the consumption
        data["HomeConsumption"] = data["Grid2Home"] + data["SolarProduction"] - data["Home2Grid"]

        return data

    def simulate_data(self, data,current_pv_size, sim_pv_size):
        # normalize the data
        data["SolarProduction " + str(sim_pv_size) + "kWp"] = (data["SolarProduction"]/current_pv_size) * sim_pv_size
        data["Additional_Solar_Production " + str(sim_pv_size) + "kWp"] = data["SolarProduction " + str(sim_pv_size) + "kWp"] - data["SolarProduction"]

        if data["Grid2Home"].sum() > 0:
            if data["Additional_Solar_Production " + str(sim_pv_size) + "kWp"].sum() > data["Grid2Home"].sum():
                additional_export = data["Additional_Solar_Production " + str(sim_pv_size) + "kWp"] - data["Grid2Home"]
                reduced_import = data["Additional_Solar_Production " + str(sim_pv_size) + "kWp"] - additional_export

                data["Grid2Home " + str(sim_pv_size) + "kWp"] = data["Grid2Home"] - reduced_import
                data["Home2Grid " + str(sim_pv_size) + "kWp"] = data["Home2Grid"] + additional_export
            else:
                data["Grid2Home " + str(sim_pv_size) + "kWp"] = data["Grid2Home"] - data["Additional_Solar_Production " + str(sim_pv_size) + "kWp"]
                data["Home2Grid " + str(sim_pv_size) + "kWp"] = data["Home2Grid"]
        else:
            data["Grid2Home " + str(sim_pv_size) + "kWp"] = data["Grid2Home"]
            data["Home2Grid " + str(sim_pv_size) + "kWp"] = data["Home2Grid"] + data["Additional_Solar_Production " + str(sim_pv_size) + "kWp"]

        data["HomeConsumption " + str(sim_pv_size) + "kWp"] = data["Grid2Home " + str(sim_pv_size) + "kWp"] + data["SolarProduction " + str(sim_pv_size) + "kWp"] - data["Home2Grid " + str(sim_pv_size) + "kWp"]

        return data

    def calculate_solar_selfconsumption(self, data, pv_size):
        # calculate the solar selfconsumption
        summe_solar = data["SolarProduction " + str(pv_size) + "kWp"].sum()
        summe_export = data["Home2Grid " + str(pv_size) + "kWp"].sum()
        solar_selfconsumption = (summe_solar - summe_export) / (summe_solar)
        return solar_selfconsumption