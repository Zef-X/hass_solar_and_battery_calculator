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

        self.SAMPLING_RATE = "5S"

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
        real_data = real_data[real_data != "unavailable"]
        real_data = real_data.astype(float)

        # correct data for original 5 second interval and interpolate missing values
        real_data = real_data.resample(self.SAMPLING_RATE).nearest(limit=1).interpolate(method="linear")

        # rename the columns "Grid2Home", "Home2Grid" and "SolarProduction"
        real_data = real_data.rename(columns={"sensor.g2h_v6_power": "Grid2Home", "sensor.h2g_v6_power": "Home2Grid", "sensor.shelly1pm_244cab441f01_power": "SolarProduction"})

        real_data = self.get_consumption(real_data)

        return real_data

    def get_consumption(self, data):
        # calculate the consumption
        data["HomeConsumption"] = data["Grid2Home"] + data["SolarProduction"] - data["Home2Grid"]

        return data

    def simulate_data_solar_only_old(self, data, current_pv_size, sim_pv_size):
        # normalize the data
        data["SolarProduction " + str(sim_pv_size) + "kWp"] = (data["SolarProduction"]/current_pv_size) * sim_pv_size
        data["SolarProduction " + str(sim_pv_size) + "kWp"] = (data["SolarProduction"] / current_pv_size) * 1.5
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

    def simulate_data_solar_only(self, data, current_pv_size, sim_pv_size):
        # adjust production to pv_size
        data["SolarProduction " + str(sim_pv_size) + "kWp"] = (data["SolarProduction"]/current_pv_size) * sim_pv_size
        data["Additional_Solar_Production " + str(sim_pv_size) + "kWp"] = data["SolarProduction " + str(sim_pv_size) + "kWp"] - data["SolarProduction"]

        # for every row, check if the additional solar production is higher than the grid2home
        # if yes, then reduce the grid2home and increase the home2grid
        data["Grid2Home " + str(sim_pv_size) + "kWp"] = data["Grid2Home"]
        data["Home2Grid " + str(sim_pv_size) + "kWp"] = data["Home2Grid"]

        for index, row in data.iterrows():
            if row["Additional_Solar_Production " + str(sim_pv_size) + "kWp"] > row["Grid2Home"]:
                additional_export = row["Additional_Solar_Production " + str(sim_pv_size) + "kWp"] - row["Grid2Home"]
                reduced_import = row["Additional_Solar_Production " + str(sim_pv_size) + "kWp"] - additional_export

                data.at[index, "Grid2Home " + str(sim_pv_size) + "kWp"] = row["Grid2Home"] - reduced_import
                data.at[index, "Home2Grid " + str(sim_pv_size) + "kWp"] = row["Home2Grid"] + additional_export

        # calculate power consumption as a checksum
        data["HomeConsumption " + str(sim_pv_size) + "kWp"] = data["Grid2Home " + str(sim_pv_size) + "kWp"] + data["SolarProduction " + str(sim_pv_size) + "kWp"] - data["Home2Grid " + str(sim_pv_size) + "kWp"]

        return data

    def simulate_data_battery(self, data, current_pv_size, sim_pv_size, battery_size):
        # check if there is data for sim_pv_size
        if "SolarProduction " + str(sim_pv_size) + "kWp" not in data.columns:
            data = self.simulate_data_solar_only(data, current_pv_size, sim_pv_size)

        # simulate battery
        Battery_SoC = "Battery_SoC " + str(sim_pv_size) + "kWp " + str(battery_size) + "kWh"
        Battery_Charge = "Battery_Charge " + str(sim_pv_size) + "kWp " + str(battery_size) + "kWh"
        Battery_Discharge = "Battery_Discharge " + str(sim_pv_size) + "kWp " + str(battery_size) + "kWh"
        Battery_Efficiency = 0.9

        data[Battery_SoC] = 0
        data[Battery_Charge] = 0
        data[Battery_Discharge] = 0

        # if Export>0
            # if SoC < BatterySize
                #Charge Battery
            # else
                # Export

        #df['equal_or_lower_than_4?'] = df['set_of_numbers'].apply(lambda x: 'True' if x <= 4 else 'False')
        data[Battery_Charge] = data["Battery_SoC " + str(sim_pv_size) + "kWp " + str(battery_size) + "kWh"].apply(lambda x: 0 if x < 0 else x)

    def simulate_data_solar_only_new(self, data, current_pv_size, sim_pv_size):
        # adjust production to pv_size
        sim_solar_production = (data["SolarProduction"]/current_pv_size) * sim_pv_size
        sim_additional_solar_production = sim_solar_production - data["SolarProduction"]

        # for every row, check if the additional solar production is higher than the grid2home
        # if yes, then reduce the grid2home and increase the home2grid
        sim_import = data["Grid2Home"]
        sim_export = data["Home2Grid"]

        for index, row in data.iterrows():
            if (sim_additional_solar_production > row["Grid2Home"]):
                sim_additional_export = sim_additional_solar_production - row["Grid2Home"]
                sim_reduced_import = sim_additional_solar_production - additional_export

                sim_import = row["Grid2Home"] - sim_reduced_import
                sim_export = row["Home2Grid"] + sim_additional_export

        # calculate power consumption as a checksum
        sim_home_consumption = sim_import + sim_solar_production - sim_export

        return data

    def get_sum_import(self, data, pv_size):
        return round(data["Grid2Home " + str(pv_size) + "kWp"].sum() / 720 / 1000, 2)

    def get_sum_export(self, data, pv_size):
        return round(data["Home2Grid " + str(pv_size) + "kWp"].sum() / 720 / 1000, 2)

    def get_sum_consumption(self, data, pv_size):
        return round(data["HomeConsumption " + str(pv_size) + "kWp"].sum() / 720 / 1000, 2)

    def get_sum_solar_production(self, data, pv_size):
        return round(data["SolarProduction " + str(pv_size) + "kWp"].sum() / 720 / 1000, 2)

    def calculate_solar_selfconsumption(self, data, pv_size):
        print("calculating self-consumption for " + str(pv_size) + "kWp")
        return (self.get_sum_solar_production(data, pv_size) - self.get_sum_export(data, pv_size)) / self.get_sum_solar_production(data, pv_size)

    def calculate_net_neutrality(self, data, pv_size):
        print("calculating net neutrality for " + str(pv_size) + "kWp")
        return self.get_sum_export(data, pv_size) / self.get_sum_import(data, pv_size)