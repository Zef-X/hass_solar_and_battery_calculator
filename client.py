import pandas as pd
import requests
import datetime
import os

class client:
    def __init__(self, url, token, current_pv_size):
        self.url = url
        self.token = token
        self.SAMPLING_RATE = "5S"
        self.current_pv_size = current_pv_size

        self.headers = {
            "Authorization": "Bearer " + self.token,
            "content-type": "application/json",
        }

    def cache_data(self,
                    sensors,
                    first_date_str = datetime.date.today().isoformat(),
                    last_date_str = datetime.date.today().isoformat()):

        print("Downloading data from " + first_date_str + " to " + last_date_str)

        first_date_str = first_date_str + "T00:00:00.000000+00:00"
        last_date_str = last_date_str + "T00:00:00.000000+00:00"

        # convert first_date_str and last_date_str to datetime objects in format %Y-%m-%dT%H:%M:%S.%fZ
        first_date = datetime.datetime.strptime(first_date_str, "%Y-%m-%dT%H:%M:%S.%f+00:00")
        last_date = datetime.datetime.strptime(last_date_str, "%Y-%m-%dT%H:%M:%S.%f+00:00")


        for date in range((last_date - first_date).days + 1):
            date = (first_date + datetime.timedelta(days=date)).isoformat()

            df = self.fetch_data(sensors, date)
            df = self.correct_data(sensors, df)

            self.save_data(df, self.current_pv_size)

    def fetch_data(self, sensors, date):
        df = pd.DataFrame(columns=sensors)
        for sensor in sensors:
            url = self.url + "api/history/period/" + date + "?filter_entity_id=" + sensor
            response = requests.get(url, headers=self.headers, verify=False)
            data = response.json()

            for item in data:
                df = df.append(item, ignore_index=True)

        # save df
        df.to_csv("df.csv", sep=";")
        df = df.pivot(index="last_changed", columns="entity_id", values="state")
        df.index = pd.to_datetime(df.index)
        df = df.sort_index()

        df = df[df != "unknown"]
        df = df[df != "unavailable"]
        df = df.astype(float)

        return df

    def correct_data(self, sensors, data):
        print("Correcting data")
        # correct data for original 5 second interval and interpolate missing values
        df = data.resample(self.SAMPLING_RATE).nearest(limit=1).interpolate(method="linear")

        # rename first sensor to "Grid2Home", "Home2Grid" and "SolarProduction"
        df = df.rename(columns={sensors[0]: "Grid2Home", sensors[1]: "Home2Grid", sensors[2]: "SolarProduction"})

        return df

    def save_data(self, data, pv_size, battery_size=0.0, folder="cache"):
        # create a folder for the data in current directory
        if not os.path.exists(folder):
            os.makedirs(folder)

        # convert pv_size to string and replace . with _
        pv_size = str(pv_size).replace(".", "_")
        battery_size = str(battery_size).replace(".", "_")

        # save the data to a csv file
        # "2022-10-11 1_5kWp 0_0kWh.csv"
        #filename = folder + "/" + data.index[0].strftime("%Y-%m-%d") + " " + pv_size + "kWp " + battery_size + "kWh.csv"
        filename = folder + "/" + data.index[0].strftime("%Y-%m-%d") + " Baseline kWp.csv"
        data.to_csv(filename, sep=";")
        print("Saved data to " + filename)













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