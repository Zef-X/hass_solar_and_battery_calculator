# fetch data from "https://homeassistant.local:8123/" via the RESTful API
import client
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import datetime
import math
import os
from tqdm import tqdm

# ----------------- CONFIGURATION -----------------

# Home Assistant Token and URL
TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI1Mzc4YWUzMWFlMTI0MTZmOTYzNTNlNDM1ZWZjNzhkNSIsImlhdCI6MTY2NTMzODYwMywiZXhwIjoxOTgwNjk4NjAzfQ.UDjuxN_JZb5ddIA34ZY2hOOfdDWlskvpXrb6AeY4qIk"
URL = "http://homeassistant.local:8123/"

# Retrieve data since...
FIRST_DATE = "2022-10-20"
#FIRST_DATE = "2022-10-10"

# Sensors to retrieve - Grid2Home, Home2Grid, Solarproduction (Watt)
SENSOR_NAMES = ["sensor.g2h_v6_power", "sensor.h2g_v6_power", "sensor.shelly1pm_244cab441f01_power"]

# installed and max possible PV capacity
CURRENT_PV_SIZE = 1.5
MAX_PV_CAPACITY = 15.0
MAX_BATTERY_SIZE = 15.0

def wipe_cache():
    # check if there is a cache folder
    if os.path.isdir("cache"):
        for file in os.listdir("cache"):
            if file.endswith(".csv"):
                os.remove(os.path.join("cache", file))
                print("Deleted: " + file)
    else:
        print("No cache found")

def load_from_cache(filename):
    try:
        print("Loading data from cache: " + filename)
        return pd.read_csv(filename, sep=";")
    except FileNotFoundError:
        print("No cache found for " + filename)
        return None

def save_to_cache(filename, data):
    # round everything to 2 decimal places
    data.to_csv(filename, sep=';', index=False)

def calculate_solar_production(max_pv_size = 10.0, current_pv_size = 1.0, battery_size = 0, recalculate = False):
    for file in os.listdir("cache"):
        if file.endswith("Baseline kWp.csv"):
            print(os.path.join("loading File" + "cache", file))
            df_baseline = load_from_cache(os.path.join("cache", file))
            date_str = (file.split(" ")[0])


            for new_pv_size in range(math.ceil(current_pv_size), int(max_pv_size)):
                df = df_baseline.copy()
                # calculate powerconsumption
                df["Powerconsumption"] = df["Grid2Home"] + df["SolarProduction"] - df["Home2Grid"]
                df["SolarProduction"] = df["SolarProduction"] * (new_pv_size / current_pv_size)
                net_flow = df["Powerconsumption"] - df["SolarProduction"]

                # if net_flow is positive, Grid2Home = abs(net_flow), else Home2Grid = abs(net_flow)
                df["Grid2Home"] = net_flow.apply(lambda x: abs(x) if x > 0 else 0)
                df["Home2Grid"] = net_flow.apply(lambda x: abs(x) if x < 0 else 0)

                df["NetFlow"] = df["Powerconsumption"] - df["SolarProduction"]

                # save to cache
                # "2022-10-11 1_5kWp 0_0kWh.csv"
                folder = "cache"
                filename = date_str + " " + str(new_pv_size).zfill(2) + "kWp " + str(battery_size).zfill(2) + "kWh.csv"
                save_to_cache(os.path.join("cache", filename), df)

def concentrate_data(max_pv_size = 10.0, current_pv_size = 1.0, for_size=None):
    # load all files in cache in correct order and make one large df

    if for_size is not None:
        current_pv_size = for_size
        max_pv_size = for_size + 1

    for pv_size in range(math.ceil(current_pv_size), int(max_pv_size)):
        df = pd.DataFrame()
        files = os.listdir("cache")
        files = [file for file in files if file.endswith(".csv")]

        print("pv_size: " + str(pv_size))
        # get all files for this pv_size

        print("Seraching for: "+ str(pv_size).zfill(2) + "kWp 00kWh.csv")
        files = [file for file in files if file.endswith(str(pv_size).zfill(2) + "kWp 00kWh.csv")]
        files.sort(key=lambda x: datetime.datetime.strptime(x.split(" ")[0], "%Y-%m-%d"))

        for file in files:
            print("Loading file: " + file)
            df = df.append(load_from_cache(os.path.join("cache", file)), ignore_index=True)

        # save to cache
        # filename should be "Date - Date 05kWp.csv"
        folder = "cache"
        filename = files[0].split(" ")[0] + " - " + files[-1].split(" ")[0] + " " + str(pv_size).zfill(2) + "kWp.csv"
        save_to_cache(os.path.join("cache", filename), df)


        # delete all files for this pv_size
        for file in files:
            os.remove(os.path.join("cache", file))
            print("Deleted: " + file)

        print("")

    print("Done concentrating data")
    print("Deleting Baseline files")
    for file in os.listdir("cache"):
        if file.endswith("Baseline kWp.csv"):
            os.remove(os.path.join("cache", file))
            print("Deleted: " + file)


    if for_size is not None:
        return df


def calculate_selfconsumption(max_pv_size = 10.0, current_pv_size = 1.0, battery_size = 0):
    # for every file in cahce without "Baseline" in name
    for file in os.listdir("cache"):
        # check if filename contains "Baseline" or "0kWh"
        if file.endswith("Baseline kWp.csv"):
            print("Skipping file: " + file)
        else:
            df = load_from_cache(os.path.join("cache", file))

            # calculate selfconsumption
            df["Selfconsumption"] = (df["SolarProduction"].sum() - df["Home2Grid"].sum()) / df["SolarProduction"].sum()
            print("Selfconsumption at pv_size " + file.split(" ")[3] + ": " + str(df["Selfconsumption"].mean()))

            # save to cache
            save_to_cache(os.path.join("cache", file), df)
            print("")

    print("Done calculating selfconsumption")


def calculate_battery(max_pv_size = 10.0, current_pv_size = 1.0, max_battery_size = 10, time=5.0):
    for file in tqdm(os.listdir("cache")):
        # check if filename contains "Baseline" or "0kWh"
        if not file.endswith("Baseline kWp.csv") or file.endswith("01kWh.csv"):
            date_first = file.split(" ")[0]
            date_last = file.split(" ")[2]
            pv_size = (file.split(" ")[3])[0:2]

            print("Loading file: " + file)
            df = load_from_cache(os.path.join("cache", file))

            # if column "Selfconsumption" does exist, rename to "Selfconsumption Solar"
            if "Selfconsumption" in df.columns:
                df = df.rename(columns={"Selfconsumption": "Selfconsumption Solar"})

            df_fresh = df

            for battery_capacity in tqdm(range(1, math.ceil(max_battery_size))):
                df = df_fresh.copy()
                print("Calculating battery capacity: " + str(battery_capacity) + "kWh")
                battery_capacity = (((60/time)*60)*1000)*battery_capacity # converting kWh to "Watt per 5 Seconds" because this is the time resolution of the data
                df["Battery SoC"] = 0 # this is the battery state of charge in "Watt per 5 Seconds" instead of kWh
                df["Battery Flow"] = 0 # this is the battery flow in Watt

                for index, row in df.iterrows():
                    if index == 0:
                        df.at[index, "Battery SoC"] = 0
                    else:
                        df.at[index, "Battery SoC"] = df.at[index-1, "Battery SoC"] + df.at[index-1, "Home2Grid"] - df.at[index-1, "Grid2Home"]
                        #df.at[index, "Battery SoC"] = df.at[index - 1, "Battery SoC"] - df.at[index - 1, "NetFlow"]
                        if df.at[index, "Battery SoC"] > battery_capacity:
                            df.at[index, "Battery SoC"] = battery_capacity
                            df.at[index, "Battery Flow"] = 0
                        elif df.at[index, "Battery SoC"] < 0:
                            df.at[index, "Battery SoC"] = 0
                            df.at[index, "Battery Flow"] = 0
                        else:
                            # Battery Flow is inverted NetFlow
                            try:
                                df.at[index, "Battery Flow"] = -df.at[index, "NetFlow"]
                                df.at[index, "NetFlow"] = 0
                            except:
                                df.at[index, "Battery Flow"] = 0

                # recalculate Grid2Home and Home2Grid
                # if "NetFlow" is positive, it is Grid2Home
                # if "NetFlow" is negative, it is Home2Grid
                df["Grid2Home"] = 0
                df["Home2Grid"] = 0
                for index, row in df.iterrows():
                    if df.at[index, "NetFlow"] > 0:
                        df.at[index, "Grid2Home"] = df.at[index, "NetFlow"]
                    else:
                        try:
                            df.at[index, "Home2Grid"] = -df.at[index, "NetFlow"]
                        except:
                            df.at[index, "Home2Grid"] = 0


                # convert "Watt per 5 Seconds" to kWh rounded to 2 decimals
                df["Battery SoC"] = df["Battery SoC"] / ((60/time)*60 * 1000)
                df["Battery SoC"] = df["Battery SoC"].round(4)
                battery_capacity = battery_capacity / ((60/time)*60 * 1000)

                # save the file with the correct name to cache
                # filename should be "Date - Date 05kWp 05kWh.csv"
                folder = "cache"
                filename = date_first + " - " + date_last + " " + pv_size + "kWp " + str(int(battery_capacity)).zfill(2) + "kWh.csv"
                save_to_cache(os.path.join("cache", filename), df)

        else:
            print("Looks like a baseline file, skipping: " + file)

def calculate_autonomy():
    # for every file in cahce without "Baseline" in name
    for file in os.listdir("cache"):
        # check if filename contains "Baseline" or "0kWh"
        if file.endswith("Baseline kWp.csv"):
            print("Skipping file: " + file)
        else:
            df = load_from_cache(os.path.join("cache", file))
            # filename looks like "2022-10-09 - 2022-10-13 02kWp 01kWh.csv"
            pv_size_and_battery_size = file.split(" ")[3].split(".")[0]
            pv_size = pv_size_and_battery_size.split("kWp")[0]
            battery_size = pv_size_and_battery_size.split("kWp")[1].split("kWh")[0]

            # calculate selfconsumption
            df["Autonomy"] = (df["SolarProduction"].sum() - df["Home2Grid"].sum()) / df["Powerconsumption"].sum()
            print("Autonomy at pv_size " + pv_size + " and battery_size " + battery_size + ": " + str(df["Autonomy"].mean()))

            # save to cache
            save_to_cache(os.path.join("cache", file), df)
            print("")

    print("Done calculating selfconsumption")

def create_matrix_autonomy(max_pv_size = 10.0, current_pv_size = 1.0, battery_size = 10.0):
    if os.path.exists(os.path.join("cache", "Matrix Autonomy.csv")):
        print("File already exists, deleting")
        os.remove(os.path.join("cache", "Matrix Autonomy.csv"))

    # add to every filename ending "kWp" a " 00kWh"
    for file in os.listdir("cache"):
        if file.endswith("kWp.csv"):
            new_filename = file.split("kWp")[0] + "kWp 00kWh.csv"
            os.rename(os.path.join("cache", file), os.path.join("cache", new_filename))

    # create a df with x-axis: pv_size, y-axis: battery_size and fill it with the mean of the autonomy
    df = pd.DataFrame(index=range(math.ceil(current_pv_size), int(max_pv_size)), columns=range(0, int(battery_size)))
    for file in os.listdir("cache"):
        # check if filename contains "Baseline" or "0kWh"
        if not file.endswith("Baseline kWp 00kWh.csv") or not file.endswith("Matrix Autonomy.csv"):
            df_file = load_from_cache(os.path.join("cache", file))

            # filename looks like "2022-10-09 - 2022-10-13 02kWp 01kWh.csv"
            pv_size = int(file.split(" ")[3].split("kWp")[0])
            battery_size = int(file.split("kWp")[1].split("kWh")[0])

            df.at[pv_size, battery_size] = df_file["Autonomy"].mean()
        else:
            print("Looks like a baseline file, skipping: " + file)

    # save to cache
    save_to_cache(os.path.join("cache", "Matrix Autonomy.csv"), df)

    # print the pv_size and battery_size with the highest autonomy
    #print("pv_size: " + str(df.idxmax(axis=0).idxmax()) + ", battery_size: " + str(df.idxmax(axis=0).max()) + ", autonomy: " + str(df.max(axis=0).max()))

def calculate_money(time=5.0):
    # for every file in cahce without "Baseline" in name
    for file in os.listdir("cache"):
        # check if filename contains "Baseline" or "0kWh"
        if file.endswith("Baseline kWp.csv"):
            print("Skipping file: " + file)
        else:
            df = load_from_cache(os.path.join("cache", file))
            # filename looks like "2022-10-09 - 2022-10-13 02kWp 01kWh.csv"
            pv_size_and_battery_size = file.split(" ")[3].split(".")[0]
            pv_size = pv_size_and_battery_size.split("kWp")[0]
            battery_size = pv_size_and_battery_size.split("kWp")[1].split("kWh")[0]

            # convert "Watt per 5 Seconds" to kWh rounded to 2 decimals
            df["Grid2Home"] = df["Grid2Home"] / ((60/time)*60 / 1000)
            df["Grid2Home"] = df["Grid2Home"].round(2)

            df["Home2Grid"] = df["Home2Grid"] / ((60/time)*60 / 1000)
            df["Home2Grid"] = df["Home2Grid"].round(2)

            import_price = 0.35
            export_price = 0.0875

            df["Grid2Home-Money"] = df["Grid2Home"].sum() * import_price
            df["Home2Grid-Money"] = df["Home2Grid"].sum() * export_price

            df["Absolut Cost"] = df["Grid2Home-Money"] - df["Home2Grid-Money"]

            # calculate cost per year by rule of three
            # convert "last_changed" to datetime
            df["last_changed"] = pd.to_datetime(df["last_changed"])
            days = (df["last_changed"].max() - df["last_changed"].min()).days
            df["Cost per Year"] = df["Absolut Cost"] / days * 365

            print(df["Absolut Cost"].mean())
            print(df["Cost per Year"].mean())

            print("Expected cost at pv_size " + pv_size + " and battery_size " + battery_size + ": " + str(df["Cost per Year"].mean()))

            # save to cache
            save_to_cache(os.path.join("cache", file), df)
            print("")

    print("Done calculating money")

def convert_to_excel():
    # convert the "Matrix Autonomy.csv" to an readable .csv
    df = load_from_cache(os.path.join("cache", "Matrix Autonomy.csv"))
    df = df.round(2)
    df.to_csv(os.path.join("cache", "Matrix Autonomy.csv"), sep=";", decimal=",")

if __name__ == '__main__':
    # wipe cache
    #wipe_cache()

    # conect zu HomeAssistant
    #hass = client.client(URL, TOKEN, CURRENT_PV_SIZE)

    # get the data from the API and save it to a csv file in the cache folder
    #df = hass.cache_data(SENSOR_NAMES, FIRST_DATE)

    # calculate the solar production for each day
    #calculate_solar_production(MAX_PV_CAPACITY, CURRENT_PV_SIZE)

    # concentrate the data for each pv_size to a single file per theoretical pv_size
    #concentrate_data(MAX_PV_CAPACITY, CURRENT_PV_SIZE)

    # calculate the self-consumption before battery
    #calculate_selfconsumption(MAX_PV_CAPACITY, CURRENT_PV_SIZE)

    # calculate the Battery Flow, Battery SoC and new NetFlow
    #calculate_battery(MAX_PV_CAPACITY, CURRENT_PV_SIZE, MAX_BATTERY_SIZE)

    # recalculate the self-consumption after battery
    #calculate_selfconsumption(MAX_PV_CAPACITY, CURRENT_PV_SIZE)

    # calculate autonomy
    #calculate_autonomy()

    # show the matrix of autonomy
    create_matrix_autonomy(MAX_PV_CAPACITY, CURRENT_PV_SIZE, MAX_BATTERY_SIZE)

    # convert the "Matrix Autonomy.csv" to a excel-readable .csv
    convert_to_excel()

    # calculate the money
    calculate_money()



