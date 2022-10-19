import pandas
import numpy
import datetime
import math



def calculate_consumption(df):
    # calculate the power consumption per time step
    df["Consumption"] = df["Grid2Home"] + df["SolarProduction"] - df["Home2Grid"]
    return df

def simulate_solar(df, current_pv_size = 1.5, simulated_pv_size = 10.0):
    # for the day
    # normalize the solar production to 1kWp
    # multiply the normalized solar production with the simulated pv size

    date = df.columns.name.split(" ")[0]
    kWp = df.columns.name.split(" ")[1]
    kWh = df.columns.name.split(" ")[2]
    df.columns.name = date + " " + str(simulated_pv_size).zfill(2) + "kWp" + " " + str(kWh).zfill(2) + "kWh"
    df["SolarProduction"] = (df["SolarProduction"]/current_pv_size) * simulated_pv_size

    return df

