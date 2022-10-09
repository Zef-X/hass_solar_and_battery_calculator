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

        return real_data