import datetime

import requests as req
import pandas as pd
from pymongo import MongoClient

URL = 'https://api.misoenergy.org/MISORTWDDataBroker/DataBrokerServices.asmx?messageType=getWindForecast&returnType=json'

def get_forecast_data(url=URL):
    res = req.get(url)
    json_data = res.json()['Forecast']
    days = json_data['Forecast']

    df = pd.io.json.json_normalize(days[0])
    for day in days[1:]:
        df = df.append(pd.io.json.json_normalize(day))

    return df


def clean_df(df):
    # converting datetimes datatypes
    df['DateTimeEST'] = pd.to_datetime(df['DateTimeEST'], utc=True)
    # drop unneccesary columns
    df.drop(['HourEndingEST'], inplace=True, axis=1)
    return df

def store_data(df):
    client = MongoClient()
    db = client['day_ahead']
    collection = db['forecast']
    collection.insert_many(df.to_dict('records'))
    client.close()

def hourly_scrape():

        while True:
            utc_time = datetime.datetime.utcnow()
            if utc_time.minute == 0:
                df = get_weather_data()
                df = clean_df(df)
                store_data(df)
                time.sleep(60)  # sleep sixty seconds to ensure we don't double-scrape

            time.sleep(60)

if __name__ == "__main__":
    df = get_weather_data()
    df = clean_df(df)
    store_data(df)
