import pandas as pd
import os
from psycopg2 import DatabaseError, connect
from dotenv import load_dotenv
import tqdm

gdf = None

load_dotenv()
DBUSER = os.environ.get("DBUSER")
DATABASE = os.environ.get("DATABASE")
DBPASS = os.environ.get("DBPASS")

conn = connect(
    f"dbname={DATABASE} user={DBUSER} password={DBPASS} host=0.0.0.0 port=5432"
)
cur = conn.cursor()

provinces = {'NL': 0, 'PE': 1, 'NS': 2, 'NB': 3, 'QC': 4, 'ON': 5, 'MB': 6, 'SK': 7, 'AB': 8, 'BC': 9, 'YT': 10, 'NT': 11, 'NU': 12}

# Working (Mostly)
def loadProvinces():

    for provinceShort, provinceID in provinces.items():
        cur.execute(
            "INSERT INTO ProvinceLookupTable(ProvinceID, ProvinceShort) VALUES" +
            f"({provinceID}, '{provinceShort}') ON CONFLICT DO NOTHING;"
        )

    conn.commit()

def loadStations(stations):
    # Loads stations into lookup table

    for _, station in stations.iterrows():
        cur.execute(
            "INSERT INTO StationLookupTable(StationID, StationName) VALUES" +
            f"({station['Station ID']}, '{station['NameFormatted']}') ON CONFLICT DO NOTHING;"
        )

    conn.commit()

def loadBurnIncidents(burnIncidents):
    # Loads BurnIncidents into database

    weatherSet = set()

    for _, burnIncident in burnIncidents.iterrows():
        cur.execute(
            "INSERT INTO BurnIncident(ClosestStationID, FireLatitude, FireLongitude, FireProvinceShort, FireDate, HectaresBurnt) VALUES" +
            f"({burnIncident['ClosestStationID']}, {burnIncident['FireLatitude']}, {burnIncident['FireLongitude']}, '{burnIncident['FireProvinceShort']}', '{burnIncident['Date']}', {burnIncident['HectaresBurnt']})" +
            "ON CONFLICT DO NOTHING;"
        )
        weatherSet.add((burnIncident['ClosestStationID'], burnIncident['Date']))

    conn.commit()
    print(weatherSet)
    print(len(weatherSet))
    return weatherSet

import requests
import io
from contextlib import closing
from concurrent.futures import ProcessPoolExecutor, as_completed
def getWeatherInfo(stationNameIndex, stationID, date):
    stationName = stationNameIndex[stationID]
    url = f"https://api.weather.gc.ca/collections/climate-daily/items?f=csv&lang=en-CA&limit=10&skipGeometry=false&offset=0&LOCAL_DATE={date}&STATION_NAME={stationName}&properties=STATION_NAME,MEAN_TEMPERATURE,SPEED_MAX_GUST,MAX_REL_HUMIDITY,PROVINCE_CODE"
    #url = f"https://api.weather.gc.ca/collections/climate-daily/items?datetime={date}%2000:00:00/{date}%2000:00:00&STN_ID={stationID}&f=csv&limit=10&startindex=0"
    print(stationName, date)
    retries = 3
    while retries > 0:
        r = requests.get(url)

        with closing(requests.get(url, stream=True)) as r:
            try:
                r.raise_for_status()
                content = r.content

                weatherOnDay = pd.read_csv(io.StringIO(content.decode("utf-8")), low_memory=False).iloc[0]

                weatherRow = {
                    "StationID" : stationID,
                    "Date" : date,
                    "StationName" : weatherOnDay["STATION_NAME"],
                    "AverageTemp" : weatherOnDay["MEAN_TEMPERATURE"],
                    "MaxGust" : weatherOnDay["SPEED_MAX_GUST"],
                    "MaxRelHumidity" : weatherOnDay["MAX_REL_HUMIDITY"],
                    "ProvinceShort" : weatherOnDay["PROVINCE_CODE"]
                }

                return weatherRow, True
            except pd.errors.EmptyDataError:
                return (stationID, date), False
            except requests.exceptions.HTTPError:
                retries -= 1
            except Exception:
                retries -= 1

    return (stationID, date), False


def requestRequiredWeather(stationNameIndex, requiredWeather):
    # From entries of burnIncidents, compile a set of days of weather we need from each weather station

    weatherData = []
    failedRetrievals = []

    with ProcessPoolExecutor(max_workers=40) as e:
        futures = [e.submit(getWeatherInfo, stationNameIndex, stationID, date) for stationID, date in list(requiredWeather)]

        for future in as_completed(futures):
            returnedVal, successful = future.result()

            if successful:
                weatherData.append(returnedVal)
            else:
                failedRetrievals.append(returnedVal)
            print(len(futures), end='\r')

    return weatherData, failedRetrievals

def loadRequiredWeather(stationLatLongIndex, dailyWeather):
    # Loads the required weather into the database

    for _, weather in dailyWeather.iterrows():
        latitude, longitude = stationLatLongIndex[weather['StationID']]
        if weather['MaxGust'] == 'nan':
            weather['MaxGust'] = None
        if weather['MaxRelHumidity'] == 'nan':
            weather['MaxRelHumidity'] = None
        cur.execute(
            "INSERT INTO DailyWeather(StationID, WeatherDate, StationName, StationLatitude, StationLongitude, StationProvinceShort, AverageTemperature, AverageWindspeed, AverageHumidity) VALUES" +
            f"({weather['StationID']}, '{weather['Date']}', '{weather['StationName']}', {latitude}, {longitude}, '{weather['ProvinceShort']}', {weather['AverageTemp']}, {weather['MaxGust']}, {weather['MaxRelHumidity']}) ON CONFLICT DO NOTHING;"
        )

    conn.commit()

def loadYearlyCosts():
    # Loads the yearly cost data into the database

    yearlyLandCosts = pd.read_csv("../YearlyLandCost.csv")

    for _, landCost in yearlyLandCosts.iterrows():
        cur.execute(
            "INSERT INTO YearlyLandCost(ProvinceID, CostYear, CostProvinceShort, InflationScalar, DollarPerAcre, DollarPerHectare) VALUES" +
            f"({provinces[landCost['CostProvinceShort']]}, {landCost['Year']}, '{landCost['CostProvinceShort']}', {landCost['InflationScaler']}, {landCost['DollarPerAcre']}, {landCost['DollarPerHectare']}) ON CONFLICT DO NOTHING;"
        )

    conn.commit()

# TODO: Write this
def findCostOfBurn(hectaresBurnt, provinceID, fireYear):
    cost = -1
    return cost

def findWeatherStats(stationID, date):
    pass
    #return temp, humid, windspd

from datetime import datetime
def generateFactTable(burnIncidents, weatherData, yearlyCostData):
    # Generates the fact table

    for _, burnIncident in burnIncidents.iterrows():
        stationID = burnIncident["ClosestStationID"]
        provinceID = provinces[burnIncident['FireProvinceShort']]
        burnIncidentID = burnIncident['BurnIncidentID']
        fireProvinceShort = burnIncident['FireProvinceShort']
        fireDate = burnIncident['Date']
        hectaresBurnt = burnIncident['HectaresBurnt']
        averageTemp = -1
        averageHumid = -1
        averageWindspeed = -1 # Not in schema for some reason (dumb)
        cost = -1

        fireYear = datetime.strptime(fireDate, '%Y-%m-%d').year
        cost = findCostOfBurn(hectaresBurnt, provinceID, fireYear)
        averageTemp, averageHumid, averageWindspeed = findWeatherStats(stationID, fireDate)

        cur.execute(
            "INSERT INTO DailyBurnCost(StationID, BurnCostDate, ProvinceID, BurnIncidentID, FireProvinceShort, AverageTemperature, AverageHumidity, HectaresBurnt, Cost) VALUES" +
            f"({stationID}, '{fireDate}', '{provinceID}', {burnIncidentID}, {fireProvinceShort}, {averageTemp}, {averageHumid}, {hectaresBurnt}, {cost}) ON CONFLICT DO NOTHING;"
        )
        # TODO: Deal with the fact that some weather is not present

    conn.commit()

loadProvinces()
loadYearlyCosts()

stations = pd.read_csv("../Station Inventory EN.csv")

# Whoever decided that names should be used to index stations instead of IDs should be shot
stationNameIndex = {}
for _, station in stations.iterrows():
    stationNameIndex[station["Station ID"]] = station["Name"]

stations["NameFormatted"] = stations["Name"].str.replace("'", "")
loadStations(stations)

burnIncidents = pd.read_csv("../firedata_station.csv")
requiredWeather = loadBurnIncidents(burnIncidents)

#weatherData, failed = requestRequiredWeather(stationNameIndex, requiredWeather)
#weatherDF = pd.DataFrame(weatherData)
#weatherDF.to_csv("./relevantWeatherData.csv")
weatherDF = pd.read_csv("../relevantWeatherData.csv")

#print("FAILED RETRIEVALS")
#print(failed)

stationLatLongIndex = {}
for _, station in stations.iterrows():
    stationLatLongIndex[station["Station ID"]] = (station["Latitude"], station["Longitude"])

loadRequiredWeather(stationLatLongIndex, weatherDF)

yearlyLandCosts = pd.read_csv("../YearlyLandCost.csv")
#generateFactTable(burnIncidents, weatherDF, yearlyLandCosts)