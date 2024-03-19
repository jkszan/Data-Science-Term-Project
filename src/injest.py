from contextlib import closing
import datetime
import io
import geopandas as gpd
import pandas as pd
import requests
import os
import math

from download import DATA_DIR
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


def haversine(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance in kilometers between two points
    on the earth (specified in decimal degrees)
    """
    # Convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    r = 6371  # Radius of Earth in kilometers
    return c * r


def get_nearest_station(
    fire_lat: float, fire_lon: float, fire_date: datetime.date, conn, prov: bool = False
):
    """
    Find the nearest weather station to a given burn incident location.
    """
    try:
        cursor = conn.cursor()
        query = """
        SELECT DISTINCT ON (StationID) StationID, StationLatitude, StationLongitude, StationProvinceShort
        FROM DailyWeather
        WHERE WeatherDate BETWEEN %s and %s
        """

        cursor.execute(
            query,
            (
                fire_date - datetime.timedelta(days=1),
                fire_date + datetime.timedelta(days=1),
            ),
        )
        stations = cursor.fetchall()

        min_distance = float("inf")
        nearest_station_id = None

        for station in stations:
            station_id, station_lat, station_lon, province = station
            distance = haversine(fire_lat, fire_lon, station_lat, station_lon)
            if distance < min_distance:
                min_distance = distance
                nearest_station_id = station_id
                nearest_province = province

        return (
            nearest_station_id if not prov else (nearest_station_id, nearest_province)
        )
    except (Exception, DatabaseError) as error:
        print(error)
    finally:
        if cursor is not None:
            cursor.close()


def hotspots():

    row = {
        "LAT": 62.314,
        "LON": -117.506,
        "REP_DATE": "2015-06-26 05:25:00",
        "ESTAREA": 7.8,
    }

    for file in tqdm.tqdm(list(DATA_DIR.iterdir())):
        year = int(file.name.split("_")[0])

        if file.suffix == ".shp":
            gdf = gpd.read_file(file)
            for i, row in gdf.iterrows():
                print(f"Loading row {i} of {len(gdf)}", end="\r")
                station, province = get_nearest_station(
                    row["LAT"],
                    row["LON"],
                    datetime.datetime.strptime(row["REP_DATE"], "%Y-%m-%d %H:%M:%S").date(),
                    conn,
                    prov=True,
                )
                cur.execute(
                    f"INSERT INTO BurnIncident (ClosestStationID, FireLatitude, FireLongitude, FireProvinceShort, FireDate, HectaresBurnt) VALUES (%s, %s, %s, %s, %s, %s)",
                    (
                        station,
                        row["LAT"],
                        row["LON"],
                        province,
                        row["REP_DATE"],
                        row["ESTAREA"],
                    ),
                )
                conn.commit()


def daily_weather():

    # Reset the weather table
    # cur.execute("DELETE FROM DailyWeather")
    # conn.commit()

    # # Reset the BurnIncident table
    # cur.execute("DELETE FROM BurnIncident")
    # conn.commit()

    for station_id in range(1, 55000):
        url = f"https://api.weather.gc.ca/collections/climate-daily/items?datetime=1840-03-01%2000:00:00/2024-03-19%2000:00:00&STN_ID={station_id}&sortby=PROVINCE_CODE,STN_ID,LOCAL_DATE&f=csv&limit=150000&startindex=0"
        # get CSV in memory
        r = requests.get(url)

        with closing(requests.get(url, stream=True)) as r:
            r.raise_for_status()
            content = r.content
            try:
                df = pd.read_csv(io.StringIO(content.decode("utf-8")), low_memory=False)
                print(f"Station {station_id} has {len(df)} rows. ", end=" ")

                for _, row in df.iterrows():
                    cur.execute(
                        "INSERT INTO DailyWeather (WeatherDate, StationName, StationLongitude, StationLatitude, StationProvinceShort, AverageTemperature, AverageWindspeed, AverageHumidity) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                        (
                            row["LOCAL_DATE"],
                            row["STATION_NAME"],
                            row["x"],
                            row["y"],
                            row["PROVINCE_CODE"],
                            row["MEAN_TEMPERATURE"],
                            row["SPEED_MAX_GUST"],
                            0,
                        ),
                    )
                    conn.commit()

                print("Inserted", len(df), "rows")

            except pd.errors.EmptyDataError:
                print(f"Station {station_id} has no/bad data")
                continue


if __name__ == "__main__":
    # daily_weather()
    print(get_nearest_station(62.314, -17.506, datetime.date(1965, 6, 1), conn, prov=True))
    hotspots()
