from contextlib import closing
import io
import geopandas as gpd
import pandas as pd
import requests

from download import DATA_DIR
from psycopg2 import connect
import os
from dotenv import load_dotenv

gdf = None

load_dotenv()
DBUSER=os.environ.get('DBUSER')
DATABASE=os.environ.get('DATABASE')
DBPASS=os.environ.get('DBPASS')

conn = connect(f"dbname={DATABASE} user={DBUSER} password={DBPASS} host=0.0.0.0 port=5432")
cur = conn.cursor()

def hotspots():
    row = {
        'LAT': 62.314,
        'LONG': -117.506,
        'DATE': '2015-06-26 05:25:00',
        'EST_AREA': 7.8
    }
    
    cur.execute(
        f"INSERT INTO BurnIncident (ClosestStationID, FireLatitude, FireLongitude, FireProvinceShort, FireDate, HectaresBurnt) VALUES (%s, %s, %s, %s, %s, %s)",
        (0, row['LAT'], row['LONG'], 'NL', row['DATE'], row['EST_AREA'])
    )

    for file in DATA_DIR.iterdir():
        year = int(file.name.split('_')[0])
        
        if file.suffix == '.shp':
            gdf = gpd.read_file(file)
            print(f"Read {file.name} with {len(gdf)} rows")
            for row in gdf.iterrows():
                print(row)

def daily_weather():
        
    # Reset the weather table
    cur.execute("DELETE FROM DailyWeather")
    conn.commit()
    
    # Reset the BurnIncident table
    cur.execute("DELETE FROM BurnIncident")
    conn.commit()
    
    for station_id in range(1, 55000):
        url = f"https://api.weather.gc.ca/collections/climate-daily/items?datetime=1840-03-01%2000:00:00/2024-03-19%2000:00:00&STN_ID={station_id}&sortby=PROVINCE_CODE,STN_ID,LOCAL_DATE&f=csv&limit=150000&startindex=0"
        # get CSV in memory
        r = requests.get(url)
        
        with closing(requests.get(url, stream=True)) as r:
            r.raise_for_status()
            content = r.content
            try:
                df = pd.read_csv(io.StringIO(content.decode('utf-8')))
                print("Station", station_id, "has", len(df), "rows")
                
                for i, row in df.iterrows():
                    # print(row)
                    cur.execute("INSERT INTO DailyWeather (WeatherDate, StationName, StationLongitude, StationLatitude, StationProvinceShort, AverageTemperature, AverageWindspeed, AverageHumidity) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                                (row['LOCAL_DATE'], row['STATION_NAME'], row['x'], row['y'], row['PROVINCE_CODE'], row['MEAN_TEMPERATURE'], row['SPEED_MAX_GUST'], 0)
                    )
                    conn.commit()
            except pd.errors.EmptyDataError:
                print(f"Station {station_id} has no data")
                continue
                
if __name__ == "__main__":
    daily_weather()