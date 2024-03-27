import os
from pathlib import Path
from typing import List, Optional
import pandas as pd
from tqdm import tqdm
from land_cost_preprocess import pre_process_landcost
import shutil


"""
Handle the pre-processing of the data

There is a single common point between the API, and hostpots data is the
Climate ID. This is used to match the data between the two sources.
This will not be injested into the database so we link the primary key
of the station lookup table to replace Climate ID as the link.

We connect the hotspots and stations using the rust
"""

DAILY_WEATHER_OUTPUT_PATH: Path = Path("../data/weather.csv")
WEATHER_DIR_IN: Path = Path("../data/weather")
WEATHER_DIR_STATION_IDS: Path = Path("../data/weather_station_ids")
HOTSPOT_FILE: Path = Path("../data/firedata_station.csv")
HOTSPOT_FILE_WITH_CLIMATE_ID: Path = Path("../data/firedata_station_with_climate_id.csv")
STATION_INVENTORY_PATH: Path = Path("../data/station_inventory.csv")
STATION_LOOKUP_PATH: Path = Path("../data/station_lookup.csv")
DATAMART_STATION_LOOKUP_PATH: Path = Path("./Datamart/station_lookup_table.csv")
DATAMART_DAILY_WEATHER_PATH: Path = Path("./Datamart/daily_weather_table.csv")
DATAMART_HOTSPOT_PATH: Path = Path("./Datamart/hotspot_table.csv")
DATAMART_PROVINCE_LOOKUP_PATH: Path = Path("./Datamart/province_lookup.csv")
LAND_COST_FILE: Path = Path("../data/land_cost.csv")
DATAMART_LAND_COST_TABLE_PATH: Path = Path("./Datamart/land_cost_table.csv")
DATAMART_DAILY_BURN_COST_PATH: Path = Path("./Datamart/daily_burn.csv")

def create_station_lookup() -> pd.DataFrame:
    columns = ["Climate ID", "Name", "Latitude", "Longitude", "Province"]
    df = pd.read_csv("../data/station_inventory.csv")
    df = df[columns]
    # Map latitude and longitude to their decimal values
    convert_to_decimal = lambda x: x / 1e7
    df["Latitude"] = df["Latitude"].apply(convert_to_decimal)
    df["Longitude"] = df["Longitude"].apply(convert_to_decimal)

    # Map province to its abbreviation
    province_map = {
        "ALBERTA": "AB",
        "BRITISH COLUMBIA": "BC",
        "MANITOBA": "MB",
        "NEW BRUNSWICK": "NB",
        "NEWFOUNDLAND": "NL",
        "NORTHWEST TERRITORIES": "NT",
        "NOVA SCOTIA": "NS",
        "NUNAVUT": "NU",
        "ONTARIO": "ON",
        "PRINCE EDWARD ISLAND": "PE",
        "QUEBEC": "QC",
        "SASKATCHEWAN": "SK",
        "YUKON TERRITORY": "YT",
    }
    df["Province"] = df["Province"].apply(lambda x: province_map[x])

    # Add index column "Station ID"
    df["Station ID"] = range(len(df))

    df.to_csv(STATION_LOOKUP_PATH, index=False)

    return df


def discover_files(path: Path) -> List[Path]:
    # Find the list of weather files in the data directory
    files = [file for file in path.iterdir() if file.suffix == ".csv" and file.stat().st_size > 0]
    return files


def find_station_id(station_lookup: pd.DataFrame, climate_id: str) -> Optional[int]:
    assert "Climate ID" in station_lookup.columns, "Climate ID column not found in station lookup table"
    assert type(climate_id) == str, "Climate ID must be a string"

    station = station_lookup[station_lookup["Climate ID"] == climate_id]
    if len(station) == 0:
        return None
    return station["Station ID"].values[0]


def match_climate_ids(station_lookup: pd.DataFrame, weather_files: List[Path]) -> None:
    # Load the climate data
    for file in tqdm(weather_files):
        df = pd.read_csv(file)

        if "Station ID" in df.columns:
            continue

        # rename the columns to match the station lookup table
        columns = {
            "STATION_NAME": "Name",
            "CLIMATE_IDENTIFIER": "Climate ID",
            "x": "Longitude",
            "y": "Latitude",
            "PROVINCE_CODE": "Province",
            "LOCAL_DATE": "Date",
        }
        df.rename(columns=columns, inplace=True)

        assert "Climate ID" in df.columns, "Climate ID column not found in weather file"
        assert "Date" in df.columns, "Date column not found in weather file"
        assert "Latitude" in df.columns, "Latitude column not found in weather file"
        assert "Longitude" in df.columns, "Longitude column not found in weather file"
        assert "Province" in df.columns, "Province column not found in weather file"

        # Match the Climate ID to the Station ID
        df["Station ID"] = df["Climate ID"].apply(lambda x: find_station_id(station_lookup, str(x)))

        # Save the updated dataframe back to the file
        file = WEATHER_DIR_STATION_IDS / file.name
        df.to_csv(file, index=False)

def match_hotspot_ids(station_lookup: pd.DataFrame) -> None:
    df = pd.read_csv(HOTSPOT_FILE)

    # Rename the columns to match the station lookup table

    columns = {
        "Unnamed: 0": "ID",
        "ClosestStationID": "Climate ID",
        "FireLatitude": "Latitude",
        "FireLongitude": "Longitude",
        "FireProvinceShort": "Province",
        "Date": "Date",
        "HectaresBurnt": "Hectares Burnt",
    }
    df.rename(columns=columns, inplace=True)

    assert "Climate ID" in df.columns, "Climate ID column not found in hotspot file"
    assert "Station ID" not in df.columns, "Station ID column already exists in hotspot file"
    assert "Latitude" in df.columns, "Latitude column not found in hotspot file"
    assert "Longitude" in df.columns, "Longitude column not found in hotspot file"
    assert "Date" in df.columns, "Date column not found in hotspot file"
    assert "Hectares Burnt" in df.columns, "Hectares Burnt column not found in hotspot file"

    df["Station ID"] = df["Climate ID"].apply(lambda x: find_station_id(station_lookup, str(x)))
    df.dropna(inplace=True, subset=['Station ID'])
    df.to_csv(HOTSPOT_FILE_WITH_CLIMATE_ID, index=False)


def concatenate_weather_files(weather_files: List[Path]) -> None:
    # Load all the weather files
    dfs = []
    for file in tqdm(weather_files):
        dfs.append(pd.read_csv(file))

    # Concatenate the dataframes
    df = pd.concat(dfs)

    # Save the concatenated dataframe
    df.to_csv(DAILY_WEATHER_OUTPUT_PATH, index=False)

def copy_inventory_to_datamart() -> None:
    df = pd.read_csv(STATION_LOOKUP_PATH)
    df = df.drop(columns=["Climate ID"])

    columns = {
        "Name": "StationName",
        "Latitude": "StationLatitude",
        "Longitude": "StationLongitude",
        "Province": "StationProvinceShort",
        "Station ID": "StationID",
    }
    df = df.rename(columns=columns)
    df.dropna(inplace=True)

    df.to_csv(DATAMART_STATION_LOOKUP_PATH, index=False)
    print("Inventory copied to Datamart")

def copy_daily_weather_to_datamart() -> None:
    df = pd.read_csv(DAILY_WEATHER_OUTPUT_PATH)
    df = df.drop(columns=["Climate ID", "Name", "Latitude", "Longitude", "Province"])

    columns = {
        "Station ID": "StationID",
        "Date": "WeatherDate",
        "MAX_REL_HUMIDITY": "MaxRelativeHumidity",
        "SPEED_MAX_GUST": "MaxWindspeedGust",
        "MEAN_TEMPERATURE": "AverageTemperature",
    }

    df = df.rename(columns=columns)
    df.dropna(inplace=True, subset=['StationID'])
    df["StationID"] = df["StationID"].astype(int)

    df.to_csv(DATAMART_DAILY_WEATHER_PATH, index=False)
    print("Daily weather copied to Datamart")

def copy_hotspot_to_datamart() -> None:
    df = pd.read_csv(HOTSPOT_FILE_WITH_CLIMATE_ID)

    columns = {
        "ID": "BurnIncidentID",
        "Station ID": "ClosestStationID",
        "Date": "FireDate",
        "Latitude": "FireLatitude",
        "Longitude": "FireLongitude",
        "Province": "FireProvinceShort",
        "BurnIncidentID": "FireID",
        "Hectares Burnt": "HectaresBurnt"
    }

    df = df.rename(columns=columns)
    df = df.drop(columns=["Climate ID"])
    df.dropna(inplace=True)

    df["ClosestStationID"] = df["ClosestStationID"].astype(int)
    df["FireID"] = df["FireID"].astype(int)

    df.to_csv(DATAMART_HOTSPOT_PATH, index=False)
    print("Hotspot copied to Datamart")

provinces = {
    'NL': 0,
    'PE': 1,
    'NS': 2,
    'NB': 3,
    'QC': 4,
    'ON': 5,
    'MB': 6,
    'SK': 7,
    'AB': 8,
    'BC': 9,
    'YT': 10,
    'NT': 11,
    'NU': 12
}

def copy_province_lookup_to_datamart() -> None:
    df = pd.DataFrame(provinces.items(), columns=['ProvinceShort', 'ProvinceID'])
    df.to_csv(DATAMART_PROVINCE_LOOKUP_PATH, index=False)
    print("Province lookup table copied to Datamart")

def copy_yearly_land_cost_to_datamart() -> None:
    df = pd.read_csv(LAND_COST_FILE)
    df = df.drop(columns=["Unnamed: 0"])
    df.dropna(inplace=True)
    df['ProvinceID'] = df['CostProvinceShort'].apply(lambda short: provinces[short])
    df.to_csv(DATAMART_LAND_COST_TABLE_PATH, index=False)
    print("Land cost copied to Datamart")

import datetime
def copy_fact_table_entries_to_datamart():
    weather_df = pd.read_csv(DATAMART_DAILY_WEATHER_PATH).rename(columns={"WeatherDate":"BurnCostDate"})
    weather_df['BurnCostDate'] = weather_df['BurnCostDate'].apply(lambda date: datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S').date())

    land_cost_df = pd.read_csv(DATAMART_LAND_COST_TABLE_PATH).rename(columns={"CostProvinceShort": "FireProvinceShort"})

    fire_data_df = pd.read_csv(DATAMART_HOTSPOT_PATH).rename(columns={"FireDate":"BurnCostDate", "ClosestStationID":"StationID"})
    fire_data_df['BurnCostDate'] = fire_data_df['BurnCostDate'].apply(lambda date: datetime.datetime.strptime(date, '%Y-%m-%d').date())
    fire_data_df['Year'] = fire_data_df['BurnCostDate'].apply(lambda date: date.year)

    daily_burn = fire_data_df.merge(land_cost_df, on=["Year", "FireProvinceShort"])
    daily_burn = daily_burn.merge(weather_df, on=["StationID", "BurnCostDate"])

    # Finding Province ID from short
    daily_burn['ProvinceID'] = daily_burn['FireProvinceShort'].apply(lambda prov: provinces[prov])

    # Deriving Cost
    daily_burn['Cost'] = daily_burn.apply(lambda fact: fact['HectaresBurnt']*fact['DollarPerHectare'], axis=1)

    # Dropping all but relevant columns
    daily_burn = daily_burn[['StationID', 'BurnIncidentID', 'ProvinceID', 'BurnCostDate', 'FireProvinceShort', 'AverageTemperature', 'MaxRelativeHumidity', 'MaxWindspeedGust', 'HectaresBurnt', 'Cost']]
    
    # Creating fact table CSV
    daily_burn.to_csv(DATAMART_DAILY_BURN_COST_PATH, index=False)

if __name__ == "__main__":

    # This script assumes you have run the Rust NN preprocessing script
    #station_lookup = create_station_lookup()  # Create the station lookup table and station IDs
    #weather_files = discover_files(WEATHER_DIR_IN)  # Discover all the weather files created by Rust NN preprocessing # Discover the hotspot file created by Rust NN preprocessing
    #match_climate_ids(station_lookup, weather_files) # Add the appropriate station ID to each weather file
    #match_hotspot_ids(station_lookup)  # Add the appropriate hotspot ID to each weather file

    #station_id_weather_files = discover_files(WEATHER_DIR_STATION_IDS)  # Discover all the weather files with station IDs
    #concatenate_weather_files(station_id_weather_files) # Concatenate all the weather files into one
    copy_inventory_to_datamart()
    copy_daily_weather_to_datamart()
    copy_hotspot_to_datamart()
    copy_province_lookup_to_datamart()
    pre_process_landcost()
    copy_yearly_land_cost_to_datamart()
    copy_fact_table_entries_to_datamart()


    print("Done!")
