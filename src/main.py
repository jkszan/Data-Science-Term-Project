from pathlib import Path
from typing import List
import pandas as pd
from tqdm import tqdm


"""
Handle the pre-processing of the data

There is a single common point between the API, and hostpots data is the 
Climate ID. This is used to match the data between the two sources.
This will not be injested into the database so we link the primary key
of the station lookup table to replace Climate ID as the link.

We connect the hotspots and stations using the rust 
"""

WEATHER_OUTPUT_PATH: Path = Path("../data/weather.csv")
WEATHER_DIR: Path = Path("../data/weather")
HOTSPOT_FILE: Path = Path("../data/firedata_station.csv")
HOTSPOT_FILE_WITH_CLIMATE_ID: Path = Path("../data/firedata_station_with_climate_id.csv")


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

    return df


def discover_weather_files() -> List[Path]:
    # Find the list of weather files in the data directory
    files = [file for file in WEATHER_DIR.iterdir() if file.suffix == ".csv" and file.stat().st_size > 0]
    return files


def find_station_id(station_lookup: pd.DataFrame, climate_id: str) -> int:
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
    
    df.to_csv(HOTSPOT_FILE_WITH_CLIMATE_ID, index=False)


def concatenate_weather_files(weather_files: List[Path]) -> None:
    # Load all the weather files
    dfs = []
    for file in tqdm(weather_files):
        dfs.append(pd.read_csv(file))

    # Concatenate the dataframes
    df = pd.concat(dfs)

    # Save the concatenated dataframe
    df.to_csv(WEATHER_OUTPUT_PATH, index=False)
    
    
def generate_daily_weather_sql() -> None:
    pass


if __name__ == "__main__":
    # This script assumes you have run the Rust NN preprocessing script
    station_lookup = create_station_lookup()  # Create the station lookup table and station IDs
    weather_files = discover_weather_files()  # Discover all the weather files created by Rust NN preprocessing # Discover the hotspot file created by Rust NN preprocessing
    match_climate_ids(station_lookup, weather_files) # Add the appropriate station ID to each weather file
    match_hotspot_ids(station_lookup)  # Add the appropriate hotspot ID to each weather file
    concatenate_weather_files(weather_files) # Concatenate all the weather files into one
    generate_daily_weather_sql()
    
    
    print("Done!")
