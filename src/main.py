from pathlib import Path
from typing import List
import pandas as pd


"""
Handle the pre-processing of the data

There is a single common point between the API, and hostpots data is the 
Climate ID. This is used to match the data between the two sources.
This will not be injested into the database so we link the primary key
of the station lookup table to replace Climate ID as the link.

We connect the hotspots and stations using the rust 
"""





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
        "YUKON TERRITORY": "YT"
    }
    df["Province"] = df["Province"].apply(lambda x: province_map[x])
    
    # Add index column "Station ID"
    df["Station ID"] = range(len(df))
    
    return df


def discover_weather_files() -> List[Path]:
    return list(Path("../data/weather").rglob("*.csv"))


def find_station_id(station_lookup: pd.DataFrame, climate_id: str) -> int:
    station = station_lookup[station_lookup["Climate ID"] == climate_id]
    if len(station) == 0:
        return None
    return station["Station ID"].values[0]


def match_climate_ids(station_lookup: pd.DataFrame, weather_files: List[Path]) -> None:
    # Load the climate data
    for file in weather_files:
        df = pd.read_csv(file)
        
        assert "Climate ID" in df.columns, "Climate ID column not found in weather file"
        assert "Station ID" not in df.columns, "Station ID column already exists in weather file"
        
        # Match the Climate ID to the Station ID
        df["Station ID"] = df["Climate ID"].apply(lambda x: find_station_id(station_lookup, x))
        
        # Save the updated dataframe back to the file
        df.to_csv(file, index=False)
        
        
def discover_hotspot_file() -> Path:
    return Path("../data/hotspot/hotspot.csv")

        
def match_hotspot_ids(station_lookup: pd.DataFrame, hotspot_file: Path) -> None:
    df = pd.read_csv(hotspot_file)
    
    assert "Climate ID" in df.columns, "Station Name column not found in hotspot file"
    assert "Station ID" not in df.columns, "Station ID column already exists in hotspot file"
    assert "Latitude" in df.columns, "Latitude column not found in hotspot file"
    assert "Longitude" in df.columns, "Longitude column not found in hotspot file"
    assert "Date" in df.columns, "Date column not found in hotspot file"
    assert "Hectares Burnt" in df.columns, "Hectares Burnt column not found in hotspot file"
    
    df["Station ID"] = df["Climate ID"].apply(lambda x: find_station_id(station_lookup, x))
    df.to_csv(hotspot_file, index=False)


def concatenate_weather_files(weather_files: List[Path]) -> None:
    # Load all the weather files
    dfs = [pd.read_csv(file) for file in weather_files]
    
    # Concatenate the dataframes
    df = pd.concat(dfs)
    
    # Save the concatenated dataframe
    df.to_csv("../data/weather.csv", index=False)
        

if __name__ == "__main__":
    # This script assumes you have run the Rust NN preprocessing script
    station_lookup = create_station_lookup() # Create the station lookup table and station IDs
    weather_files = discover_weather_files() # Discover all the weather files created by Rust NN preprocessing
    hotspot_file = discover_hotspot_file() # Discover the hotspot file created by Rust NN preprocessing
    match_climate_ids(station_lookup, weather_files) # Add the appropriate station ID to each weather file
    match_hotspot_ids(station_lookup, hotspot_file) # Add the appropriate hotspot ID to each weather file
    concatenate_weather_files(weather_files) # Concatenate all the weather files into one
    
    print("Done!")