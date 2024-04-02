import os
from pathlib import Path
import pandas as pd


ROOT = Path(os.path.abspath(__file__)).parent / "../.."
OUTPUT_DIR = ROOT / "data/machine_learning_processed"
INPUT_DIR = ROOT / "src/Datamart"


class Preprocess:
    def __init__(self, override=False):
        if not os.path.exists(OUTPUT_DIR):
            os.makedirs(OUTPUT_DIR)
        self.override = override
        self.province_mapping = self.load_province_mapping()

    def preprocess_fire_data(self):
        daily_burn_file = Path("daily_burn.csv")
        hotspot_file = Path("hotspot_table.csv")
        file = Path("fire_data.csv")
        
        if not self.override and os.path.exists(OUTPUT_DIR / file):
            return pd.read_csv(OUTPUT_DIR / file)

        daily_burn = pd.read_csv(INPUT_DIR / daily_burn_file)
        hotspot = pd.read_csv(INPUT_DIR / hotspot_file)
        hotspot = hotspot[['BurnIncidentID', 'FireLatitude', 'FireLongitude']]
        fires = hotspot.merge(daily_burn, on="BurnIncidentID")

        # Convert BurnCostDate to unix timestamp
        fires["BurnCostDate"] = pd.to_datetime(daily_burn["BurnCostDate"]).astype(int)
        fires.to_csv(OUTPUT_DIR / file, index=False)
        return fires

    def preprocess_weather_data(self):
        if not self.override and os.path.exists(OUTPUT_DIR / Path("weather_data.csv")):
            return

    def preprocess_daily_burn(self):
        if not self.override and os.path.exists(OUTPUT_DIR / Path("daily_burn.csv")):
            return

    def preprocess_weather_fire(self):
        if not self.override and os.path.exists(OUTPUT_DIR / Path("weather_fire.csv")):
            return

    def load_province_mapping(self) -> dict:
        """
        Load province lookup table and return a dictionary mapping province ID to province short name
        Mostly for interal class use.
        """
        file = Path("province_lookup.csv")
        df = pd.read_csv(INPUT_DIR / file)
        province_mapping = df.set_index("ProvinceID")["ProvinceShort"].to_dict()
        return province_mapping

    def get_fire_data(self):
        return self.preprocess_fire_data()
    
