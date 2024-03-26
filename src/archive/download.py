import requests
from pathlib import Path
import os

url = 'https://cwfis.cfs.nrcan.gc.ca/downloads/hotspots/archive/'
tmp_dir = Path('./data/tmp/')
zip_dir = tmp_dir / 'zip'
DATA_DIR = Path('./data/tmp/unzip')
os.makedirs(tmp_dir, exist_ok=True)
os.makedirs(zip_dir, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

def download():
    for year in range(1994, 2023):
        DATA_DIR = tmp_dir / f"zip/{year}_hotspots.zip"

        r = requests.get(f"{url}/{year}_hotspots.zip", stream=True)
        with open(DATA_DIR, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

        # unzip the file
        os.system(f"unzip {DATA_DIR} -d {tmp_dir / 'unzip/'}")
        print(f"Unzipped {year} data")

    print("Download complete")

if __name__ == "__main__":
    download()
