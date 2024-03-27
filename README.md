
# Data Science Term Project

## Requirements
- Python 3.8
- Rust 1.71.0
- Docker

## Running The Project
0. Install Dependencies
1. Preprocess the Fire Data
```bash
cd /src/
pip3 install -r requirements.txt
python3 firedata_preprocess.py
python3 land_cost_preprocess.py
```
2. Run the Nearest Weather Station Finder
```bash
cd /src/nearest-station/src
cargo run
```
3. Run the Main Script to generate the data
```bash
cd /src/
python3 main.py
```
4. Run the Docker Container
```bash
cd /src/
docker-compose up
```