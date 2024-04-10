
# Data Science Term Project

## For OLAP and Dashboard see [OLAP-Dashboard/README.md](OLAP-Dashboard/Group25_DashboardREADME.pdf)

## For Data Mining and outlier detection see [src/machine_learning/phase_four.md](src/machine_learning/phase_four.md)

## Requirements
- Python 3.8
- Rust 1.71.0
- Docker

## Running The Project (No Dependencies)
1. Unzip the databasedumpfile.zip file (This file is a dump of the final database with all dimension / fact table entries)
2. Startup a postgresql instance
3. Run `psql -f databasedumpfile postgresql` to load the database dump into your database
4. You can access the generated database using user "username" and password "password"

## Running The Project (Docker)
1. Unzip the daily_weather_table.zip file
2. Navigate to the src directory
3. Run `docker compose up`
4. You can enter the database via CLI by running `psql -h 127.0.0.1 -p 5436 -d postgres -U username` and supplying the password "password"

## Running The Project (Normal Pipeline)
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
