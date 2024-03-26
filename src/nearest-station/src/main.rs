use chrono::{Datelike, NaiveDate};
use indicatif::ProgressIterator;
use instant_distance::{Builder, Search};
use polars::io::csv::CsvReader;
use polars::lazy::dsl::{col, lit};
use polars::prelude::*;
use reqwest::blocking::Client;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

#[derive(Clone, Copy, Debug)]
struct Point(f64, f64);

impl instant_distance::Point for Point {
    fn distance(&self, other: &Self) -> f32 {
        // Euclidean distance metric
        (((self.0 - other.0).powf(2.0) + (self.1 - other.1).powf(2.0)) as f32).sqrt()
    }
}

const UNIXSTARTDAY: i32 = 719_163;
const WEATHER_DIRECTORY: &str = "../../../data/weather";

fn station_has_meantemp(path: &Path) -> bool{
    if let Ok(reader) = CsvReader::from_path(path){
        if let Ok(weather_station) = reader
            .infer_schema(None)
            .has_header(true)
            .truncate_ragged_lines(true)
            .finish()
        {
            let measurements = weather_station
                .lazy()
                .filter(col("MEAN_TEMPERATURE").is_not_null())
                .collect();
            if let Ok(good_measurements) = measurements {
                if good_measurements.shape().0 > 0 {
                    return true;
                }
            }
        }
    }
    false
}

fn get_closest_ranking(hotspot_data: &mut DataFrame) -> Result<Vec<Vec<String>>, String> {
    let weather_stations = CsvReader::from_path("../../../data/station_inventory.csv")
        .unwrap()
        .infer_schema(None)
        .has_header(true)
        .truncate_ragged_lines(true)
        .finish()
        .unwrap();

    let lonl = weather_stations
        .column("Longitude (Decimal Degrees)")
        .unwrap()
        .str()
        .unwrap();
    let latl = weather_stations
        .column("Latitude (Decimal Degrees)")
        .unwrap()
        .str()
        .unwrap();

    let station_ids: Vec<(Option<&str>, (Option<i64>, Option<i64>))> = weather_stations
        .column("Climate ID")
        .unwrap()
        .str()
        .unwrap()
        .iter()
        .zip(
            weather_stations
                .column("First Year")
                .unwrap()
                .i64()
                .unwrap()
                .iter()
                .zip(
                    weather_stations
                        .column("Last Year")
                        .unwrap()
                        .i64()
                        .unwrap()
                        .iter(),
                ),
        )
        .collect();

    let points: Vec<Point> = latl
        .iter()
        .zip(lonl.iter())
        .map(|(latopt, lonopt)| {
            if let (Some(latstr), Some(lonstr)) = (latopt, lonopt) {
                let point =
                    if let (Ok(lat), Ok(lon)) = (latstr.parse::<f64>(), lonstr.parse::<f64>()) {
                        Some(Point(lat, lon))
                    } else {
                        None
                    };
                point
            } else {
                None
            }
        })
        .filter(|f| f.is_some())
        .map(|f| f.unwrap())
        .collect();

    let fire_dates = hotspot_data.column("Date").unwrap().date().unwrap();

    let map = Builder::default().build(points, station_ids);
    let mut search = Search::default();

    if let (Ok(Ok(latcol)), Ok(Ok(loncol))) = (
        hotspot_data.column("FireLatitude").map(|col| col.f64()),
        hotspot_data.column("FireLongitude").map(|col| col.f64()),
    ) {
        let points: Vec<Vec<String>> = fire_dates
            .iter()
            .zip(latcol.iter().zip(loncol.iter()))
            .map(|(dateopt, (latopt, lonopt))| {
                if let (Some(date), Some(lat), Some(lon)) = (dateopt, latopt, lonopt) {
                    let orderedstations = map.search(&Point(lat, lon), &mut search);
                    orderedstations
                        .map(|mapitem| mapitem.value)
                        .filter(|(id, (firstyear, lastyear))| {
                            let year = NaiveDate::from_num_days_from_ce_opt(date + UNIXSTARTDAY)
                                .unwrap()
                                .year_ce()
                                .1;
                            let mut activestation = false;
                            if let Some(y) = firstyear {
                                activestation = *y < year as i64;
                            }
                            if let Some(y) = lastyear {
                                activestation &= (year as i64) < *y;
                            }
                            activestation & id.is_some()
                        })
                        .map(|(id, _)| id.unwrap().to_string())
                        .collect()
                } else {
                    Vec::new()
                }
            })
            .collect();
        return Ok(points);
    }
    Err(String::from("Error parsing firedata"))
}

fn main() -> Result<(), String> {
    let client = Client::new();

    let mut hotspot_data = CsvReader::from_path("../../../data/firedata_no_station.csv")
        .unwrap()
        .infer_schema(None)
        .with_try_parse_dates(true)
        .has_header(true)
        .low_memory(true)
        .finish()
        .unwrap()
        .lazy()
//        .filter(col("Date").gt(lit(NaiveDate::parse_from_str("2022-10-08","%Y-%m-%d").unwrap()))) //test set
        .filter(col("FireLatitude").is_not_null())
        .filter(col("FireLongitude").is_not_null())
        .collect()
        .unwrap();

    let closest_ranking = get_closest_ranking(&mut hotspot_data)?;

    let fire_dates = hotspot_data.column("Date").unwrap().date().unwrap();

    let earliest_date =
        NaiveDate::from_num_days_from_ce_opt(UNIXSTARTDAY + fire_dates.min().unwrap()).unwrap();
    let latest_date =
        NaiveDate::from_num_days_from_ce_opt(UNIXSTARTDAY + fire_dates.max().unwrap()).unwrap();

    let mut closest_stations: Vec<Option<String>> = Vec::with_capacity(fire_dates.len());

    let weather_dir = Path::new(WEATHER_DIRECTORY);

    if !weather_dir.exists() {
        std::fs::create_dir(weather_dir).expect("Failed creating weather download dir");
    }

    let mut processed_stations: Vec<String> = Vec::new();

    for (dateint, ranking) in fire_dates.iter().zip(closest_ranking.iter()).progress() {
        let mut closest_station = None;
        for station in ranking {
            let station_path = weather_dir.join(station).with_extension("csv");
            if !processed_stations.contains(station){
                processed_stations.push(station.clone());
                let mut weather = String::new();
                for pre_millenium in [true, false] {
                    let start = if pre_millenium {
                        earliest_date.to_string()
                    } else {
                        String::from("2000-01-01")
                    };
                    let end = if !pre_millenium {
                        latest_date.to_string()
                    } else {
                        String::from("1999-12-31")
                    };
                    let url = format!("
                                https://api.weather.gc.ca/collections/climate-daily/items?f=csv&lang=en-CA&sortby=LOCAL_DATE&skipGeometry=false&limit=10000\
                                      &offset=0&datetime={}%2000:00:00/{}%2000:00:00&CLIMATE_IDENTIFIER={}&\
                                      properties=LOCAL_DATE,STATION_NAME,MEAN_TEMPERATURE,CLIMATE_IDENTIFIER,SPEED_MAX_GUST,MAX_REL_HUMIDITY,PROVINCE_CODE",
                                      start,end,station);
                    let request = client
                        .get(url)
                        .build()
                        .expect("FAILED TO BUILD WEATHER URL REQUEST");
                    if let Ok(response) = client.execute(request) {
                        if let Ok(mut response) = response.error_for_status() {
                            if pre_millenium {
                                response
                                    .read_to_string(&mut weather)
                                    .expect("Could not parse Weather response");
                                weather = String::from(weather.trim_end());
                            } else{
                                let mut string_buf = String::new();
                                response
                                    .read_to_string(&mut string_buf)
                                    .expect("Could not parse Weather response");
                                if !weather.is_empty(){
                                // Remove first header line
                                string_buf = string_buf
                                    .lines()
                                    .skip(1)
                                    .fold(String::new(), |acc, line| format!("{}\n{}", acc, line));
                                    string_buf = format!("\n{}",string_buf);
                                }
                                weather.push_str(&string_buf);
                            }
                        }
                    }
                }
                if !weather.is_empty(){
                    if let Ok(mut new_firestations_file) = File::create(&station_path) {
                        new_firestations_file
                            .write(weather.as_bytes())
                            .expect("Could not write weather to file");
                        new_firestations_file.flush().unwrap();

                        if !station_has_meantemp(&station_path){
                            std::fs::remove_file(&station_path).expect("Error removing non-useful file");
                        }
                    }
                }
            }
            if let Ok(reader) = CsvReader::from_path(&station_path){
                if let Ok(weather_station) = reader
                    .infer_schema(None)
                    .has_header(true)
                    .with_try_parse_dates(true)
                    .truncate_ragged_lines(true)
                    .finish()
                {
                    let date =
                        NaiveDate::from_num_days_from_ce_opt(UNIXSTARTDAY + dateint.unwrap()).unwrap();
                    let measurements = weather_station
                        .lazy()
                        .filter(col("LOCAL_DATE").eq(lit(date)))
                        .filter(col("MEAN_TEMPERATURE").is_not_null())
                        .collect();
                    if let Ok(good_measurements) = measurements {
                        if good_measurements.shape().0 > 0{
                            closest_station = Some(station.to_string());
                            break;
                        }
                    }
                }
            }
        }
        closest_stations.push(closest_station);
    }

    let hotspot_data = hotspot_data
        .with_column(Series::new("ClosestStationID", closest_stations))
        .unwrap();
    println!("{}", hotspot_data);
    let path = "../../../data/firedata_station.csv";
    if let Ok(new_firestations_file) = File::create(path) {
        let writer = CsvWriter::new(new_firestations_file);
        writer.include_header(true).finish(hotspot_data).unwrap();
        println!("_station mapping written to {}", path);
    }

    Ok(())
}
