use instant_distance::{Builder, Search};
use polars::io::csv::{CsvReader, CsvWriter};
use polars::lazy::dsl::col;
use polars::prelude::*;
use postgres::{Client, Error, NoTls};
use std::fs::File;

#[derive(Clone, Copy, Debug)]
struct Point(f64, f64);

impl instant_distance::Point for Point {
    fn distance(&self, other: &Self) -> f32 {
        // Euclidean distance metric
        (((self.0 - other.0).powf(2.0) + (self.1 - other.1).powf(2.0)) as f32).sqrt()
    }
}

fn main() -> Result<(), String> {
    //let mut client = Client::connect(
    //    "host=localhost dbname=postgres user=username password=password",
    //    NoTls,
    //)?;

    let mut hotspot_data = CsvReader::from_path("../../../firedata_no_station.csv")
        .unwrap()
        .infer_schema(None)
        .with_try_parse_dates(true)
        .has_header(true)
        .low_memory(true)
        .finish()
        .unwrap();

    let mut weather_stations = CsvReader::from_path("../../../Station Inventory EN.csv")
        .unwrap()
        .infer_schema(None)
        .has_header(true)
        .truncate_ragged_lines(true)
        .finish()
        .unwrap();

    let mut lonl = weather_stations
        .column("Longitude (Decimal Degrees)")
        .unwrap()
        .str()
        .unwrap();
    let mut latl = weather_stations
        .column("Latitude (Decimal Degrees)")
        .unwrap()
        .str()
        .unwrap();

    let station_ids: Vec<(Option<i64>, (Option<i64>, Option<i64>))> = weather_stations
        .column("Station ID")
        .unwrap()
        .i64()
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

    let fire_years = hotspot_data.column("Date")
        .unwrap()
        .year()
        .unwrap();

    let map = Builder::default().build(points, station_ids);
    let mut search = Search::default();

    if let (Ok(Ok(latcol)), Ok(Ok(loncol))) = (
        hotspot_data.column("FireLatitude").map(|col| col.f64()),
        hotspot_data.column("FireLongitude").map(|col| col.f64()),
    ) {
        let points: Vec<Option<i64>> = fire_years.iter().zip(latcol
            .iter()
            .zip(loncol.iter()))
            .map(|(yearopt,(latopt, lonopt))| {
                if let (Some(year),Some(lat), Some(lon)) = (yearopt,latopt, lonopt) {
                    let mut closestid = None;
                    let mut s = map.search(&Point(lat, lon), &mut search);
                    while let Some(res) = s.next(){
                        if let (Some(id),(Some(firstyear),Some(lastyear))) = res.value{
                            if *firstyear < (year as i64) && (year as i64) < *lastyear{
                                closestid = Some(*id);
                                break;
                            } 
                        }
                    }
                    closestid
                } else {
                    None
                }
            })
            .collect();

        let hotspot_data = hotspot_data
            .with_column(Series::new("ClosestStationID", points))
            .unwrap();
        println!("{}", hotspot_data);
        let path = "../../../firedata_station.csv";
        if let Ok(new_firestations_file) = File::create(path) {
            let writer = CsvWriter::new(new_firestations_file);
            writer.include_header(true).finish(hotspot_data).unwrap();
            println!("firedata_station mapping written to {}", path);
        }
    }
    Ok(())
}
