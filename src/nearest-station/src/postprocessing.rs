use indicatif::{ProgressBar, ProgressIterator};
use polars::io::csv::CsvReader;
use polars::lazy::dsl::{col};
use polars::prelude::*;
use std::collections::{HashMap};
use polars::datatypes::PlHashMap;
use std::fs::{self, File};

fn province_map(province: &str) -> String{
    return String::from(match province{
        "ALBERTA"=> "AB",
        "BRITISH COLUMBIA"=> "BC",
        "MANITOBA"=> "MB",
        "NEW BRUNSWICK"=> "NB",
        "NEWFOUNDLAND"=> "NL",
        "NORTHWEST TERRITORIES"=> "NT",
        "NOVA SCOTIA"=> "NS",
        "NUNAVUT"=> "NU",
        "ONTARIO"=> "ON",
        "PRINCE EDWARD ISLAND"=> "PE",
        "QUEBEC"=> "QC",
        "SASKATCHEWAN"=> "SK",
        "YUKON TERRITORY"=> "YT",
        _ => panic!("No matching province!")
    })
}

pub fn postprocessing() -> Result<(), String> {

    // create_station_lookup
    let weather_dir = "../../../data/weather";
    let out_dir = "../../../data/weather_station_ids";
    let lookup_table_out = "../../../data/station_lookup.csv";
    let columns = [col("Climate ID"), col("Name"), col("Latitude (Decimal Degrees)"), col("Longitude (Decimal Degrees)"), col("Province")];

    let mut stations = CsvReader::from_path("../../../data/station_inventory.csv")
        .unwrap()
        .infer_schema(None)
        .has_header(true)
        .truncate_ragged_lines(true)
        .finish()
        .unwrap();
    
    stations = stations.lazy().select(&columns).collect().expect("Could not select columns in staiton lookup");
    
    let mut dtypes = PlHashMap::new();
    dtypes.insert("Latitude (Decimal Degrees)", DataType::Float64);
    dtypes.insert("Longitude (Decimal Degrees)", DataType::Float64);

    stations = stations.lazy().cast(dtypes, false).collect().unwrap();
    let province_short = stations.column("Province").expect("No provinces").str().expect("Error casting").apply(|popt| popt.map(|p| province_map(p).into()));
    stations.replace("Province", province_short).unwrap();
    stations = stations.with_row_index("Station ID", None).expect("Could not add index");
    if let Ok(new_stations_file) = File::create(lookup_table_out){
        let writer = CsvWriter::new(new_stations_file);
        writer.include_header(true).finish(&mut stations).unwrap();
        println!("_station lookup written to {}", lookup_table_out);
    }

    let station_ids = stations.column("Station ID").unwrap().u32().unwrap();
    let climate_ids = stations.column("Climate ID").unwrap().str().unwrap();

    let mut mapping: HashMap<String, u32> = HashMap::new();
    for (st,cl) in station_ids.iter().zip(climate_ids.iter()){
        if let (Some(climate_id),Some(station_id)) = (cl,st){
            mapping.insert(climate_id.to_owned(), station_id);
        }
    }
    
    let mut schema = Schema::new();

    schema.with_column("STATION_NAME".into(), DataType::String);
    schema.with_column("CLIMATE_IDENTIFIER".into(), DataType::String);
    schema.with_column("x".into(), DataType::Float64);
    schema.with_column("y".into(), DataType::Float64);
    schema.with_column("PROVINCE_CODE".into(), DataType::String);
    schema.with_column("LOCAL_DATE".into(), DataType::Datetime(TimeUnit::Milliseconds, None));

    let mut bar = ProgressBar::new(2869);

    println!("Matching Climate IDs...");
    for station_opt in fs::read_dir(weather_dir).unwrap(){
        bar.inc(1);
        if let Ok(station_file) = station_opt{
            let mut df = CsvReader::from_path(station_file.path())
            .unwrap()
            .infer_schema(None)
            .has_header(true)
            .truncate_ragged_lines(true)
            .finish()
            .unwrap();

            if df.get_column_names().contains(&"Station ID"){
                continue;
            }
            df.rename("STATION_NAME", "Name").expect("could not rename column");
            df.rename("CLIMATE_IDENTIFIER", "Climate ID").expect("could not rename column");
            df.rename("x", "Longitude").expect("could not rename column");
            df.rename("y", "Latitude").expect("could not rename column");
            df.rename("PROVINCE_CODE", "Province").expect("could not rename column");
            df.rename("LOCAL_DATE", "Date").expect("could not rename column");
            
            let climate_id = df.column("Climate ID").unwrap().drop_nulls().get(0).unwrap().cast(&DataType::String).to_string().replace("\"","");
            
            if let Some(stid) = mapping.get(&climate_id){
            
                let q: Series = std::iter::repeat(stid).take(df.height()).copied().collect();
                df.with_column(q.with_name("Station ID")).unwrap();

                let out_path = format!("{}/{}",out_dir,station_file.file_name().into_string().unwrap());
                if let Ok(new_stations_file) = File::create(out_path){
                    let writer = CsvWriter::new(new_stations_file);
                    writer.include_header(true).finish(&mut df).unwrap();
                }
            }
        }
    }
    bar.finish();
    Ok(())
}
