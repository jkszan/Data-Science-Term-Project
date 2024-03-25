-- INSERT INTO YearlyLandCost(
--     CostYear,
--     CostProvinceShort,
--     DollarPerAcre,
--     DollarPerHectare,
--     InflationScalar
-- ) VALUES (2000, 'ON', 1, 1000, 1);

COPY StationLookupTable(
    StationName,
    StationLatitude,
    StationLongitude,
    StationProvinceShort,
    StationID
) FROM '/docker-entrypoint-initdb.d/station_lookup_table.csv' DELIMITER ',' CSV HEADER;

COPY DailyWeather(
    AverageHumidity,
    WeatherDate,
    AverageWindspeed,
    AverageTemperature,
    StationID
) FROM '/docker-entrypoint-initdb.d/daily_weather_table.csv' DELIMITER ',' CSV HEADER;

COPY BurnIncident(
    BurnIncidentID,
    FireProvinceShort,
    FireID,
    HectaresBurnt,
    FireDate,
    FireLatitude,
    FireLongitude,
    ClosestStationID
) FROM '/docker-entrypoint-initdb.d/hotspot_table.csv' DELIMITER ',' CSV HEADER;

COPY ProvinceLookupTable(
    ProvinceShort,
    ProvinceID
) FROM '/docker-entrypoint-initdb.d/province_lookup.csv' DELIMITER ',' CSV HEADER;

COPY YearlyLandCost(
    CostYear,
    CostProvinceShort,
    DollarPerAcre,
    InflationScalar,
    DollarPerHectare,
    ProvinceID
) FROM '/docker-entrypoint-initdb.d/land_cost_table.csv' DELIMITER ',' CSV HEADER;

COPY DailyBurnCost(
    StationID,
    BurnIncidentID,
    ProvinceID,
    BurnCostDate,
    FireProvinceShort,
    AverageTemperature,
    AverageHumidity,
    AverageWindspeed,
    HectaresBurnt,Cost
) FROM '/docker-entrypoint-initdb.d/daily_burn.csv' DELIMITER ',' CSV HEADER;
