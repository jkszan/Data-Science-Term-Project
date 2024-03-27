COPY StationLookupTable(
    StationName,
    StationLatitude,
    StationLongitude,
    StationProvinceShort,
    StationID
) FROM '/docker-entrypoint-initdb.d/station_lookup_table.csv' DELIMITER ',' CSV HEADER;

COPY DailyWeather(
    MaxRelativeHumidity,
    MaxWindspeedGust,
    WeatherDate,
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
    MaxRelativeHumidity,
    MaxWindspeedGust,
    HectaresBurnt,Cost
) FROM '/docker-entrypoint-initdb.d/daily_burn.csv' DELIMITER ',' CSV HEADER;
