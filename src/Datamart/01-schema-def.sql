-- Creating an enum to constrain the possible province shorts in our many tables
CREATE TYPE province_short AS ENUM(
    'NL',
    'PE',
    'NS',
    'NB',
    'QC',
    'ON',
    'MB',
    'SK',
    'AB',
    'BC',
    'YT',
    'NT',
    'NU'
);

CREATE TABLE IF NOT EXISTS StationLookupTable(
    StationID int PRIMARY KEY,
    StationName VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS DailyWeather(
    StationID int REFERENCES StationLookupTable(StationID) NOT NULL,
    WeatherDate date NOT NULL,
    StationName VARCHAR(20),
    StationLatitude double precision NOT NULL,
    StationLongitude double precision NOT NULL,
    StationProvinceShort province_short,
    AverageTemperature double precision NOT NULL,
    AverageWindspeed double precision NOT NULL,
    AverageHumidity double precision NOT NULL,
    PRIMARY KEY(StationID, WeatherDate)
);

CREATE TABLE IF NOT EXISTS BurnIncident(
    BurnIncidentID int UNIQUE GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ClosestStationID int REFERENCES StationLookupTable(StationID) NOT NULL,
    FireLatitude double precision,
    FireLongitude double precision,
    FireProvinceShort province_short NOT NULl,
    FireDate date NOT NULL,
    HectaresBurnt double precision NOT NULL
);
-- Note, changed Date in "Burnincident" and "Dailyweather"

CREATE TABLE IF NOT EXISTS YearlyLandCost(
    ProvinceID int UNIQUE GENERATED ALWAYS AS IDENTITY NOT NULL, -- Province ID is stupid and we should change to using province short instead
    CostYear int NOT NULL,
    CostProvinceShort province_short NOT NULL,
    DollarPerAcre double precision,
    DollarPerHectare double precision NOT NULL,
    InflationScalar double precision NOT NULL,
    PRIMARY KEY (ProvinceID, CostYear)
);

CREATE TABLE IF NOT EXISTS DailyBurnCost(
    StationID int REFERENCES StationLookupTable(StationID),
    BurnCostDate date NOT NULL,
    ProvinceID int REFERENCES YearlyLandCost(ProvinceID) NOT NULL,
    BurnIncidentID int REFERENCES BurnIncident(BurnIncidentID) NOT NULL,
    FireProvinceShort province_short NOT NULL,
    AverageTemperature double precision,
    AverageHumidity double precision,
    HectaresBurnt double precision,
    Cost double precision,
    PRIMARY KEY(StationID, BurnCostDate, ProvinceID, BurnIncidentID)
);
