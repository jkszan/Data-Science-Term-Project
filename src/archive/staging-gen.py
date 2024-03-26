# Tuple inputs to the weather station table
weatherData = [
    #Example: ("2010-10-10", "Example Station", 0.3, 0.2, "AB", 23.3, 23.3, 23.3)
]

# Tuple inputs to the fire database
fireData = [
    #Example: (10, 10, "AB", "2020-05-05", "2020-05-05", 123),
]


from math import acos, sin, cos
def getDistance(longOne, latOne, longTwo, latTwo):
    return acos(sin(latOne)*sin(latTwo)+cos(latOne)*cos(latTwo)*cos(longTwo-longOne))*6371

def addFireData(stationID, latitude, longitude, provinceShort, fireDate, fireEndDate, hectares):
    return  """
INSERT INTO BurnIncident(
    ClosestStationID,
    FireLatitude,
    FireLongitude,
    FireProvinceShort,
    FireDate,
    HectaresBurnt
) VALUES ({}, {}, {}, '{}', '{}', {});\n""".format(stationID, latitude, longitude, provinceShort, fireDate, hectares)

# ID, NAME, LAT, LONG
stationLookup = []
def getStationLookupEntry(stationName, lat, long):
    for id, name, _, _ in stationLookup:
        if name == stationName:
            return id

    newStationID = len(stationLookup) + 1
    stationLookup.append((newStationID, stationName, lat, long))
    return newStationID

def addStationLookup(stationID, stationName):

    return """
INSERT INTO StationLookupTable(
    StationID,
    StationName
) VALUES ({}, '{}');\n""".format(stationID, stationName)

def findClosestStationID(fireLatitude, fireLongitude):

    closestStationDistance = float('inf')
    closestStationID = None

    for id, _, stationLat, stationLong in stationLookup:

        prospectiveDistance = getDistance(fireLatitude, fireLongitude, stationLat, stationLong)

        if prospectiveDistance < closestStationDistance:
            closestStationDistance = prospectiveDistance
            closestStationID = id

    return closestStationID

def addWeatherData(stationID, weatherDate, stationName, stationLatitude, stationLongitude, stationProvinceShort, averageTemperature, averageWindspeed, averageHumidity):

    return """
INSERT INTO DailyWeather(
    StationID,
    WeatherDate,
    StationName,
    StationLatitude,
    StationLongitude,
    StationProvinceShort,
    AverageTemperature,
    AverageWindspeed,
    AverageHumidity
) VALUES ({}, '{}', '{}', {}, {}, '{}', {}, {}, {});\n""".format(stationID, weatherDate, stationName, stationLatitude, stationLongitude, stationProvinceShort, averageTemperature, averageWindspeed, averageHumidity)

weatherImport = ""

for weatherDate, stationName, stationLatitude, stationLongitude, stationProvinceShort, averageTemperature, averageWindspeed, averageHumidity in weatherData:
    stationID = getStationLookupEntry(stationName, stationLatitude, stationLongitude)
    weatherImport += addWeatherData(stationID, weatherDate, stationName, stationLatitude, stationLongitude, stationProvinceShort, averageTemperature, averageWindspeed, averageHumidity)

stationImport = ""

for stationID, stationName, _, _ in stationLookup:
    stationImport += addStationLookup(stationID, stationName)

fireImport = ""

for latitude, longitude, provinceShort, fireDate, fireEndDate, hectares in fireData:

    realStationID = findClosestStationID(latitude, longitude)
    fireImport += addFireData(realStationID, latitude, longitude, provinceShort, fireDate, fireEndDate, hectares)

finalScript = ''.join([stationImport, weatherImport, fireImport])
print(finalScript)