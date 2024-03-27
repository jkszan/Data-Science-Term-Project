#!/usr/bin/env python
# coding: utf-8

# THIS MUST BE RUN BEFORE THE RUST PROGRAM AND THE main.py

# ## Download data first
# https://cwfis.cfs.nrcan.gc.ca/downloads/nbac/nbac_1986_to_2022_20230630.zip
# 
# Metadata description: 
# 
# https://cwfis.cfs.nrcan.gc.ca/downloads/nbac/NBAC_metadata_NAP_ISO_19115_2003.pdf

import pandas as pd
import geopandas as gpd
from pyproj import Transformer

def firedata_preprocess():

    fires = gpd.read_file('nbac_1986_to_2022_20230630.zip',engine="pyogrio")

    print("Fires not preprocessed")
    print(fires)

    # ## Data cleaning
    # Remove fires without recorded start and end dates
    fires = fires[(~fires["SDATE"].isna() &~ fires["EDATE"].isna()) | (~fires["AFSDATE"].isna() & ~fires["AFEDATE"].isna())].reset_index(drop=True)

    # Merge two alternative start-end columns
    fires["SDATE"] = fires[["SDATE","AFSDATE"]].apply(lambda x: x["SDATE"] if not pd.isnull(x["SDATE"]) else x["AFSDATE"],axis=1)
    fires["EDATE"] = fires[["EDATE","AFEDATE"]].apply(lambda x: x["EDATE"] if not pd.isnull(x["EDATE"]) else x["AFEDATE"],axis=1)

    # Convert to correct datatype
    fires["YEAR"] = fires["YEAR"].apply(lambda x: int(x))
    fires["NFIREID"] = fires["NFIREID"].apply(lambda x: int(x))

    # Remove initial unnecessary columns
    fires = fires[["YEAR","NFIREID","SDATE","EDATE","ADJ_HA","geometry"]]

    # Check for outlier fires going on for a long time
    fires["timediff"] = fires.apply(lambda x: x.EDATE - x.SDATE,axis=1)

    # Load Canadian province data

    provinces = gpd.read_file("georef-canada-province@public.zip")
    print(f"crs provinces: {provinces.crs}")
    print(f"crs fires: {fires.crs}")
    provinces = provinces.to_crs(fires.crs)

    # Remove fires outside of provinces
    province_fires = gpd.sjoin(provinces,gpd.GeoDataFrame(fires,geometry=fires.geometry,crs=provinces.crs),how="right")
    fires = province_fires.drop(["index_left","prov_code","prov_name_e","prov_area_c","prov_type","year"],axis=1)

    # Visually check distribution of fires
    #fires[fires["YEAR"] > 2000].plot(ax=provinces.plot(),color='red',marker='x',markersize=3)

    ## Remove fires going on for more than 3 months (outliers)
    print(f" Fires going on for more than 100 days: {len(fires[fires['timediff'].apply(lambda d: d.days) > 100])} of {len(fires)}")
    fires = fires[fires['timediff'].apply(lambda d: d.days) < 100]

    ## Split fires into per day hectares burnt
    fires['day'] = fires.apply(lambda x: pd.date_range(
        x["SDATE"],
        x["EDATE"]
        , freq='D'), axis=1)

    fires = fires.explode('day')

    fires['ADJ_HA'] = fires.groupby('NFIREID')['ADJ_HA'].transform(lambda x: x/ len(x)).astype(float)

    # Convert coordinates of fires to Lat-Lon
    transformer = Transformer.from_crs(fires.crs, "EPSG:4326")

    fire_centroids = fires["geometry"].apply(lambda poly: transformer.transform(poly.centroid.x,poly.centroid.y))
    fires[['Latitude', 'Longitude']] = pd.DataFrame(fire_centroids.tolist(), index=fires.index)

    # Convert provinces to shortform
    prov_short = {'Ontario' : 'ON', 'Manitoba': 'MB', 'Yukon': 'YT', 'Territoires du Nord-Ouest':'NT',
           'Alberta':'AB', 'Saskatchewan':'SK', 'Québec':'QC', 'Colombie-Britannique':'BC',
           'Nouveau-Brunswick':'NB','Terre-Neuve-et-Labrador':'NL','Île-du-Prince-Édouard':'PE','Nouvelle-Écosse':'NS', 'Nunavut':'NU'}


    fires['prov_name_f'] = fires["prov_name_f"].map(prov_short)

    # Final dropping and renaming of columns
    fires = fires.reset_index(drop=True).drop(["YEAR","SDATE","EDATE","geometry","timediff"],axis=1).rename({"prov_name_f":"FireProvinceShort","NFIREID":"BurnIncidentID","ADJ_HA":"HectaresBurnt","Longitude":"FireLongitude","Latitude":"FireLatitude","day":"Date"},axis=1)

    print("Fires preprocessed")
    print(fires)

    # Write to file
    fires.to_csv("./firedata_no_station.csv")

