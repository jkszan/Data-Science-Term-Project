import pandas as pd

# Downloads from
# Farmland: https://www150.statcan.gc.ca/n1/tbl/csv/32100047-eng.zip
# Inflation (CPI): https://www150.statcan.gc.ca/n1/tbl/csv/18100005-eng.zip


def pre_process_landcost():
    farmland: pd.DataFrame = pd.read_csv("../farmland/land_cost.csv")
    print(farmland)
    print(farmland.columns)
    print(farmland["GEO"].unique())

    farmland: pd.DataFrame = farmland[farmland["GEO"] != "Canada"]

    # Drop unnecessary columns

    farmland.drop(["DGUID","Farm land and buildings","UOM","UOM_ID","SCALAR_FACTOR","SCALAR_ID","VECTOR","COORDINATE","STATUS","SYMBOL","TERMINATED","DECIMALS"],axis=1,inplace=True)

    # Transform province na166me to province short 
    province_mapping = {
            'Prince Edward Island':'PE','Nova Scotia':'NS','New Brunswick':'NB',
            'Quebec': 'QC','Ontario':'ON','Manitoba':'MB','Saskatchewan':'SK',
            'Alberta':'AB','British Columbia':'BC','Newfoundland and Labrador':'NL'}

    farmland["GEO"] = farmland["GEO"].map(province_mapping)
    print(farmland)

    print(farmland["VALUE"])

    inflation = pd.read_csv("../farmland/CPI.csv")
    inflation = inflation[(inflation["Products and product groups"] == "All-items") & (inflation["GEO"] == "Canada")]
    print(inflation)

    inflation_factors = farmland["REF_DATE"].apply(lambda x: 100.0/inflation[inflation["REF_DATE"] == x].iloc[0]["VALUE"])
    farmland["InflationScaler"] = inflation_factors
    farmland["DollarPerHectare"] = farmland["VALUE"] / 2.471
    print(farmland)

    farmland.rename(columns={"VALUE":"DollarPerAcre","GEO":"CostProvinceShort","REF_DATE":"Year"},inplace=True)
    print(farmland)
    farmland.to_csv("../data/land_cost.csv")
    
if __name__ == "__main__":
        pre_process_landcost()
