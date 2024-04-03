# Dashboard
##  You will have to create a file at '~/.streamlit/secrets.toml' with contents in this format:
##  ```
##  [connections.postgresql]
##  dialect = "postgresql"
##  host = "localhost"
##  port = "5432"
##  database = "xxx"
##  username = "xxx"
##  password = "xxx"
##  ```


import streamlit as st

def intro(psql_conn):
    import streamlit as st

    st.write("Wild Fires and Burncost DB")
    st.sidebar.success("Select a page to explore the Dashboard.")


def map_fires(psql_conn):
    import streamlit as st
    import pandas as pd
    import pydeck as pdk
    import math
    from urllib.error import URLError

    st.markdown(f"# {list(page_names_to_funcs.keys())[2]}")
    st.write(
        """
        This demo shows how to use
[`st.pydeck_chart`](https://docs.streamlit.io/library/api-reference/charts/st.pydeck_chart)
to display geospatial data.
"""
    )

    @st.cache_data
    def get_tables():
        fire_df = psql_conn.query(""" SELECT c.cost,c.hectaresburnt, c.averagetemperature,b.burnincidentid, c.burncostdate as date,c.fireprovinceshort, \
            c.maxwindspeedgust,c.maxrelativehumidity,b.firelatitude as latitude,b.firelongitude as longitude \
                FROM dailyburncost c JOIN burnincident b ON c.burnincidentid = b.burnincidentid;""",ttl="10m")
        stations_df = psql_conn.query("SELECT StationID,StationName,StationLongitude as longitude,StationLatitude as latitude,StationProvinceShort as province FROM stationlookuptable;",ttl="10m")
        return (fire_df,stations_df)

    try:
        fires,stations = get_tables()

        st.write("Filter Fires")
        startdate = st.date_input(label="Enter start_date",value=fires["date"].min(),min_value=fires["date"].min(),max_value=fires["date"].max())
        enddate = st.date_input(label="Enter end_date",value=fires["date"].max(),min_value=fires["date"].min(),max_value=fires["date"].max())
        fires = fires[fires["date"].between(startdate,enddate)]
        orderby = st.selectbox("Limit fires by:",list(fires.columns))
        percentage = st.number_input(label="Enter limiting percentage",min_value=0.0,max_value=100.0,value=100.0)
        percentage_rows = max(math.ceil((len(fires) / 100.0) * percentage),1)
        st.write(f"{percentage}% makes up {percentage_rows} fires")
        desc = st.toggle(label="Descending",value=True)
        fires = fires.sort_values(by=[orderby],ascending=not desc).head(percentage_rows)
        ALL_LAYERS = {
            "Weather Stations": pdk.Layer(
                "HexagonLayer",
                data=stations,
                get_position=["longitude", "latitude"],
                radius=400,
                elevation=100,
                elevation_range=[0, 1000],
                extruded=True,
            ),
            "Fires": pdk.Layer(
                "ScatterplotLayer",
                data=fires[["longitude","latitude","hectaresburnt","burnincidentid"]],
                get_position=["longitude", "latitude"],
                get_color=[200, 30, 0, 160],
                get_radius="[hectaresburnt]",
                radius_scale=10.0,
            ),
        }
        st.sidebar.markdown("### Map Layers")
        selected_layers = [
            layer
            for layer_name, layer in ALL_LAYERS.items()
            if st.sidebar.checkbox(layer_name, True)
        ]
        if selected_layers:
            st.pydeck_chart(
                pdk.Deck(
                    map_style="mapbox://styles/mapbox/light-v9",
                    initial_view_state={
                        "latitude": 45.4215,
                        "longitude": -75.6972,
                        "zoom": 8,
                        "pitch": 50,
                    },
                    layers=selected_layers,
                )
            )
        else:
            st.error("Please choose at least one layer above.")
    except URLError as e:
        st.error(
            """
            **This demo requires internet access.**

            Connection error: %s
        """
            % e.reason
        )

def facttable_page(psql_conn):
    import streamlit as st
    import pandas as pd
    import altair as alt
    import math
    import datetime

    from urllib.error import URLError

    st.markdown(f"# {list(page_names_to_funcs.keys())[2]}")
    st.write(
        """
            Use this page to explore the Fact Table data by using selectors below 
            able to slice, dice, rollup and drill down using the time and province selections.
            Iceberg queries are possible by ordering according to a column and then limiting results to X%.
        """
    )

    @st.cache_data
    def get_fact_table():
        df = psql_conn.query("SELECT * from dailyburncost;",ttl="10m")
        return df

    df: pd.DataFrame = get_fact_table()

    provinces = st.multiselect(
            "Choose provinces", list(df["fireprovinceshort"].unique()), list(df["fireprovinceshort"].unique())
        )
    
    startdate = st.date_input(label="Enter start_date",value=df["burncostdate"].min(),min_value=df["burncostdate"].min(),max_value=df["burncostdate"].max())
    enddate = st.date_input(label="Enter end_date",value=df["burncostdate"].max(),min_value=df["burncostdate"].min(),max_value=df["burncostdate"].max())

    time_dimension = st.selectbox("Choose time dimension",["Year","Month","Date","Year-Month"])

    if not provinces:
        st.error("Please select at least one province.")
    else:
        view = df[df["fireprovinceshort"].isin(provinces) & df["burncostdate"].between(startdate,enddate)]

        st.write("See top/bottom X%")
        c = list(view.columns)
        c.reverse()
        orderby = st.selectbox("Order entries by:", c)
        percentage = st.number_input(label="Enter percentage",min_value=0.0,max_value=100.0,value=100.0)
        percentage_rows = max(math.ceil((len(view) / 100.0) * percentage),1)
        st.write(f"{percentage}% makes up {percentage_rows} entries")
        desc = st.toggle(label="descending",value=True)
        st.write(view.set_index("burnincidentid").sort_values(by=[orderby],ascending=not desc).head(percentage_rows))

        groups = ["fireprovinceshort"]

        if time_dimension == "Month":
            months = pd.to_datetime(view["burncostdate"],infer_datetime_format=True,errors="raise").dt.month.rename("month")
            groups.append(months)
        elif time_dimension == "Date":
            dates = pd.to_datetime(view["burncostdate"],infer_datetime_format=True,errors="raise").dt.date
            groups.append(dates)
        elif time_dimension == "Year-Month":
            years = pd.to_datetime(view["burncostdate"],infer_datetime_format=True,errors="raise").dt.year.rename("year")
            groups.append(years)
            months = pd.to_datetime(view["burncostdate"],infer_datetime_format=True,errors="raise").dt.month.rename("month")
            groups.append(months)
        else:
            years = pd.to_datetime(view["burncostdate"],infer_datetime_format=True,errors="raise").dt.year.rename("year")
            groups.append(years)

        agg = view.groupby(groups).agg(
            fires=pd.NamedAgg(column="burnincidentid", aggfunc="count"),
            avg_hectares_burnt=pd.NamedAgg(column="hectaresburnt", aggfunc="mean"),
            total_hectares_burnt=pd.NamedAgg(column="hectaresburnt", aggfunc="sum"),
            avg_temp=pd.NamedAgg(column="averagetemperature", aggfunc="mean"),
            avg_cost=pd.NamedAgg(column="cost", aggfunc="mean"),
            total_cost=pd.NamedAgg(column="cost", aggfunc="sum"),
        )

        st.markdown("## Aggregates")

        st.write("See top/bottom X%")
        orderby = st.selectbox("Order entries by:",list(agg.columns))
        percentage = st.number_input(label="Enter percentage for agg",min_value=0.0,max_value=100.0,value=100.0)
        percentage_rows = max(math.ceil((len(agg) / 100.0) * percentage),1)
        st.write(f"{percentage}% makes up {percentage_rows} entries")
        desc = st.toggle(label="Descending",value=True)
        st.write(agg.sort_values(by=[orderby],ascending=not desc).head(percentage_rows))

        groups = ["fireprovinceshort"]

        if time_dimension == "Month" or time_dimension == "Year-Month":
            groups.append(view["burncostdate"].to_numpy().astype('datetime64[M]'))
        elif time_dimension == "Date":
            groups.append(view["burncostdate"].to_numpy().astype('datetime64[d]'))
        else:
            groups.append(view["burncostdate"].to_numpy().astype('datetime64[Y]'))
        
        agg = view.groupby(groups).agg(
            fires=pd.NamedAgg(column="burnincidentid", aggfunc="count"),
            avg_hectares_burnt=pd.NamedAgg(column="hectaresburnt", aggfunc="mean"),
            total_hectares_burnt=pd.NamedAgg(column="hectaresburnt", aggfunc="sum"),
            avg_temp=pd.NamedAgg(column="averagetemperature", aggfunc="mean"),
            avg_cost=pd.NamedAgg(column="cost", aggfunc="mean"),
            total_cost=pd.NamedAgg(column="cost", aggfunc="sum"),
        )

        for col in agg.columns:

            subagg = agg[col].reset_index().rename({"level_1":"time"},axis=1).melt(("time","fireprovinceshort"))

            chart = (
                alt.Chart(subagg)
                .mark_line()
                .encode(
                    x="time",
                    y=alt.Y("value",title=col),
                    color="fireprovinceshort",
                )
            )
            st.altair_chart(chart, use_container_width=True)


if __name__ == "__main__":

    page_names_to_funcs = {
        "—": intro,
        "Mapping": map_fires,
        "Explore Fact Table": facttable_page
    }

    conn = st.connection("postgresql",type='sql')

    demo_name = st.sidebar.selectbox("Choose a page", page_names_to_funcs.keys())
    page_names_to_funcs[demo_name](conn)
