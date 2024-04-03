# OLAP queries and dashboard

## Requirements

To be able to run the queries found in [olap.ipynb](olap.ipynb) or the [dashboard](dashboard.py) you will first have to boot up and populate the postgres database. To do so follow [the README.md above](../README.md) and make sure to have installed all requirements by running `pip install -r src/requirements.txt`.

## OLAP queries

The OLAP queries are for the most part self explanatory and should be able to be seen and run by following the instructions in the notebook. 

## Dashboard

For the dashboard you will have to create a file at '~/.streamlit/secrets.toml' with contents in this format (Consult Streamlit documentation for location on Windows):
  ```
  [connections.postgresql]
  dialect = "postgresql"
  host = "localhost"
  port = "5432"
  database = "xxx"
  username = "xxx"
  password = "xxx"
  ```

The dashboard is produced using the library [Streamlit](https://streamlit.io/) which produces an interactable dashboard in the browser based on the scripting in [dashboard.py](dashboard.py). To start it up, ensure all requirements are installed and run `streamlit run dashboard.py`. The start page is mostly empty so navigate with the sidebar to one of the two other pages to begin exploring the dashboard. One thing to note is that "Time-dimension" means on which timescale the aggregates are done, where "Year-Month" treats all months across the years seperately while "Month" groups all fires in e.g. March together no matter the year. Some examples of the dashboard are produced below for your convenience:

![](Fact_table1.png)


![](Fact_table2.png)


![](Fact_table3.png)

![](map_tab.png)