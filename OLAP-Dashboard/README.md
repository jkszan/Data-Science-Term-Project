# OLAP queries and dashboard

## Requirements

To be able to run the queries found in [olap.ipynb](olap.ipynb) or the [dashboard](dashboard.py) you will first have to boot up and populate the postgres database. To do so follow [the README.md above](../README.md) and make sure to have installed all requirements by running `pip install -r src/requirements.txt`.

## OLAP queries

The OLAP queries are for the most part self explanatory and should be able to be seen and run by following the instructions in the notebook.

### OLAP Queries Listed

1. Rollup Query: Getting the Aggregated Hectares Burnt / Cost per Year

    ```SELECT EXTRACT(YEAR FROM burncostdate) AS year,SUM(hectaresburnt) AS hectaresburnt,SUM(cost) AS cost FROM dailyburncost GROUP BY year ORDER BY year;```

```
year |   hectaresburnt    |        cost
------+--------------------+--------------------
 1986 | 17027.881466640312 |  8799761.199425355
 1987 |  35923.44577856589 | 10032713.672953464
 1988 |   37061.5133965367 | 15754547.418136569
 1989 | 1378639.9272416614 | 327686933.20211315
 1990 |  20832.41039935276 | 10258858.663619723
 1991 |  85214.06238526855 |  36294745.85677179
 1992 |  67045.79093791095 | 11473744.399522731
 1993 |  68941.81952615915 | 7926416.2110517705
 1994 |  526351.5190959437 |  78300209.38625383
 1995 |  866634.9730330523 | 286788685.77212363
 1996 | 146752.07816767506 |   86244924.6192076
 1997 | 31904.930806261644 |  18508866.37611507
 1998 | 310891.21427160315 |  90647933.22141828
```


2. Drilldown Query: Getting Aggregated Hectares Burnt/Cost per year per Province

    ```SELECT EXTRACT(YEAR FROM burncostdate) AS year, fireprovinceshort AS province,SUM(hectaresburnt) AS hectaresburnt,SUM(cost) AS cost FROM dailyburncost GROUP BY GROUPING SETS ((year,province)) ORDER BY year;```

```
year | province |    hectaresburnt    |        cost
------+----------+---------------------+--------------------
 1986 | ON       |  16831.541084577115 |  8773381.188561335
 1986 | SK       |  196.34038206316623 | 26380.010864011005
 1987 | ON       |   14227.92792308665 |  7416257.047727833
 1987 | SK       |  21695.517855478924 | 2616456.6252257414
 1988 | ON       |   23462.85961072778 |  14138485.61731017
 1988 | MB       |  5781.8737278200515 |  711327.2413020197
 1988 | SK       |   7816.780057988726 |  904734.5595244003
 1989 | NL       |  2150.3956761946715 |  884177.2590100288
 1989 | MB       |   773174.7988482267 | 102631053.83335538
 ```

3. Slice Query: Getting all FACTs from the Year 2022

    ```SELECT * FROM dailyburncost WHERE EXTRACT(YEAR FROM burncostdate)=2022 ORDER BY burncostdate```

```
 stationid | burncostdate | provinceid | burnincidentid | fireprovinceshort | averagetemperature | maxrelativehumidity | maxwindspeedgust |   hectaresburnt    |        cost
-----------+--------------+------------+----------------+-------------------+--------------------+---------------------+------------------+--------------------+---------------------
      3608 | 2022-02-13   |          8 |         257170 | AB                |                 -2 |                 100 |               31 | 0.0503053707555555 |    71.1522747028193
      3375 | 2022-03-23   |          8 |         256865 | AB                |               11.5 |                     |                  | 0.0942326565887499 |  133.28334066276037
       635 | 2022-04-23   |          9 |         252012 | BC                |                9.9 |                  96 |                  | 0.0491556021770883 |   198.6514946743844
      3422 | 2022-04-24   |          8 |         251337 | AB                |                6.2 |                  94 |               37 | 0.8863002114565217 |  1253.5893318658614
      5690 | 2022-04-30   |          5 |         254631 | ON                |                7.2 |                  79 |                  | 0.6395851343101605 |   4649.222251104453
      3202 | 2022-05-03   |          8 |         256420 | AB                |                9.5 |                  87 |               38 | 0.0983999156321428 |    139.177541535548
      3202 | 2022-05-03   |          8 |         258926 | AB                |                9.5 |                  87 |               38 |  5.943082458089286 |   8405.938159053847
      4351 | 2022-05-09   |          7 |         257967 | SK                |                9.9 |                  75 |               32 | 0.0778792243913793 |    64.6104451648432
      8326 | 2022-05-09   |          2 |         253030 | NS                |                7.1 |                  61 |               35 | 17.888593154333332 |  26481.778129725346
      4351 | 2022-05-10   |          7 |         257968 | SK                |                7.5 |                  85 |               33 | 0.0778792243913793 |    64.6104451648432
```

4. Dice Query: Getting the per Province Per Year Aggregated Hectares Burnt and Costs for the Provinces of Quebec and British Columbia

    ```SELECT EXTRACT(YEAR FROM burncostdate) AS year,fireprovinceshort AS province,SUM(hectaresburnt) AS hectaresburnt,SUM(cost) AS cost FROM dailyburncost WHERE fireprovinceshort='QC' OR fireprovinceshort='BC' GROUP BY GROUPING SETS ((year,fireprovinceshort)) ORDER BY year;```

```
 year | province |    hectaresburnt    |        cost
------+----------+---------------------+--------------------
 1989 | BC       |     801.22030819001 |  311927.9386802062
 1989 | QC       |   417050.8967506842 | 131815762.99566288
 1990 | BC       |  1200.2578239916022 |  526053.9147644294
 1990 | QC       |   1531.475724106538 |  532390.7919900914
 1991 | QC       |  35161.019537605025 | 13062653.150757343
 1991 | BC       |   939.3577084399344 |  452381.8992486935
 1992 | QC       |  113.12012299568964 | 43169.678666505606
 1992 | BC       |    2129.72786970193 | 1070466.2137473891
 1993 | QC       |    368.954055574313 | 145879.44649781616
 1993 | BC       | 0.08747862706606391 |  49.52755939515313
 1994 | QC       |   3840.322969722674 | 1602336.2937208025
 1994 | BC       |   616.5227659669126 |  396460.8155084678
 1995 | BC       |  1483.4053860478775 | 1060775.9276190195
```

TODO: This query is functionally speaking only one if condition (if hectares burnt > 200), needs to be replaced

5. Dice Query 2: Getting all FACTs where there is greater than 200 hectares burnt
  
    ```min_cost = SELECT MIN(cost) from dailyburncost WHERE hectaresburnt > 200.0;```
  
    ```SELECT * FROM dailyburncost WHERE hectaresburnt>200.0 OR cost > {min_cost} ORDER BY hectaresburnt ASC;```

```
 stationid | burncostdate | provinceid | burnincidentid | fireprovinceshort | averagetemperature | maxrelativehumidity | maxwindspeedgust |   hectaresburnt    |        cost
-----------+--------------+------------+----------------+-------------------+--------------------+---------------------+------------------+--------------------+--------------------
      5407 | 2021-07-28   |          5 |         246454 | ON                |                 19 |                     |                  |  4.021553808366667 | 23492.970143169907
      5407 | 2021-07-10   |          5 |         246436 | ON                |               17.3 |                     |                  |  4.021553808366667 | 23492.970143169907
      5407 | 2021-07-09   |          5 |         246435 | ON                |               15.5 |                     |                  |  4.021553808366667 | 23492.970143169907
      5407 | 2021-07-08   |          5 |         246434 | ON                |               14.5 |                     |                  |  4.021553808366667 | 23492.970143169907
      5407 | 2021-07-22   |          5 |         246448 | ON                |                 17 |                     |                  |  4.021553808366667 | 23492.970143169907
      5407 | 2021-07-23   |          5 |         246449 | ON                |               17.8 |                     |                  |  4.021553808366667 | 23492.970143169907
      5407 | 2021-07-24   |          5 |         246450 | ON                |               21.3 |                     |                  |  4.021553808366667 | 23492.970143169907
      5407 | 2021-07-25   |          5 |         246451 | ON                |               21.8 |                     |                  |  4.021553808366667 | 23492.970143169907
      5407 | 2021-07-26   |          5 |         246452 | ON                |               20.8 |                     |                  |  4.021553808366667 | 23492.970143169907
```

1. Comb Query 1: Getting average cost and hectares burned from FACT Table for fires that have burned more than 10 hectares grouped by province

    ```SELECT fireprovinceshort, AVG(cost) as cost,AVG(hectaresburnt) as avg_burnt, COUNT(burnincidentid) as fires FROM dailyburncost WHERE hectaresburnt > 10 GROUP BY fireprovinceshort```

```
 fireprovinceshort |        cost        |     avg_burnt      | fires 
-------------------+--------------------+--------------------+-------
 NL                | 354559.55060377024 | 306.10658650885557 |   571
 NS                |  64389.88283441146 |  66.64730327994103 |    10
 QC                | 163011.24117769834 | 200.98308753362286 |  6750
 ON                |  253802.0590604995 |  93.87534816590585 |  9739
 MB                |  61366.15314534975 | 160.53798155974187 | 14950
 SK                |  51334.11666419762 | 154.95906082830376 | 18304
 AB                |  70249.51028253877 |  102.4188332134256 |  6006
 BC                |  302068.7708467641 | 111.05944081250921 |  9818
```

7. Comb Query 2: Getting average Temp, Hectares Burnt, and Total cost from fires burning more than 10 hectares by year.

    ```SELECT EXTRACT(YEAR FROM burncostdate) as year, AVG (averagetemperature) as avg_temp, AVG(hectaresburnt) as avg_burnt, SUM(cost) as total_cost FROM dailyburncost WHERE hectaresburnt > 10 GROUP BY year ORDER BY year;```

```
 year |      avg_temp      |     avg_burnt      |     total_cost
------+--------------------+--------------------+--------------------
 1986 | 15.065053763440854 |  84.47487399176363 |  8189994.582136311
 1987 |  16.13895747599444 |  44.88821023212052 |   8684482.33694823
 1988 | 16.056714628297343 |  34.95451087265067 | 11583502.368882671
 1989 | 17.912069756769522 |  314.3479198833328 | 323865507.15756345
 1990 |  15.83274111675128 |  45.42988452887317 |   8768303.96628786
 1991 | 17.467824497257673 |  76.43773029026994 |  35570380.01278175
 1992 | 14.533806146572124 |  155.7814339868413 | 10889832.398385208
 1993 |  12.69942196531793 | 194.76202004253352 |  7470136.468116514
 1994 | 15.614835747086047 | 184.43397109467898 |   77155473.3159063
 1995 | 16.480870396939448 | 205.92798774311274 |  283805284.2387678
 1996 |  16.08126326963902 |  74.63139936154586 |  82653252.56108153
 1997 | 14.141843971631243 | 52.883526889490824 |  17446497.18393898
```

8. Comb Query 3: Getting FACT table entries from August.

    ```SELECT burncostdate,fireprovinceshort,averagetemperature,hectaresburnt,cost FROM dailyburncost WHERE averagetemperature IS NOT NULL AND EXTRACT(MONTH FROM burncostdate)=8 ORDER BY averagetemperature DESC;```

```
 burncostdate | fireprovinceshort | averagetemperature |     hectaresburnt      |          cost
--------------+-------------------+--------------------+------------------------+------------------------
 2004-08-15   | BC                |                 31 |     1.9774237418978724 |     1997.4300525200683
 2004-08-15   | BC                |                 31 |     16.551771700793104 |      16719.23195677037
 2004-08-15   | BC                |                 31 |     5.5514422162635135 |      5607.608163413083
 2004-08-15   | BC                |                 31 |     0.2594403792630952 |      262.0652313398161
 2018-08-12   | MB                |               30.4 |     0.0075990262146666 |       6.86403339179921
 2021-08-15   | BC                |               30.2 |       24.1973123749375 |      86291.67003154563
 2021-08-15   | BC                |               30.2 |         18.66123901718 |      66549.10490465001
 2021-08-15   | BC                |               30.2 |      56.00711424971014 |      199730.7530426733
 2021-08-15   | BC                |               30.2 |     2.3543771399846154 |       8396.10334178245
 2021-08-15   | BC                |               30.2 |       1205.67675666875 |      4299645.317590054
 1998-08-06   | SK                |                 30 |     0.0241684494769072 |     3.2570188894415613
 2021-08-15   | BC                |                 30 |      957.8359991776316 |     3415803.6522676204
```

  
9. Comb Query 4: Getting fires in June, July, or August in Ontario by year


    ```SELECT EXTRACT(YEAR FROM burncostdate) as year, AVG(averagetemperature) as avg_temp,SUM(hectaresburnt) as total_hectare_burnt,SUM(cost) as cost FROM (SELECT * FROM dailyburncost WHERE EXTRACT(MONTH FROM burncostdate)=6 OR EXTRACT(MONTH FROM burncostdate)=7 OR EXTRACT(MONTH FROM burncostdate)=8 ) WHERE fireprovinceshort='ON' GROUP BY year ORDER BY year ASC;```

```
 year |      avg_temp      | total_hectare_burnt |        cost
------+--------------------+---------------------+--------------------
 1986 | 14.469951534733449 |  13564.149788102195 |  7070265.045356327
 1987 | 16.545919778699897 |   13227.67173394098 |  6894877.051119337
 1988 | 18.000817307692262 |  17475.794743917017 | 10530739.932696287
 1989 | 18.220064550833794 |  106844.98320244312 |  82501103.98634677
 1990 | 16.400829875518685 |    9200.85485524539 |  7994429.532258958
 1991 |  17.58764044943821 |   19651.12095414346 | 18315067.404853284
 1992 | 12.824260355029585 |  1637.9818507074642 | 1447734.6669142374
 1993 | 15.806329113924049 |   654.2675861598867 |  567685.0282180471
 1994 | 17.094249201277954 |  1252.5188646774718 |  1081697.797337804
 1995 | 18.146010844306755 |   137297.0886663584 | 121572654.79643449
 1996 | 15.859498480243149 |  36503.620479258956 | 35218385.763882056
 1997 | 15.334394904458597 |   910.0298040610726 |   983686.607303572
 1998 |  16.07062975027142 |  11813.211204389007 | 13448224.653155047
 1999 | 14.621813725490206 |  11751.537601831027 | 13791768.128413608
 2000 |  16.29876543209876 |  315.16228867761544 |  378041.6930960958
 2001 |  17.22814465408806 |   456.5877039496165 |  559509.3353134096
```

10.   Iceberg Query: Getting the 10 weeks of the year with the most fires, as well as their average temperature and average costs
    
      ```SELECT EXTRACT(WEEK FROM burncostdate) as week, AVG(averagetemperature) avg_temp, COUNT(burnincidentid) AS fires, AVG(cost) as avg_cost FROM dailyburncost GROUP BY week ORDER BY fires DESC LIMIT 10;```

```
 week |      avg_temp      | fires |      avg_cost      
------+--------------------+-------+--------------------
   29 |  17.75540386038046 | 14869 |  67915.83662463122
   28 | 17.535134949522444 | 14561 |  84718.62231880634
   30 | 17.739053377814695 | 14388 | 57222.219177688006
   31 |   17.2993579225225 | 14017 |  55380.37973021749
   27 | 16.919980830199563 | 13563 |  76063.62693145222
   32 | 17.460191791330914 | 13035 | 62704.451023903835
   33 | 16.882128216310335 | 11465 |  56814.51364408961
   26 |  16.58846457037751 | 11417 | 45434.346044547034
   25 | 15.653364900262055 |  9926 | 34526.378918785165
   34 | 15.785639508420681 |  8788 | 51673.873800171146
(10 rows)
```

11. Windowing Query: A ranking of months by most hectares burnt for each year

      ```SELECT EXTRACT(YEAR FROM burncostdate) as year, EXTRACT(MONTH FROM burncostdate) as month,AVG(averagetemperature) as avg_temp,SUM(hectaresburnt) as total_hectare_burnt,SUM(cost) as cost, RANK() OVER (PARTITION BY EXTRACT(YEAR FROM burncostdate) ORDER BY SUM(hectaresburnt) DESC) FROM dailyburncost GROUP BY GROUPING SETS ((year,month)) ORDER BY year,rank;```

```
year | month |       avg_temp       | total_hectare_burnt  |        cost        | rank
------+-------+----------------------+----------------------+--------------------+------
 1986 |     6 |   12.967687074829936 |      9445.2399858812 |  4890738.601219509 |    1
 1986 |     7 |   16.335377358490565 |   3971.0091406412243 | 2065723.4363922821 |    2
 1986 |     5 |   16.169620253164553 |   3185.5047963637553 | 1653527.8395789892 |    3
 1986 |     8 |   15.026203208556154 |   304.04427838601526 | 134782.23914061638 |    4
 1986 |     9 |   7.6927536231884055 |    87.11618064104631 | 45409.000674086536 |    5
 1986 |     4 |                  3.9 |     22.3485580217033 | 3002.7200579544706 |    6
 1986 |    10 |   2.8166666666666664 |   12.618526705120624 |  6577.362361875905 |    7
 1987 |     7 |    16.96859838274926 |   16441.274756242743 |   4658313.32717081 |    1
 1987 |     6 |   16.010196078431413 |   12191.896422823502 | 3176618.8636521394 |    2
 1987 |     5 |   10.720469798657735 |   3739.1830863782748 |  828913.8189015682 |    3
 1987 |     8 |   14.623546511627946 |    3487.571637192051 | 1338429.3534290253 |    4
 1987 |     4 |     8.95416666666667 |   34.191076191792334 |  15150.77679246737 |    5
```

12.  Usage of Window Clause: A calculation of the percentage of hectares burnt in a 3 month moving average

      ```SELECT burnincidentid,EXTRACT(YEAR FROM burncostdate) as year,EXTRACT(MONTH FROM burncostdate) as month,hectaresburnt, hectaresburnt / SUM(hectaresburnt) OVER W AS fires_mov_avg FROM dailyburncost GROUP BY GROUPING SETS ((burnincidentid,year,month,hectaresburnt)) WINDOW W AS (PARTITION BY EXTRACT(YEAR FROM burncostdate) ORDER BY EXTRACT(MONTH FROM burncostdate) RANGE BETWEEN '1' PRECEDING AND '1' FOLLOWING)```


```
 burnincidentid | year | month |     hectaresburnt      |     fires_mov_avg
----------------+------+-------+------------------------+------------------------
            188 | 1986 |     4 |       4.46971160434066 |  0.0013933653164756165
            189 | 1986 |     4 |       4.46971160434066 |  0.0013933653164756165
            190 | 1986 |     4 |       4.46971160434066 |  0.0013933653164756165
            186 | 1986 |     4 |       4.46971160434066 |  0.0013933653164756165
            187 | 1986 |     4 |       4.46971160434066 |  0.0013933653164756165
            274 | 1986 |     5 |     1.0265254301647333 |  8.112841678785102e-05
            273 | 1986 |     5 |     1.0265254301647333 |  8.112841678785102e-05
            109 | 1986 |     5 |     11.745024203524355 |  0.0009282334278012099
            110 | 1986 |     5 |     11.745024203524355 |  0.0009282334278012099
            111 | 1986 |     5 |     11.745024203524355 |  0.0009282334278012099
            112 | 1986 |     5 |     11.745024203524355 |  0.0009282334278012099
            548 | 1986 |     5 |     4.7396281176610975 |   0.000374582561765977
            549 | 1986 |     5 |     4.7396281176610975 |   0.000374582561765977
            272 | 1986 |     5 |     1.0265254301647333 |  8.112841678785102e-05
```

## Dashboard

For the dashboard you will have to create a file at '~/.streamlit/secrets.toml' with contents in this format (Consult Streamlit documentation for location on Windows):
  ```
  [connections.postgresql]
  dialect = "postgresql"
  host = "localhost"
  port = "5432"
  database = "firedb"
  username = "username"
  password = "password"
  ```

The dashboard is produced using the library [Streamlit](https://streamlit.io/) which produces an interactable dashboard in the browser based on the scripting in [dashboard.py](dashboard.py). To start it up, ensure all requirements are installed and run `streamlit run dashboard.py`. The start page is mostly empty so navigate with the sidebar to one of the two other pages to begin exploring the dashboard. One thing to note is that "Time-dimension" means on which timescale the aggregates are done, where "Year-Month" treats all months across the years seperately while "Month" groups all fires in e.g. March together no matter the year. Some examples of the dashboard are produced below for your convenience:

![](Fact_table1.png)

![](Fact_table2.png)

![](Fact_table3.png)

![](map_tab.png)