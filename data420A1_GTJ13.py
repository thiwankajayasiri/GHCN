
#Q1 - playing with hadoop

# How is the data structured? Draw a directory tree to represent this in a sensible way.


#approach 1 for tree diagram 
hadoop fs -ls -R /data/ghcnd | awk '{print $8}' | \
sed -e 's/[^-][^\/]*\//--/g' -e 's/^/ /' -e 's/-/|/'

##Latest version for tree diagrm on the since $tree was not working
hdfs dfs -ls -R /data/ghcnd | awk '{print $8}' | sed -e 's/[^-][^\/]*\//--/g' -e 's/^/ /' -e 's/-/|/'
## check the file size how it changes
hdfs dfs -ls -R -h /data/ghcnd


#check the files in the directory
hdfs dfs -ls /data/ghcnd

#Results

# [gtj13@canterbury.ac.nz@mathmadslinux1p ~]$ hdfs dfs -ls /data/ghcnd
# Found 5 items
# -rwxr-xr-x   4 hadoop hadoop       3670 2019-03-17 21:08 /data/ghcnd/countries
# drwxr-xr-x   - hadoop hadoop          0 2019-03-17 21:27 /data/ghcnd/daily
# -rwxr-xr-x   4 hadoop hadoop   27402154 2019-03-17 21:08 /data/ghcnd/inventory
# -rwxr-xr-x   4 hadoop hadoop       1086 2019-03-17 21:07 /data/ghcnd/	states
# -rwxr-xr-x   4 hadoop hadoop    8914416 2019-03-17 21:08 /data/ghcnd/stations


#check the file size \What is the total size of all of the data? How much of that is daily?

hdfs dfs -du -h /data/ghcnd	

#Resutls
# [gtj13@canterbury.ac.nz@mathmadslinux1p ~]$ hdfs dfs -du -h /data/ghcnd
# 3.6 K   14.3 K   /data/ghcnd/countries
# 13.4 G  53.5 G   /data/ghcnd/daily
# 26.1 M  104.5 M  /data/ghcnd/inventory
# 1.1 K   4.2 K    /data/ghcnd/states
# 8.5 M   34.0 M   /data/ghcnd/stations


#How many years in the daily file

hdfs dfs -count -h /data/ghcnd/daily

# [gtj13@canterbury.ac.nz@mathmadslinux1p ~]$ hdfs dfs -count -h /data/ghcnd/daily
#            1          255             13.4 G /data/ghcnd/daily



hdfs dfs -count /data/ghcnd/daily


## making directory for the outputs
hadoop fs -mkdir /user/gtj13/outputs


hadoop fs -ls /user/gtj13





##########Question2 - Processing part##############

#Setting the schema


from pyspark.sql import SQLContext 
from pyspark.sql.types import * 
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import *

#################################
########## Processing ###########
#################################




###### Question 2 (a) #######
## Define the schema ##

from pyspark.sql.types import *

schema_Daily = StructType([
    StructField("ID", StringType(), True),
    StructField("DATE", StringType(), True),
    StructField("ELEMENT", StringType(), True),
    StructField("VALUE", DoubleType(), True),
    StructField("MEASUREMENT", StringType(), True),
    StructField("QUALITY FLAG", StringType(), True),
    StructField("SOURCE FLAG", StringType(), True),          
    StructField("OBSERVATION TIME", StringType(), True),
])

schema_Stations = StructType([
    StructField("ID", StringType(), True),
    StructField("LATITUDE", DoubleType(), True),
    StructField("LONGITUDE", DoubleType(), True),
    StructField("ELEVATION", DoubleType(), True),
    StructField("STATE", StringType(), True),
    StructField("NAME", StringType(), True),
    StructField("GSN FLAG", StringType(), True),
    StructField("HCN/CRN FLAG", StringType(), True),
    StructField("WMO ID", StringType(), True),
])

schema_Countries = StructType([
    StructField("CODE", StringType(), True),
    StructField("NAME", StringType(), True),
])

schema_States = StructType([
    StructField("CODE", StringType(), True),
    StructField("NAME", StringType(), True),
])

schema_Inventory = StructType([
    StructField("ID", StringType(), True),
    StructField("LATITUDE", DoubleType(), True),
    StructField("LONGITUDE", DoubleType(), True),
    StructField("ELEMENT", StringType(), True),
    StructField("FIRSTYEAR", IntegerType(), True),
    StructField("LASTYEAR", IntegerType(), True),
])


###### Question 2 (b) #######

## Load the rows ##
# Imports

from pyspark.sql.types import *

# Load

schema_dailys = StructType([
    StructField("ID", StringType(), True),
    StructField("DATE", StringType(), True),
    StructField("ELEMENT", StringType(), True),
    StructField("VALUE", StringType(), True),
    StructField("MEASUREMENT", StringType(), True),
    StructField("QUALITY FLAG", StringType(), True),
    StructField("SOURCE FLAG", StringType(), True),          
    StructField("OBSERVATION TIME", StringType(), True),
])

dailys = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_dailys)
    .load("hdfs:///data/ghcnd/daily/2017.csv.gz")
    .limit(1000)
)

dailys.show()

###### Question 2 (c) #######

from pyspark.sql import functions as F

### station ###

stations_text_only = (
    spark.read.format('text')
    .load("hdfs:///data/ghcnd/stations")
)

stations = stations_text_only.select(
    F.trim(F.substring(F.col('value'), 1, 11)).alias('ID').cast(schema_Stations['ID'].dataType),
    F.trim(F.substring(F.col('value'), 13, 8)).alias('LATITUDE').cast(schema_Stations['LATITUDE'].dataType),
    F.trim(F.substring(F.col('value'), 22, 9)).alias('LONGITUDE').cast(schema_Stations['LONGITUDE'].dataType),
    F.trim(F.substring(F.col('value'), 32, 6)).alias('ELEVATION').cast(schema_Stations['ELEVATION'].dataType),
    F.trim(F.substring(F.col('value'), 39, 2)).alias('STATE').cast(schema_Stations['STATE'].dataType),
    F.trim(F.substring(F.col('value'), 42, 30)).alias('NAME').cast(schema_Stations['NAME'].dataType),
    F.trim(F.substring(F.col('value'), 73, 3)).alias('GSN FLAG').cast(schema_Stations['GSN FLAG'].dataType),
    F.trim(F.substring(F.col('value'), 77, 3)).alias('HCN/CRN FLAG').cast(schema_Stations['HCN/CRN FLAG'].dataType),
    F.trim(F.substring(F.col('value'), 81, 5)).alias('WMO ID').cast(schema_Stations['WMO ID'].dataType)
    )

stations.count()


stations.filter(stations['WMO ID'] == '').count()

### countries ### 

countries_text_only = (
    spark.read.format('text')
    .load("hdfs:///data/ghcnd/countries")
)


countries = countries_text_only.select(
    F.trim(F.substring(F.col('value'), 1, 2)).alias('CODE').cast(schema_Countries['CODE'].dataType),
    F.trim(F.substring(F.col('value'), 4, 47)).alias('NAME').cast(schema_Countries['NAME'].dataType)
    )


countries.count()

### states ###

states_text_only = (
    spark.read.format('text')
    .load("hdfs:///data/ghcnd/states")
)


states = states_text_only.select(
    F.trim(F.substring(F.col('value'), 1, 2)).alias('CODE').cast(schema_States['CODE'].dataType),
    F.trim(F.substring(F.col('value'), 4, 47)).alias('NAME').cast(schema_States['NAME'].dataType)
    )


states.count()

### inventory ###

inventory_text_only = (
    spark.read.format('text')
    .load("hdfs:///data/ghcnd/inventory")
)

inventory = inventory_text_only.select(
    F.trim(F.substring(F.col('value'), 1, 11)).alias('ID').cast(schema_Inventory['ID'].dataType),
    F.trim(F.substring(F.col('value'), 13, 8)).alias('LATITUDE').cast(schema_Inventory['LATITUDE'].dataType),
    F.trim(F.substring(F.col('value'), 22, 9)).alias('LONGITUDE').cast(schema_Inventory['LONGITUDE'].dataType),
    F.trim(F.substring(F.col('value'), 32, 4)).alias('ELEMENT').cast(schema_Inventory['ELEMENT'].dataType),
    F.trim(F.substring(F.col('value'), 37, 4)).alias('FIRSTYEAR').cast(schema_Inventory['FIRSTYEAR'].dataType),
    F.trim(F.substring(F.col('value'), 42, 4)).alias('LASTYEAR').cast(schema_Inventory['LASTYEAR'].dataType)
    )

inventory.count()


###### Question 3 (a) #######

country_code = F.substring(F.col('ID'), 1, 2)
stations_a = stations.withColumn('CODE', country_code)
stations_a.show()


# +-----------+--------+---------+---------+-----+--------------------+--------+------------+------+----+
# |         ID|LATITUDE|LONGITUDE|ELEVATION|STATE|                NAME|GSN FLAG|HCN/CRN FLAG|WMO ID|CODE|
# +-----------+--------+---------+---------+-----+--------------------+--------+------------+------+----+
# |ACW00011604| 17.1167| -61.7833|     10.1|     |ST JOHNS COOLIDGE...|        |            |      |  AC|
# |ACW00011647| 17.1333| -61.7833|     19.2|     |            ST JOHNS|        |            |      |  AC|
# |AE000041196|  25.333|   55.517|     34.0|     | SHARJAH INTER. AIRP|     GSN|            | 41196|  AE|
# |AEM00041194|  25.255|   55.364|     10.4|     |          DUBAI INTL|        |            | 41194|  AE|
# |AEM00041217|  24.433|   54.651|     26.8|     |      ABU DHABI INTL|        |            | 41217|  AE|
# |AEM00041218|  24.262|   55.609|    264.9|     |         AL AIN INTL|        |            | 41218|  AE|
# |AF000040930|  35.317|   69.017|   3366.0|     |        NORTH-SALANG|     GSN|            | 40930|  AF|
# |AFM00040938|   34.21|   62.228|    977.2|     |               HERAT|        |            | 40938|  AF|
# |AFM00040948|  34.566|   69.212|   1791.3|     |          KABUL INTL|        |            | 40948|  AF|
# |AFM00040990|    31.5|    65.85|   1010.0|     |    KANDAHAR AIRPORT|        |            | 40990|  AF|
# |AG000060390| 36.7167|     3.25|     24.0|     |  ALGER-DAR EL BEIDA|     GSN|            | 60390|  AG|
# |AG000060590| 30.5667|   2.8667|    397.0|     |            EL-GOLEA|     GSN|            | 60590|  AG|
# |AG000060611|   28.05|   9.6331|    561.0|     |           IN-AMENAS|     GSN|            | 60611|  AG|
# |AG000060680|    22.8|   5.4331|   1362.0|     |         TAMANRASSET|     GSN|            | 60680|  AG|
# |AGE00135039| 35.7297|     0.65|     50.0|     |ORAN-HOPITAL MILI...|        |            |      |  AG|
# |AGE00147704|   36.97|     7.79|    161.0|     | ANNABA-CAP DE GARDE|        |            |      |  AG|
# |AGE00147705|   36.78|     3.07|     59.0|     |ALGIERS-VILLE/UNI...|        |            |      |  AG|
# |AGE00147706|    36.8|     3.03|    344.0|     |   ALGIERS-BOUZAREAH|        |            |      |  AG|
# |AGE00147707|    36.8|     3.04|     38.0|     |  ALGIERS-CAP CAXINE|        |            |      |  AG|
# |AGE00147708|   36.72|     4.05|    222.0|     |          TIZI OUZOU|        |            | 60395|  AG|
# +-----------+--------+---------+---------+-----+--------------------+--------+------------+------+----+



###### Question 3 (b) #######
countries = countries.select(
    F.col('CODE'),
    F.col('NAME').alias('COUNTRY_NAME')
    )

stations_b = stations_a.join(countries,"CODE","left")
stations_b.show()


# +----+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+--------------------+
# |CODE|         ID|LATITUDE|LONGITUDE|ELEVATION|STATE|                NAME|GSN FLAG|HCN/CRN FLAG|WMO ID|        COUNTRY_NAME|
# +----+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+--------------------+
# |  AC|ACW00011604| 17.1167| -61.7833|     10.1|     |ST JOHNS COOLIDGE...|        |            |      | Antigua and Barbuda|
# |  AC|ACW00011647| 17.1333| -61.7833|     19.2|     |            ST JOHNS|        |            |      | Antigua and Barbuda|
# |  AE|AE000041196|  25.333|   55.517|     34.0|     | SHARJAH INTER. AIRP|     GSN|            | 41196|United Arab Emirates|
# |  AE|AEM00041194|  25.255|   55.364|     10.4|     |          DUBAI INTL|        |            | 41194|United Arab Emirates|
# |  AE|AEM00041217|  24.433|   54.651|     26.8|     |      ABU DHABI INTL|        |            | 41217|United Arab Emirates|
# |  AE|AEM00041218|  24.262|   55.609|    264.9|     |         AL AIN INTL|        |            | 41218|United Arab Emirates|
# |  AF|AF000040930|  35.317|   69.017|   3366.0|     |        NORTH-SALANG|     GSN|            | 40930|         Afghanistan|
# |  AF|AFM00040938|   34.21|   62.228|    977.2|     |               HERAT|        |            | 40938|         Afghanistan|
# |  AF|AFM00040948|  34.566|   69.212|   1791.3|     |          KABUL INTL|        |            | 40948|         Afghanistan|
# |  AF|AFM00040990|    31.5|    65.85|   1010.0|     |    KANDAHAR AIRPORT|        |            | 40990|         Afghanistan|
# |  AG|AG000060390| 36.7167|     3.25|     24.0|     |  ALGER-DAR EL BEIDA|     GSN|            | 60390|             Algeria|
# |  AG|AG000060590| 30.5667|   2.8667|    397.0|     |            EL-GOLEA|     GSN|            | 60590|             Algeria|
# |  AG|AG000060611|   28.05|   9.6331|    561.0|     |           IN-AMENAS|     GSN|            | 60611|             Algeria|
# |  AG|AG000060680|    22.8|   5.4331|   1362.0|     |         TAMANRASSET|     GSN|            | 60680|             Algeria|
# |  AG|AGE00135039| 35.7297|     0.65|     50.0|     |ORAN-HOPITAL MILI...|        |            |      |             Algeria|
# |  AG|AGE00147704|   36.97|     7.79|    161.0|     | ANNABA-CAP DE GARDE|        |            |      |             Algeria|
# |  AG|AGE00147705|   36.78|     3.07|     59.0|     |ALGIERS-VILLE/UNI...|        |            |      |             Algeria|
# |  AG|AGE00147706|    36.8|     3.03|    344.0|     |   ALGIERS-BOUZAREAH|        |            |      |             Algeria|
# |  AG|AGE00147707|    36.8|     3.04|     38.0|     |  ALGIERS-CAP CAXINE|        |            |      |             Algeria|
# |  AG|AGE00147708|   36.72|     4.05|    222.0|     |          TIZI OUZOU|        |            | 60395|             Algeria|
# +----+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+--------------------+
# only showing top 20 rows











## another method##

stations_b = (
    stations_a
    .join(
        countries
        .select(
            F.col('CODE'),
            F.col('NAME').alias('COUNTRY_NAME')
        ),
        on='CODE', how='left')
    )

stations_b.show()


###### Question 3 (c) #######


# stations_c = stations_b.join(states, stations.STATE == states.STATE_CODE, 'left_outer').drop('STATE_CODE')
# stations_c.show()

stations_c = (
    stations_b
    .join(
        states
        .select(
             F.col('CODE'),
             F.col('NAME').alias('STATE_NAME')
        ),
        on = 'CODE',how = 'left')
    )

stations_c.show()

# |CODE|         ID|LATITUDE|LONGITUDE|ELEVATION|STATE|                NAME|GSN FLAG|HCN/CRN FLAG|WMO ID|        COUNTRY_NAME|STATE_NAME|
# +----+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+--------------------+----------+
# |  AC|ACW00011604| 17.1167| -61.7833|     10.1|     |ST JOHNS COOLIDGE...|        |            |      | Antigua and Barbuda|      null|
# |  AC|ACW00011647| 17.1333| -61.7833|     19.2|     |            ST JOHNS|        |            |      | Antigua and Barbuda|      null|
# |  AE|AE000041196|  25.333|   55.517|     34.0|     | SHARJAH INTER. AIRP|     GSN|            | 41196|United Arab Emirates|      null|
# |  AE|AEM00041194|  25.255|   55.364|     10.4|     |          DUBAI INTL|        |            | 41194|United Arab Emirates|      null|
# |  AE|AEM00041217|  24.433|   54.651|     26.8|     |      ABU DHABI INTL|        |            | 41217|United Arab Emirates|      null|
# |  AE|AEM00041218|  24.262|   55.609|    264.9|     |         AL AIN INTL|        |            | 41218|United Arab Emirates|      null|
# |  AF|AF000040930|  35.317|   69.017|   3366.0|     |        NORTH-SALANG|     GSN|            | 40930|         Afghanistan|      null|
# |  AF|AFM00040938|   34.21|   62.228|    977.2|     |               HERAT|        |            | 40938|         Afghanistan|      null|
# |  AF|AFM00040948|  34.566|   69.212|   1791.3|     |          KABUL INTL|        |            | 40948|         Afghanistan|      null|
# |  AF|AFM00040990|    31.5|    65.85|   1010.0|     |    KANDAHAR AIRPORT|        |            | 40990|         Afghanistan|      null|
# |  AG|AG000060390| 36.7167|     3.25|     24.0|     |  ALGER-DAR EL BEIDA|     GSN|            | 60390|             Algeria|      null|
# |  AG|AG000060590| 30.5667|   2.8667|    397.0|     |            EL-GOLEA|     GSN|            | 60590|             Algeria|      null|
# |  AG|AG000060611|   28.05|   9.6331|    561.0|     |           IN-AMENAS|     GSN|            | 60611|             Algeria|      null|
# |  AG|AG000060680|    22.8|   5.4331|   1362.0|     |         TAMANRASSET|     GSN|            | 60680|             Algeria|      null|
# |  AG|AGE00135039| 35.7297|     0.65|     50.0|     |ORAN-HOPITAL MILI...|        |            |      |             Algeria|      null|
# |  AG|AGE00147704|   36.97|     7.79|    161.0|     | ANNABA-CAP DE GARDE|        |            |      |             Algeria|      null|
# |  AG|AGE00147705|   36.78|     3.07|     59.0|     |ALGIERS-VILLE/UNI...|        |            |      |             Algeria|      null|
# |  AG|AGE00147706|    36.8|     3.03|    344.0|     |   ALGIERS-BOUZAREAH|        |            |      |             Algeria|      null|
# |  AG|AGE00147707|    36.8|     3.04|     38.0|     |  ALGIERS-CAP CAXINE|        |            |      |             Algeria|      null|
# |  AG|AGE00147708|   36.72|     4.05|    222.0|     |          TIZI OUZOU|        |            | 60395|             Algeria|      null|




###### Question 3 (d) #######


new_inv = (
    inventory
    .groupBy('ID')
    .agg(
        F.min('FIRSTYEAR').alias('MINIMUM_YEAR'),
        F.max('LASTYEAR').alias('MAXIMUM_YEAR'),
        F.countDistinct('ELEMENT').alias('NUM_ELEMENTS')
        )
    .join(
        inventory
        .filter((inventory.ELEMENT == 'PRCP') | (inventory.ELEMENT == 'SNOW')| (inventory.ELEMENT == 'SNWD')| (inventory.ELEMENT == 'TMAX')| (inventory.ELEMENT == 'TMIN'))
        .groupBy('ID')
        .agg(
            F.countDistinct('ELEMENT').alias('NUM_CORE_ELEMENTS')
            ), 'ID', 'left'
        )
    .join(
        inventory
        .filter(inventory.ELEMENT == 'PRCP')
        .groupBy('ID')
        .agg(
            F.countDistinct('ELEMENT').alias('PRCP')
            ), 'ID', 'left'
        )

    )
new_inv = new_inv.withColumn('NUM_OTHER_ELEMENTS', F.col('NUM_ELEMENTS') - F.col('NUM_CORE_ELEMENTS'))
new_inv.show()

# +-----------+------------+------------+------------+-----------------+----+------------------+
# |         ID|MINIMUM_YEAR|MAXIMUM_YEAR|NUM_ELEMENTS|NUM_CORE_ELEMENTS|PRCP|NUM_OTHER_ELEMENTS|
# +-----------+------------+------------+------------+-----------------+----+------------------+
# |ACW00011647|        1957|        1970|           7|                5|   1|                 2|
# |AEM00041217|        1983|        2017|           4|                3|   1|                 1|
# |AG000060590|        1892|        2017|           4|                3|   1|                 1|
# |AGE00147706|        1893|        1920|           3|                3|   1|                 0|
# |AGE00147708|        1879|        2017|           5|                4|   1|                 1|
# |AGE00147709|        1879|        1938|           3|                3|   1|                 0|
# |AGE00147710|        1909|        2009|           4|                3|   1|                 1|
# |AGE00147711|        1880|        1938|           3|                3|   1|                 0|
# |AGE00147714|        1896|        1938|           3|                3|   1|                 0|
# |AGE00147719|        1888|        2017|           4|                3|   1|                 1|
# |AGM00060351|        1981|        2017|           4|                3|   1|                 1|
# |AGM00060353|        1996|        2017|           4|                3|   1|                 1|
# |AGM00060360|        1945|        2017|           4|                3|   1|                 1|
# |AGM00060387|        1995|        2004|           4|                3|   1|                 1|
# |AGM00060445|        1957|        2017|           5|                4|   1|                 1|
# |AGM00060452|        1985|        2017|           4|                3|   1|                 1|
# |AGM00060467|        1981|        2017|           4|                3|   1|                 1|
# |AGM00060468|        1973|        2017|           5|                4|   1|                 1|
# |AGM00060507|        1943|        2017|           5|                4|   1|                 1|
# |AGM00060511|        1983|        2017|           5|                4|   1|                 1|
# +-----------+------------+------------+------------+-----------------+----+------------------+
# only showing top 20 rows






stations_inventory_5 = new_inv.filter(F.col('NUM_CORE_ELEMENTS') == 5)
stations_inventory_5.count()
#Out[10]: 20224



stations_inventory_pre = new_inv.filter((F.col('NUM_ELEMENTS') == 1) & (F.col('PRCP') == 1))
stations_inventory_pre.count()
# 15970



stations_inventory_other = new_inv.filter((F.col('NUM_OTHER_ELEMENTS') >= 1 ))
stations_inventory_other.show()
stations_inventory_other.count()

# +-----------+------------+------------+------------+-----------------+----+------------------+
# |         ID|MINIMUM_YEAR|MAXIMUM_YEAR|NUM_ELEMENTS|NUM_CORE_ELEMENTS|PRCP|NUM_OTHER_ELEMENTS|
# +-----------+------------+------------+------------+-----------------+----+------------------+
# |ACW00011647|        1957|        1970|           7|                5|   1|                 2|
# |AEM00041217|        1983|        2017|           4|                3|   1|                 1|
# |AG000060590|        1892|        2017|           4|                3|   1|                 1|
# |AGE00147708|        1879|        2017|           5|                4|   1|                 1|
# |AGE00147710|        1909|        2009|           4|                3|   1|                 1|
# |AGE00147719|        1888|        2017|           4|                3|   1|                 1|
# |AGM00060351|        1981|        2017|           4|                3|   1|                 1|
# |AGM00060353|        1996|        2017|           4|                3|   1|                 1|
# |AGM00060360|        1945|        2017|           4|                3|   1|                 1|
# |AGM00060387|        1995|        2004|           4|                3|   1|                 1|
# |AGM00060445|        1957|        2017|           5|                4|   1|                 1|
# |AGM00060452|        1985|        2017|           4|                3|   1|                 1|
# |AGM00060467|        1981|        2017|           4|                3|   1|                 1|
# |AGM00060468|        1973|        2017|           5|                4|   1|                 1|
# |AGM00060507|        1943|        2017|           5|                4|   1|                 1|
# |AGM00060511|        1983|        2017|           5|                4|   1|                 1|
# |AGM00060535|        1973|        2017|           5|                4|   1|                 1|
# |AGM00060540|        1981|        2017|           5|                4|   1|                 1|
# |AGM00060602|        1973|        2017|           5|                4|   1|                 1|
# |AGM00060603|        2007|        2017|           4|                3|   1|                 1|
# +-----------+------------+------------+------------+-----------------+----+------------------+
# only showing top 20 rows

# Out[12]: 67506



###### Question 3 (e) #######

stations_inventory = (
    stations_c
    .join(
        new_inv,on='ID', how='left'))

stations_inventory.show()
stations_inventory.write.csv("hdfs:///user/gtj13/outputs/ghcnd/stations_inventory")

# +-----------+----+--------+---------+---------+-----+--------------------+--------+------------+------+--------------------+----------+------------+------------+------------+-----------------+----+------------------+
# |         ID|CODE|LATITUDE|LONGITUDE|ELEVATION|STATE|                NAME|GSN FLAG|HCN/CRN FLAG|WMO ID|        COUNTRY_NAME|STATE_NAME|MINIMUM_YEAR|MAXIMUM_YEAR|NUM_ELEMENTS|NUM_CORE_ELEMENTS|PRCP|NUM_OTHER_ELEMENTS|
# +-----------+----+--------+---------+---------+-----+--------------------+--------+------------+------+--------------------+----------+------------+------------+------------+-----------------+----+------------------+
# |ACW00011647|  AC| 17.1333| -61.7833|     19.2|     |            ST JOHNS|        |            |      | Antigua and Barbuda|      null|        1957|        1970|           7|                5|   1|                 2|
# |AEM00041217|  AE|  24.433|   54.651|     26.8|     |      ABU DHABI INTL|        |            | 41217|United Arab Emirates|      null|        1983|        2017|           4|                3|   1|                 1|
# |AG000060590|  AG| 30.5667|   2.8667|    397.0|     |            EL-GOLEA|     GSN|            | 60590|             Algeria|      null|        1892|        2017|           4|                3|   1|                 1|
# |AGE00147706|  AG|    36.8|     3.03|    344.0|     |   ALGIERS-BOUZAREAH|        |            |      |             Algeria|      null|        1893|        1920|           3|                3|   1|                 0|
# |AGE00147708|  AG|   36.72|     4.05|    222.0|     |          TIZI OUZOU|        |            | 60395|             Algeria|      null|        1879|        2017|           5|                4|   1|                 1|
# |AGE00147709|  AG|   36.63|      4.2|    942.0|     |       FORT NATIONAL|        |            |      |             Algeria|      null|        1879|        1938|           3|                3|   1|                 0|
# |AGE00147710|  AG|   36.75|      5.1|      9.0|     |BEJAIA-BOUGIE (PORT)|        |            | 60401|             Algeria|      null|        1909|        2009|           4|                3|   1|                 1|
# |AGE00147711|  AG| 36.3697|     6.62|    660.0|     |         CONSTANTINE|        |            |      |             Algeria|      null|        1880|        1938|           3|                3|   1|                 0|
# |AGE00147714|  AG|   35.77|      0.8|     78.0|     |     ORAN-CAP FALCON|        |            |      |             Algeria|      null|        1896|        1938|           3|                3|   1|                 0|
# |AGE00147719|  AG| 33.7997|     2.89|    767.0|     |            LAGHOUAT|        |            | 60545|             Algeria|      null|        1888|        2017|           4|                3|   1|                 1|
# |AGM00060351|  AG|  36.795|    5.874|     11.0|     |               JIJEL|        |            | 60351|             Algeria|      null|        1981|        2017|           4|                3|   1|                 1|
# |AGM00060353|  AG|  36.817|    5.883|      6.0|     |          JIJEL-PORT|        |            | 60353|             Algeria|      null|        1996|        2017|           4|                3|   1|                 1|
# |AGM00060360|  AG|  36.822|    7.809|      4.9|     |              ANNABA|        |            | 60360|             Algeria|      null|        1945|        2017|           4|                3|   1|                 1|
# |AGM00060387|  AG|  36.917|     3.95|      8.0|     |              DELLYS|        |            | 60387|             Algeria|      null|        1995|        2004|           4|                3|   1|                 1|
# |AGM00060445|  AG|  36.178|    5.324|   1050.0|     |     SETIF AIN ARNAT|        |            | 60445|             Algeria|      null|        1957|        2017|           5|                4|   1|                 1|
# |AGM00060452|  AG|  35.817|   -0.267|      4.0|     |               ARZEW|        |            | 60452|             Algeria|      null|        1985|        2017|           4|                3|   1|                 1|
# |AGM00060467|  AG|  35.667|      4.5|    442.0|     |              M'SILA|        |            | 60467|             Algeria|      null|        1981|        2017|           4|                3|   1|                 1|
# |AGM00060468|  AG|   35.55|    6.183|   1052.0|     |               BATNA|        |            | 60468|             Algeria|      null|        1973|        2017|           5|                4|   1|                 1|
# |AGM00060507|  AG|  35.208|    0.147|    513.9|     |              GHRISS|        |            | 60507|             Algeria|      null|        1943|        2017|           5|                4|   1|                 1|
# |AGM00060511|  AG|  35.341|    1.463|    989.1|     |          BOU CHEKIF|        |            | 60511|             Algeria|      null|        1983|        2017|           5|                4|   1|                 1|
# +-----------+----+--------+---------+---------+-----+--------------------+--------+------------+------+--------------------+----------+------------+------------+------------+-----------------+----+------------------+
# only showing top 20 rows



schema_newStations = StructType([
    StructField("ID", StringType(), True),
    StructField("CODE", StringType(), True),
    StructField("LATITUDE", DoubleType(), True),
    StructField("LONGITUDE", DoubleType(), True),
    StructField("ELEVATION", DoubleType(), True),
    StructField("STATE", StringType(), True),
    StructField("NAME", StringType(), True),
    StructField("GSN FLAG", StringType(), True),
    StructField("HCN/CRN FLAG", StringType(), True),
    StructField("WMO ID", StringType(), True),
    StructField("COUNTRY_NAME", StringType(), True),
    StructField("STATE_NAME", StringType(), True),  
    StructField("MINIMUM_YEAR", IntegerType(), True), 
    StructField("MAXIMUM_YEAR", IntegerType(), True), 
    StructField("NUM_ELEMENTS", IntegerType(), True), 
    StructField("NUM_CORE_ELEMENTS", IntegerType(), True), 
    StructField("PRCP", IntegerType(), True), 
    StructField("NUM_OTHER_ELEMENTS", IntegerType(), True), 
])

stations_inventory = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_newStations)
    .load("hdfs:///user/gtj13/outputs/ghcnd/stations_inventory")
)




###### Question 3 (f) #######

daily_stations = dailys.join(stations_inventory, dailys.ID == stations_inventory.ID, 'left_outer')
# count the number of stations which are in daily but not in stations
dailys.select('ID').subtract(stations_inventory.select('ID')).count()
# 0








#################################
########## Processing ###########
#################################

###### Question 1 (a) #######
 
stations_inventory.select('ID').distinct().count()
#Out[4]: How many stations - 103656

stations_inventory.filter(stations_inventory.MAXIMUM_YEAR >= 2017).select('ID').distinct().count() 
#Out[7]: How many stations have been active in 2017 - 37546


from pyspark.sql.functions import col


stations_inventory.filter(col('GSN FLAG') == 'GSN').count()
#Out[26]: 991

stations_inventory.filter(col('HCN/CRN FLAG') == 'HCN').count()
#Out[27]: 1218

stations_inventory.filter(col('HCN/CRN FLAG') == 'CRN').count()
#Out[28]: 230

stations_inventory.filter((col('HCN/CRN FLAG') != '')& (col('GSN FLAG') != '')).count()
#Out[7]: 14

###### Question 1 (b) #######
# Count the total number of stations in each country, and store the output in countries using
# the withColumnRenamed command.
# Method 1 
count_country = stations_inventory.groupby('COUNTRY_NAME').agg({'ID':'count'})


count_country = (count_country
                        .withColumnRenamed('COUNTRY_NAME','NAME')
                        .withColumnRenamed('count(ID)' , 'number_of_stations'))

countries = (
    countries
    .join(
        count_country
        .select(
            F.col('NAME'), 
            F.col('number_of_stations'), 
        ),
              
        on='NAME', how='left')
    )

countries.show()

# new_df = pd.merge(A_df, B_df,  how='left', left_on='[A_c1,c2]', right_on = '[B_c1,c2]')
# import pandas as pd 
# pd.merge(left, right, on='key')
# countries1  = pd.merge(count_country, countries, how ='left', indicator =True)

# countries = (
#     countries
#         .join(
#             count_country
#             .select(
#                 F.col('NAME'),
#                 F.col('number_of_stations')
#                 ),
#             on='NAME' , how='left')
            
#     )
# countries.show()

from pyspark.sql import functions as F
countries = (
    countries
        .join(
        count_country
        .select(
            F.col('COUNTRY_NAME'),
            F.col('count(ID)').alias('number_of_stations')
        ),
        on='COUNTRY_NAME', how='left')
    )

countries.show()

countries.write.csv("hdfs:///user/gtj13/outputs/ghcnd/countries", mode = 'overwrite', header = True)

#####

count_states = stations_inventory.groupby('STATE').agg({'ID':'count'})
states = (
    states
    .join(
        count_states
        .select(
            F.col('STATE').alias('CODE'),
            F.col('count(ID)').alias('number_of_stations')
        ),
        on='CODE', how='left')
    )
states.show()

states.write.csv("hdfs:///user/gtj13/outputs/ghcnd/states", mode = 'overwrite', header = True)


###### Question 1 (C) #######

stations_inventory.filter(col('LATITUDE') < 0).count()
##Out[33]: 25337 stations in Southern Hemisphere

stations_inventory.filter(col('COUNTRY_NAME') == 'United States').count()
##Out[34]: 56918 stations are there in total in the territories of the United States


###### Question 2 (a) #######
import math

def calculation(la1, lo1, la2, lo2):
    R = 6373.0
    dlo = radians(lo2 - lo1)
    dla = radians(la2 - la1)

    a = (sin(dla/2))**2 + cos(radians(la1)) * cos(radians(la2)) * (sin(dlo/2))**2
    
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    
    distance = R * c
    return distance
### another method, its the same

calculation_udf = F.udf(lambda x, y, a, b: calculation(x, y, a, b), DoubleType())

def calculation(la1, lo1, la2, lo2):
    R = 6373.0
    distance = math.acos(
        math.sin(math.radians(la1)) * math.sin(math.radians(la2)) + 
        math.cos(math.radians(la1)) * math.cos(math.radians(la2)) * 
        math.cos(math.radians(lo1 - lo2))
    ) * R
    return distance

calculation_udf = F.udf(lambda x, y, a, b: calculation(x, y, a, b), DoubleType())



### TESTING

#test_1 = stations_inventory.sample(False, 0.1, 5)
#test_1 = test_1 .limit(10000)
#test_1 .count()

#test_2 = stations_inventory.sample(False, 0.1, 6)
#test_2 = test_2.limit(10000)
#test_2.count()

#test_1 = test_1.select('NAME', 'LATITUDE', 'LONGITUDE')
#test_1 = test_1.toDF('NAME1', 'LATITUDE1', 'LONGITUDE1')
#test_2 = test_2.select('NAME', 'LATITUDE', 'LONGITUDE')
#test_2 = test_2.toDF('NAME2', 'LATITUDE2', 'LONGITUDE2')

#test_1.show(n = 5)
#test_2.show(n = 5)
#test = test_1.crossJoin(test_2)
#test.show(n = 10)

#test_distance = test.withColumn('DISTANCE', calculation_udf(F.col('LATITUDE1'), F.col('LONGITUDE1'), F.col('LATITUDE2'), F.col('LONGITUDE2')))
#test_distance.show(n = 10)

###### Question 2 (b) #######

NZ = stations_inventory.filter(col('COUNTRY_NAME') == 'New Zealand')

NZ1 = NZ.select('NAME', 'LATITUDE', 'LONGITUDE').toDF('NAME1', 'LATITUDE1', 'LONGITUDE1')
NZ2 = NZ.select('NAME', 'LATITUDE', 'LONGITUDE').toDF('NAME2', 'LATITUDE2', 'LONGITUDE2')

#NAME1 or NAME2
NZ = NZ1.crossJoin(NZ2).filter(F.col('NAME1') != F.col('NAME2'))
NZ.show(n = 10)

NZ_result = NZ.withColumn('distance', calculation_udf(F.col('LATITUDE1'), F.col('LONGITUDE1'), F.col('LATITUDE2'), F.col('LONGITUDE2')))
NZ_result.show()

NZ_result.orderBy("distance").show()

NZ_result.write.csv("hdfs:///user/gtj13/outputs/ghcnd/NZ_result")

###### Question 3 (a) #######
hdfs dfs -ls hdfs:///data/ghcnd/daily/2017*
hdfs getconf -confKey "dfs.blocksize"##134217728
hdfs dfs -ls hdfs:///data/ghcnd/daily/2010*##207181730
hdfs fsck hdfs:///data/ghcnd/daily/2010.csv.gz -files -blocks  ##134217728 72964002

###### Question 3 (b) #######
schema_Daily = StructType([
    StructField("ID", StringType(), True),
    StructField("DATE", StringType(), True),
    StructField("ELEMENT", StringType(), True),
    StructField("VALUE", DoubleType(), True),
    StructField("MEASUREMENT", StringType(), True),
    StructField("QUALITY FLAG", StringType(), True),
    StructField("SOURCE FLAG", StringType(), True),          StructField("OBSERVATION TIME", StringType(), True),
])


daily_2010_2017 = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", False)
    .option("inferSchema", False)
    .schema(schema_Daily)
    .load("hdfs:///data/ghcnd/daily/{2010,2017}.csv.gz")
)
daily_2010_2017.count()

###Out[5]: 58851079


###### Question 3 (c) #######
daily_sixyear = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", False)
    .option("inferSchema", False)
    .schema(schema_Daily)
    .load("hdfs:///data/ghcnd/daily/{2010,2011,2012,2013,2014,2015}.csv.gz")
)
daily_sixyear.count()
daily_sixyear.show()
##Out[6]: 207716098


# +-----------+--------+-------+-----+-----------+------------+-----------+----------------+
# |         ID|    DATE|ELEMENT|VALUE|MEASUREMENT|QUALITY FLAG|SOURCE FLAG|OBSERVATION TIME|
# +-----------+--------+-------+-----+-----------+------------+-----------+----------------+
# |ASN00015643|20100101|   TMAX|414.0|       null|        null|          a|            null|
# |ASN00015643|20100101|   TMIN|254.0|       null|        null|          a|            null|
# |ASN00015643|20100101|   PRCP|  0.0|       null|        null|          a|            null|
# |US1MSHD0003|20100101|   PRCP| 41.0|       null|        null|          N|            null|
# |US1MODG0003|20100101|   PRCP|  0.0|       null|        null|          N|            null|
# |US1MODG0003|20100101|   SNOW|  0.0|       null|        null|          N|            null|
# |US1MODG0003|20100101|   SNWD|  0.0|       null|        null|          N|            null|
# |US1MODG0003|20100101|   WESD|  0.0|       null|        null|          N|            null|
# |US1MODG0003|20100101|   WESF|  0.0|       null|        null|          N|            null|
# |US1MOCW0004|20100101|   PRCP|  0.0|       null|        null|          N|            null|
# |US1MOCW0004|20100101|   SNOW|  0.0|       null|        null|          N|            null|
# |ASN00085296|20100101|   TMAX|277.0|       null|        null|          a|            null|
# |ASN00085296|20100101|   TMIN|170.0|       null|        null|          a|            null|
# |ASN00085296|20100101|   PRCP| 58.0|       null|        null|          a|            null|
# |ASN00085280|20100101|   TMAX|256.0|       null|        null|          a|            null|
# |ASN00085280|20100101|   TMIN|186.0|       null|        null|          a|            null|
# |ASN00085280|20100101|   PRCP| 16.0|       null|        null|          a|            null|
# |ASN00040209|20100101|   TMAX|283.0|       null|        null|          a|            null|
# |ASN00040209|20100101|   TMIN|224.0|       null|        null|          a|            null|
# |ASN00040209|20100101|   PRCP| 14.0|       null|        null|          a|            null|
# +-----------+--------+-------+-----+-----------+------------+-----------+----------------+
# only showing top 20 rows



###### Question 4 (a) #######

daily_all = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", False)
    .option("inferSchema", False)
    .schema(schema_Daily )
    .load("hdfs:///data/ghcnd/daily/")
)
daily_all.count()
# Out[85]: 2624027105


###### Question 4 (b) #######

daily_all.filter(F.col('ELEMENT').isin('PRCP','SNOW','SNWD','TMAX','TMIN')).count()

#Out[11]: 2225133835



daily_all.filter(F.col('ELEMENT').isin('PRCP','SNOW','SNWD','TMAX','TMIN')).groupby('ELEMENT').agg({'ELEMENT':'count'}).show()
# +-------+--------------+
# |ELEMENT|count(ELEMENT)|
# +-------+--------------+
# |   SNOW|     322003304|
# |   SNWD|     261455306|
# |   PRCP|     918490401|
# |   TMIN|     360656728|
# |   TMAX|     362528096|
# +-------+--------------+



###### Question 4 (c) #######

##TMAX TMIN
TMAX_dairy = daily_all.filter(F.col('ELEMENT') == 'TMAX')
TMIN_dairy = daily_all.filter(F.col('ELEMENT') == 'TMIN')
difference = TMIN_dairy.select('ID','DATE').subtract(TMIN_dairy.select('ID','DATE'))
difference.count()
difference.select('ID').distinct().count() # 26625 stations

##Three observations
station_three_observations = stations_inventory.filter((F.col('HCN/CRN FLAG') != '')|(F.col('GSN FLAG') != ''))
difference.select('ID').intersect(station_three_observations.select('STATION_ID')).count() ### 2111


###### Question 4 (d) #######

dailys_TT = daily_all.filter(
        (F.substring(
            F.col('ID'), 1, 2) == 'NZ')&
             ((F.col('ELEMENT') == 'TMAX')|
                    (F.col('ELEMENT') == 'TMIN')))

dailys_TT.count() ##447017
dailys_TT.sort('ID', 'DATE').write.csv("hdfs:///user/gtj13/outputs/ghcnd/dailys_TT")


dailys_TT_AV = daily_all.groupBy(
                    'DATE', 'ELEMENT').agg(F.avg('VALUE').alias('AVERAGE'))

dailys_TT_AV.show()


dailys_TT_AV.sort('DATE').write.csv("hdfs:///user/gtj13/outputs/ghcnd/dailys_TT_AV")
year_code = F.substring(F.col('DATE'), 1, 4)
dailys_TT_AV = dailys_TT.withColumn('YEAR', year_code)
dailys_TT_AV.select('YEAR').distinct().count()

hdfs dfs -copyToLocal hdfs:///user/gtj13/outputs/ghcnd/dailys_TT_AV/ /users/home/gtj13

wc -l `find /users/home/gtj13/dailys_TT_AV/*.csv -type f`

###### Question 4 (e) #######

rainfall = daily_all.filter(F.col('element') == 'PRCP')

rainfall = (rainfall
    .select('ID', 'DATE', 'ELEMENT', 'VALUE')
    .withColumn('country_code', F.substring(F.col('DATE'), 1, 4))
    .withColumn('year',  F.substring(F.col('ID'), 1, 2))
    )

## grouping
rainfall = (rainfall.groupby('country_code', 'year').agg({'value':'mean'}))


rainfall.coalesce(1).write.format('com.databricks.spark.csv').options(delimiter = ',').save('hdfs:///user/gtj13/outputs/ghcnd/rainfall/', mode = 'overwrite', header = True)

# dbutils.fs.cp('hdfs:///user/gtj13/outputs/ghcnd/rainfall/, 'hdfs:///user/gtj13/outputs/ghcnd/rainfall/')

## 
hdfs dfs -copyToLocal hdfs:///user/gtj13/outputs/ghcnd/rainfall/ /users/home/gtj13




















