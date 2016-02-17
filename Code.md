
# 1 Load and transform data

## 1.1 Load spark-csv package from Databricks

**Note:** Run this before any spark command in the notebook. Restart interpreter if necessary!



```scala
%dep
z.reset()
z.addRepo("Spark Packages Repo").url("http://dl.bintray.com/spark-packages/maven")
z.load("com.databricks:spark-csv_2.10:1.3.0")

```


Calling sc will initialize the executors (org.apache.spark.executor.CoarseGrainedExecutorBackend) via yarn, if Zeppelin is configured as "yarn-client"


```python
%pyspark

from __future__ import print_function

print(sc.version)
```


## 1.2 Load data from csv

Data: https://www.kaggle.com/worldbank/world-development-indicators/downloads/world-development-indicators-release-2016-01-28-06-31-53.zip


```bash
%sh

hdfs dfs -ls /tmp/world-development-indicators
```


```python
%pyspark
from pyspark.sql.types import *

def loadCsv(table, schema):
    data = sqlContext.read.load('/tmp/world-development-indicators/' + table + '.csv', 
                               format='com.databricks.spark.csv', 
                               header='true', 
                               schema=schema).cache()
    sqlContext.registerDataFrameAsTable(data, table+"RDD")
    return data

schema = StructType([ \
   StructField("CountryName",   StringType(),  True), \
   StructField("CountryCode",   StringType(),  True), \
   StructField("IndicatorName", StringType(),  True), \
   StructField("IndicatorCode", StringType(),  True), \
   StructField("Year",          IntegerType(), True), \
   StructField("Value",         DoubleType(),  True)  \
])
indicators_csv = loadCsv("Indicators", schema)

print(indicators_csv.count())
```


Let's look at the schema of the Indicators table


```python
%pyspark
indicators_csv.printSchema()
```


Code/value encoding is not that optimal ... Let's transform the data set and store the result os ORC



## 1.3 Transform Indicators table to Columns 



Spark 1.5 does not provide a `pivot` method for DataFrames, hence we need to write our own pivot via RDDs and `aggregateByKey`

Some caveats for this step:
- Return a row from `merge`, python dictionaries are deprecated
- `**value` is a nice trick to convert a dictionary to a keyword parameter list (Rows are unmutable)
- Initialize with all indicators and set them to None
- `.`are not allowed in column names, so replace with `_`


```python
%pyspark

columns = indicators_csv.map(lambda row: row.IndicatorCode.replace(".", "_")).distinct().collect()
bc = sc.broadcast(columns)

def seq(u, v):
    if u == None: 
        u = {ind: None for ind in bc.value}          # Use value of broadcast variable to initialize 
                                                     # the dictionary and ensure all rows have all indicators
    u[v.IndicatorCode.replace(".","_")] = v.Value    # Set this indicators value converted to float
    return u

def comb(u1, u2):
    u1.update(u2)
    return u1

def merge(keys, value):
    value["Country"] = keys[0]
    value["Year"] = int(keys[1])
    return Row(**value)

data = indicators_csv.select(["CountryCode", "IndicatorCode", "Year", "Value"])\
                     .rdd\
                     .keyBy(lambda row: row.CountryCode + "|" + str(row.Year))\
                     .aggregateByKey(None, seq, comb)\
                     .map(lambda tuple: merge(tuple[0].split("|"), tuple[1]))\
                     .cache()


```


Finally, transform RDD back to DataFrame and register a table with the hiveContext (due to ORC)

**Notes:**

- The StructType schema **has to be sorted!** Spark does not match schema names with Row column names but uses the order of elements in Row and schema to apply types
- Also, due to the many null values, automatic schema inference will only work properly when "samplingRatio=100" in createDataFrame. However, I wouldn't rely on it ...


```python
%pyspark

from pyspark.sql.types import *

from pyspark.sql import HiveContext
hiveContext = HiveContext(sc)
hiveContext.setConf("spark.sql.orc.filterPushdown", "true")

fields = [StructField(ind, StringType(), True) for ind in columns ] + \
         [StructField("Year", IntegerType(), False), StructField("Country", StringType(), False)]
sortedFields = sorted(fields, key=lambda x: x.name)
sortedSchema = StructType(fields=sortedFields)

indicators = sqlContext.createDataFrame(data, schema = sortedSchema)
sqlContext.registerDataFrameAsTable(indicators, "Indicators")
```


```python
%pyspark

print indicators.first()


```


## 1.4 Save transformed table as ORC


```bash
%sh
hdfs dfs -rm -r /tmp/indicators_transformed_orc

```


```python
%pyspark

indicators.write.orc("/tmp/indicators_transformed_orc")

```


```bash
%sh
hdfs dfs -rm -r /tmp/indicators_countries_orc
hdfs dfs -rm -r /tmp/indicators_descriptions_orc
```


```python
%pyspark
countries = indicators_csv.map(
                lambda row: Row(Code=row.CountryCode, Name=row.CountryName)
            ).distinct()
hiveContext.createDataFrame(countries).write.orc("/tmp/indicators_countries_orc")

descriptions = indicators_csv.map(
                  lambda row: Row(Code=row.IndicatorCode, Name=row.IndicatorName)
               ).distinct()
hiveContext.createDataFrame(descriptions).write.orc("/tmp/indicators_descriptions_orc")

print countries.count(), descriptions.count()

```


# 2 Data Analysis

## 2.1 Load ORC data again to benefit from predicate pushdow, etc


```python
%pyspark

indicators_t = hiveContext.read.orc("/tmp/indicators_transformed_orc")
sqlContext.registerDataFrameAsTable(indicators_t, "Indicators_t")
sqlContext.cacheTable("Indicators_t")

```


## 2.2 Some simple Queries


```sql
%sql

-- SP.DYN.CBRT.IN: Birth rate, crude (per 1,000 people)

select Country, Year, SP_DYN_CBRT_IN from Indicators_t
where Country in ('AUT', 'FRA', 'DEU', 'GRC', 'IRL', 'ITA', 'NLD', 'PRT', 'ESP', 'GBR')
  and Year > 1990

```


```sql
%sql

-- SL.UEM.1524.NE.ZS: Unemployment, youth total (% of total labor force ages 15-24) (national estimate)

select Country, Year, SL_UEM_1524_NE_ZS from Indicators_t
where Country in ('AUT', 'FRA', 'DEU', 'GRC', 'IRL', 'ITA', 'NLD', 'PRT', 'ESP', 'GBR') 
  and Year > 1990


```


```sql
%sql

-- SL.UEM.1524.NE.ZS: Unemployment, youth total (% of total labor force ages 15-24) (national estimate)
-- SL.UEM.TOTL.NE.ZS: Unemployment, total (% of total labor force) (national estimate)
-- SP.DYN.CBRT.IN: Birth rate, crude (per 1,000 people)

select Country, Year, SL_UEM_1524_NE_ZS, SP_DYN_CBRT_IN  from Indicators_t
where Country in ('AUT', 'FRA', 'DEU', 'GRC', 'IRL', 'ITA', 'NLD', 'PRT', 'ESP', 'GBR') 
  and Year > 1990

```


## Pure SQL approach

Of course, this result could have been calculated without pivoting the table


```python
%pyspark

sqlContext.registerDataFrameAsTable(indicators_csv, "Indicators")
```


```sql
%sql

select Year, CountryCode, max(SL) as UNEM, max(SP) as CBRT from
  (select Year, CountryCode, 
          case IndicatorCode when 'SP.DYN.CBRT.IN'  then max(Value) else NULL end as SP,
          case IndicatorCode when 'SL.UEM.1524.NE.ZS' then max(Value) else NULL end as SL
   from Indicators
   where IndicatorCode in ('SP.DYN.CBRT.IN', 'SL.UEM.1524.NE.ZS') 
     and CountryCode in ('AUT', 'FRA', 'DEU', 'GRC', 'IRL', 'ITA', 'NLD', 'PRT', 'ESP', 'GBR') 
     and year > 1990
   group by Year, CountryCode, IndicatorCode
   order by Year, CountryCode) a
group by Year, CountryCode
```


## Little Indicator helper


```python
%pyspark
for k,v in dict(sqlContext.sql("""
    select distinct IndicatorCode, IndicatorName from IndicatorsRDD
    where IndicatorName like "%nemploym%"
""").map(lambda row: list(row)).collect()).iteritems():

    print "%50s: %s" % (k,v)
```

