Given the [World Development Indicator Data](https://www.kaggle.com/worldbank/world-development-indicators/downloads/world-development-indicators-release-2016-01-28-06-31-53.zip) from [Kaggle public data](https://www.kaggle.com/worldbank/world-development-indicators)

this [Apache Zeppelin](https://zeppelin.incubator.apache.org/) notebook tries to solve the following tasks:

- Load the data from HDFS (csv with commas in quotes)
- Trasform the data from stacked to unstacked, i.e. Indicators keys as columns instead of rows
- Store data as [ORC](https://orc.apache.org/)
- Relaod data from [ORC](https://orc.apache.org/) and do some simple queries
- Come up with a SQL solution for the same result using the unstacked (original) data

An Markdown copy of the Zeppelin Notebook can be found in [Code.md](Code.md)
