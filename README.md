# Data Warehouse for Climate Change Analytics

This project was built as the Capstone project of Udacity's Data Engineering Nanodegree. This readme contains the following sections:

1. Project Scope
2. Data Sources
3. Data Warehouse design
4. Project structure
5. Further discussion

## 1. Project Scope

The aim of the project is to build a **Data Warehouse** in Amazon Web Services to enable analytic insights about **Climate Change** and its relation with **Green House Gases** emissions and **Industrial Production**. The final schema available to users will iclude data from many different sources, including: industrial production by country and year, a historical series of emissions by country and year that goes back to the XIX century, a historical series of earth temperature and currencies exchange data.

With this objective in mind, for most of the data it makes no sense to go to a deeper level of aggregation than by country and year. Only for temperature and currencies data will be included another level of granularity, because doing the yearly average would imply some assumptions that shouldn't be done without thinking carefully. For example, maybe in some regions climate change is making weather more extreme, with colder winters and hotter summers, but the yearly average remains the same.

Finally, to get the insights, a **Dashboard** tool like Tableau or Power BI could be connected to the database. This tools also have options to plot data in a map of the globe that could produce very neat reports. An example of a question that could drive one of these reports is: *Which is the country that produces less emissions per dollar of revenue of a certain industry?*

## 2. Data Sources

[Climate Change: Earth Surface Temperature Data](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data)

[Annual CO2 emissions per country](https://ourworldindata.org/co2-and-other-greenhouse-gas-emissions#co2-in-the-atmosphere)

[STAN Industrial Analysis](https://stats.oecd.org/Index.aspx?DataSetCode=STANI4_2016)

[Foreign Exchange Rates 2000-2019](https://www.kaggle.com/brunotly/foreign-exchange-rates-per-dollar-20002019)

### Climate Change: Earth Surface Temperature Data

From this dataset we will use *GlobalLandTemperaturesByCountry.csv* as explained in the project scope. Data comes from the Berkeley Earth Surface Temperature Study which combines 1.6 billion temperature reports from 16 pre-existing archives.

Data goes from the year 1743 to the present, and includes:
- LandAverageTemperature: global average land temperature in celsius
- LandAverageTemperatureUncertainty: the 95% confidence interval around the average

### CO2 emissions

This dataset contains emission data of CO2 in tonnes of different countries from year 1751 - 2017. Data originally published by: *Le Quéré et al. (2018). Global Carbon Project; Carbon Dioxide Information Analysis Centre (CDIAC).*

### STAN Industrial Analysis

*The STAN database for industrial analysis provides analysts and researchers with a comprehensive tool for analysing industrial performance at a relatively detailed level of activity across countries. The selected dataset includes data of the monetary value produced by different industries at an aggregation level of country and year.*

This dataset will be the main source for our facts table and hopefully will provide new insights about wich countries have greener industries, which could lay the foundation of a further study about how to redesign our countries for the future.

### Foreign exchange rates

A dataset generated on the Federal Reserve's Download Data Program, with some changes. This table had to be preprocessed before the copy statement because there isn't a good implementation of unpivot tables in Redshift.

## 3. Data Warehouse design

The Data Warehouse will follow a **Kimball's Bus Architecture** with a **Redshift** database in **2nd Normal Form** for users and analytics applications with the following **Star Schema**:

![Database schema](images/schema.png)

There will be 4 staging tables that will ingest data with few transformations from the sources.

An etl on *etl.py* will load the data into the staging tables, transform and insert it on the final tables.

## 4. Project structure

There are 5 python files in this project:

- **sql.py**: this file contains the sql queries that will be imported by the other scripts.
- **create_tables.py**: a script that drops previous versions of the tables and then create them.
- **etl.py**: a script that executes the data extraction from S3, loads it onto staging tables and then transforms and inserts it onto the final tables.
- **data_quality.py**: a script that runs quality checks on the final tables.
- **get_redshift_logs.py**: a script that fetches the error logs from Redshift system tables.

To build the project, one has to complete the **dwh.cfg** file first and then run the python scripts in the following order:
**create_tables.py** >> **etl.py** >> **data_quality.py**

## 5. Further discussion

This initial project of a Data Warehouse could evolve in many different directions and some of these directions would require of structural changes and/or incorporation of new tools. In this sections we'll discuss some situations that could arise in the future and how to deal with them.

### The data was increased by 100x

It may happen that the amount of data increases by a huge factor. In this case we could transform the Data Warehouse into a Data Lake and use Spark to process the data. In Amazon Web Services we have EC2 and EMR to implement this solution efficient and painlessly, though the costs of these servers are far higher than those of Redshift or RDS.

With this amount of data, Redshift would still be a great option, but if we got to a point with so much data that Redshift couldn't handle it properly, we could migrate the Database to a distributed database like Cassandra.

### The pipelines would be run on a daily basis by 7 am every day

Another direction the project could take is one in which we need to update data daily, or in any regular pattern.

In this case, a tool like Airflow would come in hand. In Airflow we can create a data pipeline that will be executed on a desired schedule. Airflow is easy to configure, use and very flexible. Tasks are built as DAGs (directed acyclic graphs) and we have nice tools to inspect the current and historical executions.

Another option would be to schedule our pipeline in our Linux Server with Cron, but with this setup we have less traceability.

In any case, we would need also a data scrapper, which could be built with python too. In the case of exchange rates, data could be get from the [Federal Reserve](https://www.federalreserve.gov/releases/h10/Hist/default.htm), whose data is updated weekely.

### The database needed to be accessed by 100+ people

In a case were there were a lot of users accessing the same database, we would have to optimize it for reads. For this kind of situation there's not a suit-all solution, a part from increasing the number of clusters, which would also increase the maintenance costs of the system. A use study of the database to **optimize** the schema for **user needs** would be the best option, denormalizing when needed or creating indexes for the tables.

Redshift is a columnar database, which makes it better than many other options for dealing with this situation. Specifically on Redshift, sortkeys and distkeys should be set wisely to optimize query performance.