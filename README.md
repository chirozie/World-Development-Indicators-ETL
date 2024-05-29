# World-Development-Indicators ETL
Data Engineering ETL pipeline for WDI data to Snowflake data warehouse

## Introduction
Country macroeconomic analysis reports are used to guide top management in formulating sustainable 
strategies for the organization. The data collection of the macroeconomic indicators and report 
preparation being manual, the report is currently only being circulated on a quarterly basis. This has 
been highlighted by a recent Bank of Mauritius survey and the organization has been mandated to 
automate the report so that the organization is able to react faster in the current difficult economic 
conditions. You are requested to build an efficient data pipeline that can cater for all the current 
reporting requirements as well as any future ones.

## Data Sources
1. Corruption Perception Index Data Set (attached)
2. World Development Indicators. Historical data to be download from: https://datacatalog.worldbank.org/search/dataset/0037712/World-Development-Indicators

## Objectives
1. Implement a database with necessary tables, columns, data types and any constraints.
3. Develop the data pipeline to trigger the process that will consume the data from the different sources 
and load them in the tables created.

![Pipeline Architecture and Dimensional model](https://github.com/chirozie/World-Development-Indicators-ETL/blob/main/Data%20Model%20%26%20Pipeline%20Architecture.png)

## Note
To recreate the pipeline
1. Download the WDICSV.csv from the overview page the link in data source section
2. Create a .env file with credentials to your snowflake datawarehouse
