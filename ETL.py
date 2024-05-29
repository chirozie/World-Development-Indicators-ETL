# importing libraries
import pandas as pd
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

# Extraction/Ingestion
cpi_df_others = pd.read_excel('./source_data/Corruption Perception Index Data Set.xlsx', sheet_name='CPI Timeseries 2012 - 2020', header=2)
other_indicators_df = pd.read_csv('./source_data/WDICSV.csv')
country_df = pd.read_csv('./source_data/WDICountry.csv')


# preliminary cleaning/transformation for CPI data 2020 and historical

# Transformation to unpivot the CPI data
# Melt the DataFrame to long format
df_melted = pd.melt(cpi_df_others, id_vars=['Country', 'ISO3', 'Region'], value_vars=cpi_df_others.columns[3:], 
                    var_name='measure', value_name='score')

## formatting the different column CPI Score column names (now in measure column) to maintain constant formatting
df_melted['measure'] = df_melted['measure'].str.capitalize()

## split the measure column to measure type and year
df_melted[['measure_type', 'year']] = df_melted['measure'].str.rsplit(' ', n=1, expand=True)

## pivoting the dataframe
df_wide = df_melted.pivot(index=['Country', 'ISO3', 'Region', 'year'], columns='measure_type', values='score').reset_index()
cpi_df = df_wide.drop(['Sources', 'Standard error'], axis = 1)


## make a column names lower case
cpi_df.columns = cpi_df.columns.str.lower()
cpi_df.to_csv('cpi.csv', index=False)


# Preliminary cleaning and transformation for other indicators data 

## unpivot the columns
other_indicators_melted = \
                pd.melt(other_indicators_df, id_vars=['Country Name', 'Country Code', 'Indicator Name', 'Indicator Code'], 
                value_vars=other_indicators_df.columns[4:], var_name='year', value_name='score')
    

other_indicators = other_indicators_melted
other_indicators.columns = other_indicators.columns.str.lower().str.replace(" ", "_")
other_indicators.rename(columns={'country_name':'country', 'country_code':'iso3'}, inplace=True)

# drop rows (countries - year) with missing indicator scores
other_indicators = other_indicators.dropna(subset=['score'])
other_indicators['score'] = round(other_indicators['score'], 2)
other_indicators.to_csv('other_indicators.csv', index=False)

## Preliminary transformation for country dataset
country_df = country_df[['Country Code', 'Short Name', 'Region']]
country_df.columns = country_df.columns.str.lower().str.replace(' ', '_')
country_df.rename(columns={'short_name': 'name'})
country_df


## Loading data to staging schema

# Snowflake connection
## load environment variabbles (for credentials)
load_dotenv('.env')
engine = create_engine(
    'snowflake://{user}:{password}@{account_identifier}/{database}/{schema}?warehouse_name={warehouse}'.format(
        user = os.getenv('user'),
        password = os.getenv('password'),
        account_identifier = os.getenv('account_identifier'),
        database = os.getenv('database'),
        schema = os.getenv('schema'),
        warehouse = os.getenv('warehouse')
    )
)

#loading country data to staging table
country_df.to_sql('staging_country', con=engine, if_exists='append', index=False)

#loading CPI data to staging table
cpi_df.to_sql('staging_cpi', con=engine, if_exists='append', index=False)

## loading other indicators data to staging schema table (batch loading)
chunk_size = 100000  # Adjust chunk size based on your system's memory capacity
for chunk in pd.read_csv('other_indicators.csv', chunksize=chunk_size):
    chunk.to_sql('staging_other_indicators', con=engine, if_exists='append', index=False)
print('all data loaded')


## Execute database package to transform and load to EDW/Prod Schema
Session = sessionmaker(bind=engine)
session = Session()
session.execute('CALL STG.prc_WDI_load_all()')