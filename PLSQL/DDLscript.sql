-- database creation
CREATE DATABASE IF NOT EXISTS WDI;

-- schema creation
CREATE SCHEMA IF NOT EXISTS STG;
CREATE SCHEMA IF NOT EXISTS EDW;

-- staging tables DDL
CREATE TABLE IF NOT EXISTS STG.staging_cpi(
    country varchar(100),
    iso3 char(3),
    region varchar(50),
    year integer,
    cpi_score float,
    rank float
);

drop table STG.staging_other_indicators
CREATE TABLE IF NOT EXISTS STG.staging_other_indicators(
    country varchar(100),
    iso3 char(3),
    indicator_name varchar(300),
    indicator_code varchar(100),
    year integer,
    score numeric
);

CREATE TABLE IF NOT EXISTS STG.staging_country(
    country_code varchar(3),
    name varchar(100),
    region varchar(100)
);

--log table for logging procedure run status
CREATE TABLE STG.log_table (
    procedure_name STRING,
    status STRING,
    error_message STRING,
    log_timestamp TIMESTAMP
);


-- EDW/Prod tables DDL
CREATE TABLE IF NOT EXISTS dimCountry(
    country_code varchar(3) PRIMARY KEY NOT NULL,
    name varchar(100),
    region varchar(50)
);


CREATE TABLE IF NOT EXISTS dimIndicator(
    indicator_code varchar(100) PRIMARY KEY NOT NULL,
    indicator_name text
);

CREATE TABLE IF NOT EXISTS factIndicatorValues(
    indicator_code varchar(100) NOT NULL,
    year integer,
    country_code varchar(3),
    score float,
    rank integer,
    PRIMARY KEY (indicator_code, year, country_code)
    
);

---partition (cluster) the indicator values fact table by year
ALTER TABLE EDW.factindicatorvalues
CLUSTER BY (year)