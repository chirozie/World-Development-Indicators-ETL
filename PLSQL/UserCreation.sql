-- create role for data analyst
CREATE ROLE analyst_role;

-- Grant usage on the data warehose
GRANT USAGE ON WAREHOUSE WDI TO ROLE analyst_role;

-- Grant usage on the database
GRANT USAGE ON DATABASE WDI TO ROLE analyst_role;

-- Grant usage on the schema
GRANT USAGE ON SCHEMA WDI.EDW TO ROLE analyst_role;

-- Grant select privilege on all tables in the schema
GRANT SELECT ON ALL TABLES IN SCHEMA WDI.EDW TO ROLE analyst_role;

-- future tables that might be created in the schema
GRANT SELECT ON FUTURE TABLES IN SCHEMA WDI.EDW TO ROLE analyst_role;


--create user for analytst_role
CREATE USER analyst_user
  PASSWORD = '*******'
  DEFAULT_ROLE = analyst_role
  DEFAULT_WAREHOUSE = WDI
  DEFAULT_NAMESPACE = WDI.EDW
  MUST_CHANGE_PASSWORD = FALSE;

-- Assign the role to the user
GRANT ROLE analyst_role TO USER analyst_user;
