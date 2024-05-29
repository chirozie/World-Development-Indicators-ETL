USE DATABASE WDI;

-- procedures to load dimension tables
CREATE OR REPLACE PROCEDURE STG.prc_load_WDI_country()
RETURNS STRING
AS
$$
BEGIN
    BEGIN
        -- Main procedure logic
        INSERT INTO EDW.dimCountry (country_code, name, region)
        SELECT DISTINCT a.country_code,
                        a.name,
                        NVL(b.region, a.region) AS region
        FROM STG.staging_country a
        LEFT JOIN STG.staging_cpi b
            ON a.country_code = b.iso3;

        -- Log the success status
        INSERT INTO STG.log_table (procedure_name, status, error_message, log_timestamp)
        VALUES ('STG.prc_load_WDI_country', 'Success', NULL, CURRENT_TIMESTAMP);

        RETURN 'Success';
    EXCEPTION
        WHEN OTHER THEN
            -- If an error occurs, set status to 'Failure' and capture the error message
           RETURN OBJECT_CONSTRUCT('SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);

            -- Log the failure status
            INSERT INTO STG.log_table (procedure_name, status, error_message, log_timestamp)
            VALUES ('STG.prc_load_WDI_country', 'sqlstate', sqlerrm, CURRENT_TIMESTAMP);
    END;
END;
$$;



CREATE OR REPLACE PROCEDURE STG.prc_load_indicators()
RETURNS STRING
AS
$$
BEGIN
    BEGIN
        -- Main procedure logic
        INSERT INTO EDW.dimIndicator (indicator_code, indicator_name)
        SELECT DISTINCT indicator_code,
                        indicator_name,
        FROM STG.staging_other_indicators
        UNION
        -- assuming CPI as the code for corruption perc. index
        SELECT 'CPI' indicator_code,      
                'Corruption Perception Index' indicator_name
        FROM DUAL;

        -- Log the success status
        INSERT INTO STG.log_table (procedure_name, status, error_message, log_timestamp)
        VALUES ('STG.prc_load_indicators', 'Success', NULL, CURRENT_TIMESTAMP);

        RETURN 'Success';
    EXCEPTION
        WHEN OTHER THEN
            -- If an error occurs, set status to 'Failure' and capture the error message
           RETURN OBJECT_CONSTRUCT('SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);

            -- Log the failure status
            INSERT INTO STG.log_table (procedure_name, status, error_message, log_timestamp)
            VALUES ('STG.prc_load_indicators', 'sqlstate', sqlerrm, CURRENT_TIMESTAMP);
    END;
END;
$$;


-- procedure to load fact table
CREATE OR REPLACE PROCEDURE STG.prc_load_indicator_values()
RETURNS STRING
AS
$$
BEGIN
    BEGIN
        -- Main procedure logic
        INSERT INTO EDW.factIndicatorValues (indicator_code, year, country_code, score, rank)
        SELECT DISTINCT indicator_code, CAST(year AS INTEGER),
                        iso3 country_code,
                        score,
                        rank() over(PARTITION BY indicator_code, year ORDER BY score desc) AS rank
        FROM STG.staging_other_indicators
        UNION
        SELECT 'CPI' indicator_code, CAST(year AS INTEGER), 
                iso3 country_code,
                "cpi score" score,
                rank() over(PARTITION BY year ORDER BY "cpi score" desc) AS rank
        FROM STG.staging_cpi;

        -- Log the success status
        INSERT INTO STG.log_table (procedure_name, status, error_message, log_timestamp)
        VALUES ('STG.prc_load_indicator_values', 'Success', NULL, CURRENT_TIMESTAMP);

        RETURN 'Success';
    EXCEPTION
        WHEN OTHER THEN
            -- If an error occurs, set status to 'Failure' and capture the error message
           RETURN OBJECT_CONSTRUCT('SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);

            -- Log the failure status
            INSERT INTO STG.log_table (procedure_name, status, error_message, log_timestamp)
            VALUES ('STG.prc_load_indicator_values', 'sqlstate', sqlerrm, CURRENT_TIMESTAMP);
    END;
END;
$$;


CREATE OR REPLACE PROCEDURE STG.prc_WDI_load_all()
RETURNS STRING
AS
$$
BEGIN
    BEGIN
        -- Main procedure logic
        CALL STG.prc_load_WDI_country();
        CALL STG.prc_load_indicators();
        CALL STG.prc_load_indicator_values();

        -- Log the success status
        INSERT INTO STG.log_table (procedure_name, status, error_message, log_timestamp)
        VALUES ('STG.prc_WDI_load_all', 'Success', NULL, CURRENT_TIMESTAMP);

        RETURN 'Success';
    EXCEPTION
        WHEN OTHER THEN
            -- If an error occurs, set status to 'Failure' and capture the error message
           RETURN OBJECT_CONSTRUCT('SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);

            -- Log the failure status
            INSERT INTO STG.log_table (procedure_name, status, error_message, log_timestamp)
            VALUES ('STG.prc_WDI_load_all', 'sqlstate', sqlerrm, CURRENT_TIMESTAMP);
    END;
END;
$$;


