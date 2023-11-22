/*-----------------------------------------------------------------------------
Hands-On Lab: Data Engineering with Snowpark Java
Script:       01_setup_snowflake.sql
Author:       Jeremiah Hansen, Keith Gaputis
Last Updated: 11/6/2023
-----------------------------------------------------------------------------*/

-- ----------------------------------------------------------------------------
-- Step #1: Create the account level objects
-- ----------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

-- Roles
SET MY_USER = CURRENT_USER();
CREATE OR REPLACE ROLE JAVA_DE_ROLE;
GRANT ROLE JAVA_DE_ROLE TO ROLE SYSADMIN;
GRANT ROLE JAVA_DE_ROLE TO USER IDENTIFIER($MY_USER);

GRANT EXECUTE TASK ON ACCOUNT TO ROLE JAVA_DE_ROLE;
GRANT MONITOR EXECUTION ON ACCOUNT TO ROLE JAVA_DE_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE JAVA_DE_ROLE;

-- Databases
CREATE OR REPLACE DATABASE JAVA_DE_DB;
GRANT OWNERSHIP ON DATABASE JAVA_DE_DB TO ROLE JAVA_DE_ROLE;

-- Warehouses
CREATE OR REPLACE WAREHOUSE JAVA_DE_WH WAREHOUSE_SIZE = XSMALL, AUTO_SUSPEND = 300, AUTO_RESUME= TRUE;
GRANT OWNERSHIP ON WAREHOUSE JAVA_DE_WH TO ROLE JAVA_DE_ROLE;


-- ----------------------------------------------------------------------------
-- Step #2: Create the database level objects
-- ----------------------------------------------------------------------------
USE ROLE JAVA_DE_ROLE;
USE WAREHOUSE JAVA_DE_WH;
USE DATABASE JAVA_DE_DB;

-- Schemas
CREATE OR REPLACE SCHEMA EXTERNAL;
CREATE OR REPLACE SCHEMA RAW_POS;
CREATE OR REPLACE SCHEMA RAW_CUSTOMER;
CREATE OR REPLACE SCHEMA HARMONIZED;
CREATE OR REPLACE SCHEMA ANALYTICS;

-- External Frostbyte objects
USE SCHEMA EXTERNAL;
CREATE OR REPLACE FILE FORMAT PARQUET_FORMAT
    TYPE = PARQUET
    COMPRESSION = SNAPPY
;
CREATE OR REPLACE STAGE FROSTBYTE_RAW_STAGE
    URL = 's3://sfquickstarts/data-engineering-with-snowpark-python/'
;

-- ANALYTICS objects
USE SCHEMA HARMONIZED;
-- This will be added in step 5
--CREATE OR REPLACE FUNCTION HARMONIZED.FAHRENHEIT_TO_CELSIUS_UDF(TEMP_F NUMBER(35,4))
--RETURNS NUMBER(35,4)
--AS
--$$
--    (temp_f - 32) * (5/9)
--$$;

CREATE OR REPLACE FUNCTION HARMONIZED.INCH_TO_MILLIMETER_UDF(INCH NUMBER(35,4))
RETURNS NUMBER(35,4)
    AS
$$
    inch * 25.4
$$;
