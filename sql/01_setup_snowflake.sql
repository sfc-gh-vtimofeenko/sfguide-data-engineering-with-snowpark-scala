/*-----------------------------------------------------------------------------
Hands-On Lab: Data Engineering with Snowpark Scala
Script:       01_setup_snowflake.sql
Author:       Jeremiah Hansen, Keith Gaputis, Vladimir Timofeenko
Last Updated: 11/27/2023
-----------------------------------------------------------------------------*/

-- ----------------------------------------------------------------------------
-- Step #1: Create the account level objects
-- ----------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

-- Roles
SET MY_USER = CURRENT_USER();
CREATE OR REPLACE ROLE SCALA_DE_ROLE;
GRANT ROLE SCALA_DE_ROLE TO ROLE SYSADMIN;
GRANT ROLE SCALA_DE_ROLE TO USER IDENTIFIER($MY_USER);

GRANT EXECUTE TASK ON ACCOUNT TO ROLE SCALA_DE_ROLE;
GRANT MONITOR EXECUTION ON ACCOUNT TO ROLE SCALA_DE_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE SCALA_DE_ROLE;

-- Databases
CREATE OR REPLACE DATABASE SCALA_DE_DB;
GRANT OWNERSHIP ON DATABASE SCALA_DE_DB TO ROLE SCALA_DE_ROLE;

-- Warehouses
CREATE OR REPLACE WAREHOUSE SCALA_DE_WH WAREHOUSE_SIZE = XSMALL, AUTO_SUSPEND = 300, AUTO_RESUME= TRUE;
GRANT OWNERSHIP ON WAREHOUSE SCALA_DE_WH TO ROLE SCALA_DE_ROLE;


-- ----------------------------------------------------------------------------
-- Step #2: Create the database level objects
-- ----------------------------------------------------------------------------
USE ROLE SCALA_DE_ROLE;
USE WAREHOUSE SCALA_DE_WH;
USE DATABASE SCALA_DE_DB;

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
