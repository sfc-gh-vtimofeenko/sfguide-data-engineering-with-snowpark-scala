/*-----------------------------------------------------------------------------
Hands-On Lab: Data Engineering with Snowpark Java
Script:       10_teardown.sql
Author:       Jeremiah Hansen, Keith Gaputis, Vladimir Timofeenko
Last Updated: 11/29/2023
-----------------------------------------------------------------------------*/


USE ROLE ACCOUNTADMIN;

DROP DATABASE SCALA_DE_DB;
DROP WAREHOUSE SCALA_DE_WH;
DROP ROLE SCALA_DE_ROLE;

-- Drop the weather share
DROP DATABASE FROSTBYTE_WEATHERSOURCE;
