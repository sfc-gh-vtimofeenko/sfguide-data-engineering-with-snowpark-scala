/*-----------------------------------------------------------------------------
Hands-On Lab: Data Engineering with Snowpark Java
Script:       11_teardown.sql
Author:       Jeremiah Hansen, Keith Gaputis
Last Updated: 11/6/2023
-----------------------------------------------------------------------------*/


USE ROLE ACCOUNTADMIN;

DROP DATABASE JAVA_DE_DB;
DROP WAREHOUSE JAVA_DE_WH;
DROP ROLE JAVA_DE_ROLE;

-- Drop the weather share
DROP DATABASE FROSTBYTE_WEATHERSOURCE;
