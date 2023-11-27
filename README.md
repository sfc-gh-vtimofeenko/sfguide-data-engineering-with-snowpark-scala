# Data Engineering Pipelines with Snowpark Scala

This repository contains the code for the *Data Engineering Pipelines with Snowpark Scala* Snowflake Quickstart.

### ➡️ NOTE: This quickstart has not been published yet to https://quickstarts.snowflake.com.

___
Here is an overview of what we'll build in this lab:

<img src="images/demo_overview.png" width=800px>

## Requirements

- Java 11
- Scala 2.13.12
- SBT 1.9.7
- (optional) scalafmt
- (optional) snowcli

## Configure Snowflake connection properties
Create a `snowflake.properties` file in the project root folder. This will be required to create a local Scala Snowpark session as well as to deploy Snowpark applications to Snowflake using the Snowflake Maven Plugin.  

An example has been provided in `snowflake.properties.example`.

## Lab Steps (Part 1)
1. Follow the SQL instructions in `sql/01_setup_snowflake.sql` to prepare your Snowflake account for the lab.
2. Run `Step02_LoadRawData` Java program (from your local machine) to ingest raw data from S3 into Snowflake.
3. Follow the SQL instructions in `sql/03_load_weather.sql` to configure the Frostbyte weather data share in your Snowflake account.
4. Run `Step04_CreatePOSView` Java program (from your local machine) to create a flattened view (and corresponding stream object) for POS data.
5. Review Java code in `Step05_FarenheitToCelsiusUDF`. This contains a Java UDF that will be used in Step 7.
6. Review Java code in `Step06_UpdateOrdersProcedure`.  This contains a Java stored procedure that will merge data from the flattened POS view into the `orders` table.
7. Review Java code in `Step07_UpdateDailyCityMetricsProcedure`. This contains a Java stored procedure that will perform various aggregation and transformations to prepare the `daily_city_metrics` table in the `analytics` layer.

## Deploy UDFs and stored procedures

```
mvn clean package snowflake:deploy
```

## Lab Steps (Part 2)
8. Follow the SQL instructions in `sql/08_orchestrate_jobs.sql` to configure and execute tasks that will trigger the Java stored procedures.  These procedures will populate the `orders` and `daily_city_metrics` tables.
9. Follow the SQL instructions in `sql/09_process_incrementally.sql` to load additional data for incremental processing
10. Follow the SQL instructions in `sql/10_teardown.sql` to remove assets related to this lab from your Snowflake account.

## Advanced

### View JAR dependencies to be uploaded

```
mvn dependency:list -DincludeScope=compile
```
