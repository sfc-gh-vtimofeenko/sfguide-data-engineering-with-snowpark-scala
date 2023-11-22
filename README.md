# Data Engineering Pipelines with Snowpark Java
This repository contains the code for the *Data Engineering Pipelines with Snowpark Java* Snowflake Quickstart.

### ➡️ NOTE: This quickstart has not been published yet to https://quickstarts.snowflake.com.

___
Here is an overview of what we'll build in this lab:

<img src="images/demo_overview.png" width=800px>

## Requirements

- Java 11
- Maven

## Configure Snowflake connection properties
Create a `snowflake.properties` file in the project root folder. This will be required to create a local Java Snowpark session as well as to deploy Snowpark applications to Snowflake using the Snowflake Maven Plugin.  

An example has been provided in `snowflake.properties.example`.

## Lab Steps (Part 1)
- Follow the SQL instructions in `sql/01_setup_snowflake.sql` to prepare your Snowflake account
- Run `Step02_LoadRawData` Java program from local machine
- Follow the SQL instructions in `sql/03_load_weather.sql` to configure the Frostbyte weather data share
- Run `Step04_CreatePOSView` Java program from local machine
- Review Java code in `Step05_FarenheitToCelsiusUDF`
- Review Java code in `Step06_UpdateOrdersProcedure`
- Review Java code in `Step07_UpdateDailyCityMetricsProcedure`

## Deploy UDFs and stored procedures

```
mvn clean package snowflake:deploy
```

## Lab Steps (Part 2)
- Follow the SQL instructions in `sql/08_orchestrate_jobs.sql` to configure and execute tasks for Java stored procedures
- Follow the SQL instructions in `sql/09_process_incrementally.sql` to load additional data for incremental processing
- Follow the SQL instructions in `sql/11_teardown.sql` to remove assets related to this lab from your Snowflake account.



## Advanced

### View JAR dependencies to be uploaded

```
mvn dependency:list -DincludeScope=compile
```