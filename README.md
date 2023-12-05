# Data Engineering Pipelines with Snowpark Scala

This repository contains the code for the *Data Engineering Pipelines with Snowpark Scala* Snowflake Quickstart.

### ➡️ NOTE: This quickstart has not been published yet to https://quickstarts.snowflake.com.

___
Here is an overview of what we'll build in this lab:

<img src="images/demo_overview.png" width=800px>

## Setup

The following is required:

- A Snowflake account
- Packages on local machine:
    - Java 11
    - Scala 2.12.18
    - SBT 1.9.7
    - Maven 

<details>
 <summary>Packages setup</summary>
 
 Use your package manager of choice to install the aforementioned packages or if Nix and direnv are installed, run `direnv allow` which will pull in all the needed packages and set up the environment.

 *Note*: Nix/direnv combination is entirely optional. It does greatly simplify the packages installation.

 If using Nix, make sure to enable the "flakes" experimental feature.
</details>

<details>
 <summary>Authentication setup</summary>
 
 This repository is intended to work with either `snowflake.properties` (see [example](./snowflake.properties.example)) or with environment variables that can be retrieved from `snowsql` configuration by .envrc (see `.envrc`) file.

 Properties file takes precedence.
 
 One important environment variable to consider is `export QUICKSTART_RUN_LOCALLY=TRUE` which will allow you to run the procedures locally.
</details>

## Overview

### Lab Steps (Part 1)
1. Follow the SQL instructions in `sql/01_setup_snowflake.sql` to prepare your Snowflake account for the lab.
2. Run `Step02_LoadRawData` Scala program (from your local machine) to ingest raw data from S3 into Snowflake:

    ```shell
    sbt "runMain com.snowflake.examples.de.Step02_LoadRawData"
    ```
    
3. Follow the SQL instructions in `sql/03_load_weather.sql` to configure the Frostbyte weather data share in your Snowflake account.
4. Run `Step04_CreatePOSView` Scala program (from your local machine) to create a flattened view (and corresponding stream object) for POS data:

    ```shell
    sbt "runMain com.snowflake.examples.de.Step04_CreatePOSView"
    ```

5. Review Scala code in `Step05_FarenheitToCelsiusUDF`. This contains a UDF that will be used in Step 7.
6. Review Scala code in [`Step06_UpdateOrdersProcedure`](./src/main/scala/com/snowflake/examples/de/Step06_UpdateOrdersProcedure.scala).  This file contains a Scala stored procedure that will merge data from the flattened POS view into the `orders` table.
7. Review Scala code in [`Step07_UpdateDailyCityMetricsProcedure`](./src/main/scala/com/snowflake/examples/de/Step07_UpdateDailyCityMetricsProcedure.scala). This file contains a Scala stored procedure that will perform various aggregation and transformations to prepare the `daily_city_metrics` table in the `analytics` layer.

### Deploy UDFs and stored procedures

```shell
mvn clean package snowflake:deploy
```

### Lab Steps (Part 2)
8. Follow the SQL instructions in `sql/08_orchestrate_jobs.sql` to configure and execute tasks that will trigger the Scala stored procedures.  These procedures will populate the `orders` and `daily_city_metrics` tables.
9. Follow the SQL instructions in `sql/09_process_incrementally.sql` to load additional data for incremental processing
10. Follow the SQL instructions in `sql/10_teardown.sql` to remove assets related to this lab from your Snowflake account.

## Advanced

### View JAR dependencies to be uploaded

```
mvn dependency:list -DincludeScope=compile
```


## See also

- [Snowpark Scala template](https://github.com/Snowflake-Labs/snowpark-scala-template/)
