# Data Engineering Pipelines with Snowpark Java
This repository contains the code for the *Data Engineering Pipelines with Snowpark Java* Snowflake Quickstart.

### ➡️ NOTE: This quickstart has not been published yet to https://quickstarts.snowflake.com.

___
Here is an overview of what we'll build in this lab:

<img src="images/demo_overview.png" width=800px>

## Requirements

- Java 11
- Maven

## Snowflake setup

Follow the instructions in `sql/01_setup_snowflake.sql` to prepare your Snowflake account.


## Deploy UDFs and stored procedures

```
mvn clean package snowflake:deploy
```

## Advanced

### View JAR dependencies to be uploaded

```
mvn dependency:list -DincludeScope=compile
```