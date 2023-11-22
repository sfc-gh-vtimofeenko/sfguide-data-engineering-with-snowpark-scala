package com.snowflake.examples.de;

import com.snowflake.examples.utils.LocalSessionHelper;
import com.snowflake.examples.utils.TableUtils;
import com.snowflake.snowpark_java.*;
import com.snowflake.snowpark_java.types.DataTypes;
import com.snowflake.snowpark_java.types.StructField;
import com.snowflake.snowpark_java.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.snowflake.snowpark_java.Functions.*;

public class Step07_UpdateDailyCityMetricsProcedure {

    private static final Logger logger = LoggerFactory.getLogger(Step06_UpdateOrdersProcedure.class);

    private void createMetricsTable(Session session) {
        List<StructField> sharedColumns = List.of(
                new StructField("DATE", DataTypes.DateType),
                new StructField("CITY_NAME", DataTypes.StringType),
                new StructField("COUNTRY_DESC", DataTypes.StringType),
                new StructField("DAILY_SALES", DataTypes.StringType),
                new StructField("AVG_TEMPERATURE_FAHRENHEIT", DataTypes.createDecimalType(38, 0)),
                new StructField("AVG_TEMPERATURE_CELSIUS", DataTypes.createDecimalType(38, 0)),
                new StructField("AVG_PRECIPITATION_INCHES", DataTypes.createDecimalType(38, 0)),
                new StructField("AVG_PRECIPITATION_MILLIMETERS", DataTypes.createDecimalType(38, 0)),
                new StructField("MAX_WIND_SPEED_100M_MPH", DataTypes.createDecimalType(38, 0))
        );
        List<StructField> dailyCityMetricsColumns = new ArrayList<>(sharedColumns);
        dailyCityMetricsColumns.add(new StructField("META_UPDATED_AT", DataTypes.TimestampType));
        StructType dailyCityMetricsSchema = new StructType(dailyCityMetricsColumns.toArray(new StructField[0]));

        var dcm = session.createDataFrame(new Row[]{}, dailyCityMetricsSchema);
        dcm.write().mode(SaveMode.Overwrite).saveAsTable("ANALYTICS.DAILY_CITY_METRICS");
    }

    public static void mergeDailyCityMetrics(Session session) {
        // Resize the warehouse before starting the merge operation
        session.sql("ALTER WAREHOUSE JAVA_DE_WH SET WAREHOUSE_SIZE = 'XLARGE' WAIT_FOR_COMPLETION = TRUE").collect();

        // Log the count of records in the stream
        long ordersStreamCount = session.table("HARMONIZED.ORDERS_STREAM").count();
        System.out.println(ordersStreamCount + " records in stream");

        // Get distinct order stream dates
        DataFrame ordersStreamDates = session.table("HARMONIZED.ORDERS_STREAM")
                .select(col("ORDER_TS_DATE").as("DATE"))
                .distinct();
        ordersStreamDates.limit(5).show();

        // Aggregate orders data
        DataFrame orders = session.table("HARMONIZED.ORDERS_STREAM")
                .groupBy(col("ORDER_TS_DATE"), col("PRIMARY_CITY"), col("COUNTRY"))
                .agg(sum(col("PRICE")).as("price_nulls"))
                .withColumn("DAILY_SALES", sqlExpr("ZEROIFNULL(price_nulls)"))
                .select(col("ORDER_TS_DATE").as("DATE"),
                        col("PRIMARY_CITY").as("CITY_NAME"),
                        col("COUNTRY").as("COUNTRY_DESC"),
                        col("DAILY_SALES")
                );

        // Join weather data with postal codes and countries
        DataFrame weatherPc = session.table("FROSTBYTE_WEATHERSOURCE.ONPOINT_ID.POSTAL_CODES");
        System.out.println("weatherPc:");
        weatherPc.schema().printTreeString();

        DataFrame countries = session.table("RAW_POS.COUNTRY");
        System.out.println("countries:");
        countries.schema().printTreeString();

        DataFrame weather = session.table("FROSTBYTE_WEATHERSOURCE.ONPOINT_ID.HISTORY_DAY");
        System.out.println("weather:");
        weather.schema().printTreeString();

        weather = weather
                .join(weatherPc, weather.col("POSTAL_CODE").equal_to(weatherPc.col("POSTAL_CODE")))
                .join(countries, weather.col("COUNTRY").equal_to(countries.col("ISO_COUNTRY")).and(weather.col("CITY_NAME").equal_to(countries.col("CITY"))))
                .join(ordersStreamDates, weather.col("DATE_VALID_STD").equal_to(ordersStreamDates.col("DATE")))
                .drop(weatherPc.col("CITY_NAME"));
        System.out.println("weather after join 1:");
        weather.schema().printTreeString();


        // Aggregate weather data
        DataFrame weatherAgg = weather.groupBy(col("DATE_VALID_STD"), col("CITY_NAME"), col("COUNTRY"))
                .agg(
                        avg(col("AVG_TEMPERATURE_AIR_2M_F")).as("AVG_TEMPERATURE_F"),
                        avg(callUDF("HARMONIZED.FAHRENHEIT_TO_CELSIUS_UDF", col("AVG_TEMPERATURE_AIR_2M_F"))).as("AVG_TEMPERATURE_C"),
                        avg(col("TOT_PRECIPITATION_IN")).as("AVG_PRECIPITATION_IN"),
                        avg(callUDF("HARMONIZED.INCH_TO_MILLIMETER_UDF", col("TOT_PRECIPITATION_IN"))).as("AVG_PRECIPITATION_MM"),
                        max(col("MAX_WIND_SPEED_100M_MPH")).as("MAX_WIND_SPEED_100M_MPH")
                )
                .select(
                        col("DATE_VALID_STD").as("DATE"),
                        weather.col("CITY_NAME"),
                        col("COUNTRY").as("COUNTRY_DESC"),
                        round(col("AVG_TEMPERATURE_F"), lit(2)).as("AVG_TEMPERATURE_FAHRENHEIT"),
                        round(col("AVG_TEMPERATURE_C"), lit(2)).as("AVG_TEMPERATURE_CELSIUS"),
                        round(col("AVG_PRECIPITATION_IN"), lit(2)).as("AVG_PRECIPITATION_INCHES"),
                        round(col("AVG_PRECIPITATION_MM"), lit(2)).as("AVG_PRECIPITATION_MILLIMETERS"),
                        col("MAX_WIND_SPEED_100M_MPH")
                );
        System.out.println("weather agg:");
        weather.schema().printTreeString();

        // Prepare staging DataFrame for merging
        DataFrame dailyCityMetricsStg = orders.join(weatherAgg,
                        orders.col("DATE").equal_to(weatherAgg.col("DATE"))
                                .and(orders.col("CITY_NAME").equal_to(weatherAgg.col("CITY_NAME")))
                                .and(orders.col("COUNTRY_DESC").equal_to(weatherAgg.col("COUNTRY_DESC"))),
                        "left");
        dailyCityMetricsStg = dailyCityMetricsStg.drop(weatherAgg.col("DATE"),weatherAgg.col("CITY_NAME"), weatherAgg.col("COUNTRY_DESC"));
        System.out.println("dailyCityMetricsStg before select:");
        weather.schema().printTreeString();
        dailyCityMetricsStg = dailyCityMetricsStg.select("DATE", "CITY_NAME", "COUNTRY_DESC", "DAILY_SALES", "AVG_TEMPERATURE_FAHRENHEIT",
                        "AVG_TEMPERATURE_CELSIUS", "AVG_PRECIPITATION_INCHES", "AVG_PRECIPITATION_MILLIMETERS",
                        "MAX_WIND_SPEED_100M_MPH");


        Map<Column, Column> colsToUpdate = new HashMap<>();
        for (String sourceColName : dailyCityMetricsStg.schema().names()) {
            colsToUpdate.put(col(sourceColName), dailyCityMetricsStg.col(sourceColName));
        }
        colsToUpdate.put(col("META_UPDATED_AT"), Functions.current_timestamp());

        // Merge the staging DataFrame into the target table
        var dcm = session.table("ANALYTICS.DAILY_CITY_METRICS");
        dcm.merge(
                        dailyCityMetricsStg, dcm.col("DATE").equal_to(dailyCityMetricsStg.col("DATE"))
                                .and(dcm.col("CITY_NAME").equal_to(dailyCityMetricsStg.col("CITY_NAME")))
                                .and(dcm.col("COUNTRY_DESC").equal_to((dailyCityMetricsStg.col("COUNTRY_DESC")))))
                .whenMatched().update(colsToUpdate)
                .whenNotMatched().insert(colsToUpdate)
                .collect();

        // Resize the warehouse after the merge operation
        session.sql("ALTER WAREHOUSE JAVA_DE_WH SET WAREHOUSE_SIZE = 'XSMALL' WAIT_FOR_COMPLETION = TRUE").collect();
    }


    public String execute(Session session) {
        // Create the DAILY_CITY_METRICS table if it doesn't exist
        if (!TableUtils.tableExists(session, "ANALYTICS", "DAILY_CITY_METRICS")) {
            createMetricsTable(session);
        }
        mergeDailyCityMetrics(session);
        return "Successfully processed DAILY_CITY_METRICS";
    }

    public static void main(String[] args) {
        Session localSession = LocalSessionHelper.buildSnowparkSession();
        Step07_UpdateDailyCityMetricsProcedure instance = new Step07_UpdateDailyCityMetricsProcedure();
        String output = instance.execute(localSession);
        logger.info("Received output: " + output);
        localSession.close();
    }

}
