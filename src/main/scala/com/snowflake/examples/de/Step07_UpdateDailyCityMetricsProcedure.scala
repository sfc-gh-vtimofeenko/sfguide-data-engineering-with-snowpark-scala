package com.snowflake.examples.de

import com.snowflake.examples.utils.DataframeHelpers
import com.snowflake.examples.utils.TableUtils.tableExists
import com.snowflake.examples.utils.WithLocalSession
import com.snowflake.examples.utils.WithWHResize
import com.snowflake.snowpark.Column
import com.snowflake.snowpark.DataFrame
import com.snowflake.snowpark.MergeResult
import com.snowflake.snowpark.SaveMode
import com.snowflake.snowpark.Session
import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.types.StructField
import com.snowflake.snowpark.types._

import scala.util.Failure
import scala.util.Success
import scala.util.Try

object Step07_UpdateDailyCityMetricsProcedure extends WithLocalSession with WithWHResize with DataframeHelpers {

  def createMetricsTable(session: Session): Try[Unit] = Try {
    val schemaForDataFile = StructType(
      Seq(
        StructField("DATE", DateType, true),
        StructField("CITY_NAME", StringType, true),
        StructField("COUNTRY_DESC", StringType, true),
        StructField("DAILY_SALES", StringType, true),
        StructField("AVG_TEMPERATURE_FAHRENHEIT", DecimalType(38, 0)),
        StructField("AVG_TEMPERATURE_CELSIUS", DecimalType(38, 0)),
        StructField("AVG_PRECIPITATION_INCHES", DecimalType(38, 0)),
        StructField("AVG_PRECIPITATION_MILLIMETERS", DecimalType(38, 0)),
        StructField("MAX_WIND_SPEED_100M_MPH", DecimalType(38, 0)),
        StructField("META_UPDATED_AT", TimestampType)
      )
    )

    val dcm = session createDataFrame (Seq(), schemaForDataFile)
    dcm.write.mode(SaveMode.Overwrite) saveAsTable ("ANALYTICS.DAILY_CITY_METRICS")

  }

  private def mergeDailyCityMetrics(session: Session): Try[MergeResult] = withWHResize(
    session, {

      // Log the count of records in the stream
      val ordersStreamCount: Long = session.table("HARMONIZED.ORDERS_STREAM").count()

      logger.info(s"There are $ordersStreamCount records in the stream")

      // Get distinct order stream dates
      val ordersStreamDates: DataFrame =
        session.table("HARMONIZED.ORDERS_STREAM").select(col("ORDER_TS_DATE").as("DATE")).distinct()

      ordersStreamDates.limit(5).show()

      // Aggregate orders data
      val orders: DataFrame =
        session
          .table("HARMONIZED.ORDERS_STREAM")
          .groupBy(col("ORDER_TS_DATE"), col("PRIMARY_CITY"), col("COUNTRY"))
          .agg(sum(col("PRICE")).as("price_nulls"))
          .withColumn("DAILY_SALES", sqlExpr("ZEROIFNULL(price_nulls)"))
          .select(
            col("ORDER_TS_DATE").as("DATE"),
            col("PRIMARY_CITY").as("CITY_NAME"),
            col("COUNTRY").as("COUNTRY_DESC"),
            col("DAILY_SALES")
          )

      // Join weather data with postal codes and countries
      val weatherPc = session.table("FROSTBYTE_WEATHERSOURCE.ONPOINT_ID.POSTAL_CODES").printAndPassDf("weatherPc")
      val countries = session.table("RAW_POS.COUNTRY").printAndPassDf("countries")
      val weather = session.table("FROSTBYTE_WEATHERSOURCE.ONPOINT_ID.HISTORY_DAY").printAndPassDf("weather")

      val weatherJoined = weather
        .join(weatherPc, usingColumn = "POSTAL_CODE")
        .join(
          countries,
          weather
            .col("COUNTRY")
            .equal_to(countries.col("ISO_COUNTRY"))
            .and(weather.col("CITY_NAME").equal_to(countries.col("CITY")))
        )
        .join(ordersStreamDates, weather.col("DATE_VALID_STD").equal_to(ordersStreamDates.col("DATE")))
        .drop(weatherPc.col("CITY_NAME"))
        .printAndPassDf("weather after join 1")

      val weatherAgg = weatherJoined
        .groupBy(col("DATE_VALID_STD"), col("CITY_NAME"), col("COUNTRY"))
        .agg(
          avg(col("AVG_TEMPERATURE_AIR_2M_F")).as("AVG_TEMPERATURE_F"),
          avg(callUDF("HARMONIZED.FAHRENHEIT_TO_CELSIUS_UDF", col("AVG_TEMPERATURE_AIR_2M_F"))).as("AVG_TEMPERATURE_C"),
          avg(col("TOT_PRECIPITATION_IN")).as("AVG_PRECIPITATION_IN"),
          avg(callUDF("HARMONIZED.INCH_TO_MILLIMETER_UDF", col("TOT_PRECIPITATION_IN"))).as("AVG_PRECIPITATION_MM"),
          max(col("MAX_WIND_SPEED_100M_MPH")).as("MAX_WIND_SPEED_100M_MPH")
        )
        .select(
          col("DATE_VALID_STD").as("DATE"),
          weatherJoined.col("CITY_NAME"),
          col("COUNTRY").as("COUNTRY_DESC"),
          round(col("AVG_TEMPERATURE_F"), lit(2)).as("AVG_TEMPERATURE_FAHRENHEIT"),
          round(col("AVG_TEMPERATURE_C"), lit(2)).as("AVG_TEMPERATURE_CELSIUS"),
          round(col("AVG_PRECIPITATION_IN"), lit(2)).as("AVG_PRECIPITATION_INCHES"),
          round(col("AVG_PRECIPITATION_MM"), lit(2)).as("AVG_PRECIPITATION_MILLIMETERS"),
          col("MAX_WIND_SPEED_100M_MPH")
        )
        .printAndPassDf("weather agg")

      // Prepare staging DataFrame for merging
      val dailyCityMetricsStg: DataFrame = orders
        .join(
          weatherAgg,
          usingColumns = Seq("DATE", "CITY_NAME", "COUNTRY_DESC"),
          "left"
        )
        .printAndPassDf("dailyCityMetricsStg before select")
        .select(
          "DATE",
          "CITY_NAME",
          "COUNTRY_DESC",
          "DAILY_SALES",
          "AVG_TEMPERATURE_FAHRENHEIT",
          "AVG_TEMPERATURE_CELSIUS",
          "AVG_PRECIPITATION_INCHES",
          "AVG_PRECIPITATION_MILLIMETERS",
          "MAX_WIND_SPEED_100M_MPH"
        )

      val colsToUpdate: Map[Column, Column] =
        Map(dailyCityMetricsStg.schema.names map { sourceColName =>
          (col(sourceColName), dailyCityMetricsStg.col(sourceColName))
        }: _*) + (col("META_UPDATED_AT") -> current_timestamp())

      val dcm = session.table("ANALYTICS.DAILY_CITY_METRICS")

      Success(
        dcm
          .merge(dailyCityMetricsStg, Seq("DATE", "CITY_NAME", "COUNTRY_DESC"))
          .whenMatched
          .update(colsToUpdate)
          .whenNotMatched
          .insert(colsToUpdate)
          .collect()
      )
    }
  )

  def execute(session: Session) = {
    // Create the DAILY_CITY_METRICS table if it doesn't exist
    if (!tableExists(session, "ANALYTICS", "DAILY_CITY_METRICS")) {
      createMetricsTable(session) match {
        case Failure(exception) => {
          logger error s"Could not create table ANALYTICS.DAILY_CITY_METRICS, reason: $exception"
          System exit 1
        }
        case Success(value) => logger info "Created table ANALYTICS.DAILY_CITY_METRICS"
      } // TODO: Move to a CreateIfNotExists function?
    }

    mergeDailyCityMetrics(session)

    "Successfully processed DAILY_CITY_METRICS"
  }

}
