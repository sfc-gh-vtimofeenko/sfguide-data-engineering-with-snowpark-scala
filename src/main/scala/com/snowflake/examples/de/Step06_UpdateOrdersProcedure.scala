package com.snowflake.examples.de

import com.snowflake.examples.utils.TableUtils.tableExists
import com.snowflake.examples.utils.WithLogging
import com.snowflake.examples.utils.WithSession
import com.snowflake.examples.utils.WithWHResize
import com.snowflake.snowpark.Column
import com.snowflake.snowpark.functions.col
import com.snowflake.snowpark.functions.current_timestamp

import scala.util.Failure
import scala.util.Success
import scala.util.Try

object Step06_UpdateOrdersProcedure extends WithLogging with WithSession with WithWHResize {

  /** Creates the table called ORDERS */
  private def createOrdersTable(): Try[Unit] = Try {
    logger info "Creating ORDERS table"
    session sql "CREATE TABLE HARMONIZED.ORDERS LIKE HARMONIZED.POS_FLATTENED_V" collect ()

    session sql "ALTER TABLE HARMONIZED.ORDERS ADD COLUMN META_UPDATED_AT TIMESTAMP" collect ()
  }

  /** Creates a stream on a the table called ORDERS */
  private def createOrdersStream(): Try[Unit] = Try {
    logger info "Creating ORDERS stream"
    session sql "CREATE STREAM HARMONIZED.ORDERS_STREAM ON TABLE HARMONIZED.ORDERS" collect ()
  }

  /** Merges the updates from the stream into HARMONIZED.ORDERS table */
  private def mergeOrderUpdates(): Try[Unit] = withWHResize {

    val source = session table "HARMONIZED.POS_FLATTENED_V_STREAM"
    val target = session table "HARMONIZED.ORDERS"

    val colsToUpdate: Map[Column, Column] =
      Map(source.schema.names.filter(!_.contains("METADATA")) map { sourceColName =>
        (target.col(sourceColName), source.col(sourceColName))
      }: _*) + (target.col("META_UPDATED_AT") -> current_timestamp())

    target
      .merge(source, target.col("ORDER_DETAIL_ID").equal_to(source.col("ORDER_DETAIL_ID")))
      .whenMatched
      .update(colsToUpdate)
      .whenNotMatched
      .insert(colsToUpdate)
      .collect()
  }

  /** Executes the SQL to generate HARMONIZED.ORDERS table
    *
    * Modeled after com.snowflake.examples.de.Step06_UpdateOrdersProcedure.execute in Java quickstart
    */
  private def execute(): String = {
    if (!tableExists(session, "HARMONIZED", "ORDERS")) {
      createOrdersTable match {
        case Failure(exception) => {
          logger error s"Could not create table ORDERS, reason: $exception"
          System exit 1
        }
        case Success(value) => logger info "Created table ORDERS"
      }
      createOrdersStream match {
        case Failure(exception) => {
          logger error s"Could not create stream ORDERS, reason: $exception"
          System exit 1
        }
        case Success(value) => logger info "Created stream on ORDERS"
      }
    }

    mergeOrderUpdates match {
      case Failure(exception) => {
        logger error s"Could not merge updates to ORDERS, reason: $exception"
        System exit 1
      }
      case Success(value) => logger info "Merged updates to ORDERS"
    }

    "Successfully processed ORDERS"
  }

  /** The main object entrypoint, to be run with sbt "runMain com.snowflake.examples.de.Step06_UpdateOrdersProcedure"
    *
    * Modeled after com.snowflake.examples.de.Step06_UpdateOrdersProcedure.main in Java quickstart
    */
  def main(args: Array[String]): Unit = {
    val output = execute
    logger info s"Received output: $output"
    session close
  }

}
