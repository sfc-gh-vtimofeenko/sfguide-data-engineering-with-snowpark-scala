package com.snowflake.examples.de

import com.snowflake.examples.utils.DataframeHelpers
import com.snowflake.examples.utils.TableUtils.tableExists
import com.snowflake.examples.utils.WithLocalSession
import com.snowflake.examples.utils.WithWHResize
import com.snowflake.snowpark.Column
import com.snowflake.snowpark.MergeResult
import com.snowflake.snowpark.Session
import com.snowflake.snowpark.functions.current_timestamp

import scala.util.Failure
import scala.util.Success
import scala.util.Try

object Step06_UpdateOrdersProcedure extends WithLocalSession with WithWHResize with DataframeHelpers {

  /** Creates the table called ORDERS */
  private def createOrdersTable(session: Session): Try[Unit] = Try {
    logger.info("Creating ORDERS table")
    session.sql("CREATE TABLE HARMONIZED.ORDERS LIKE HARMONIZED.POS_FLATTENED_V").collect()
    session.sql("ALTER TABLE HARMONIZED.ORDERS ADD COLUMN META_UPDATED_AT TIMESTAMP").collect()
  }

  /** Creates a stream on a the table called ORDERS */
  private def createOrdersStream(session: Session): Try[Unit] = Try {
    logger.info("Creating ORDERS stream")
    session.sql("CREATE STREAM HARMONIZED.ORDERS_STREAM ON TABLE HARMONIZED.ORDERS").collect()
  }

  /** Merges the updates from the stream into HARMONIZED.ORDERS table */
  private def mergeOrderUpdates(session: Session): Try[MergeResult] = withWHResize(
    session, {

      val source = session.table("HARMONIZED.POS_FLATTENED_V_STREAM")
      val target = session.table("HARMONIZED.ORDERS")

      val colsToUpdate: Map[Column, Column] =
        Map(source.schema.names.filter(!_.contains("METADATA")) map { sourceColName =>
          (target.col(sourceColName), source.col(sourceColName))
        }: _*) + (target.col("META_UPDATED_AT") -> current_timestamp())

      Success(
        target
          // This method uses the implicit class UpdatableHelpers
          .merge(source, "ORDER_DETAIL_ID")
          .whenMatched
          .update(colsToUpdate)
          .whenNotMatched
          .insert(colsToUpdate)
          .collect()
      )

    }
  )

  /** Executes the SQL to generate HARMONIZED.ORDERS table
    *
    * Modeled after com.snowflake.examples.de.Step06_UpdateOrdersProcedure.execute in Java quickstart
    *
    * It will be used as a stored procedure in Snowflake
    */
  def execute(session: Session): String = {
    if (!tableExists(session, "HARMONIZED", "ORDERS")) {
      createOrdersTable(session) match {
        case Failure(exception) => {
          logger.error(s"Could not create table ORDERS, reason: $exception")
          System exit 1
        }
        case Success(_) => logger.info("Created table ORDERS")
      }
      createOrdersStream(session) match {
        case Failure(exception) => {
          logger.error(s"Could not create stream ORDERS, reason: $exception")
          System exit 1
        }
        case Success(_) => logger info "Created stream on ORDERS"
      }
    }

    mergeOrderUpdates(session) match {
      case Failure(exception) => {
        logger.error(s"Could not merge updates to ORDERS, reason: $exception")
        System exit 1
      }
      case Success(_) => logger.info("Merged updates to ORDERS")
    }

    "Successfully processed ORDERS"
  }

}
