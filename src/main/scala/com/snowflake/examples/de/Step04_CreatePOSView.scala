package com.snowflake.examples.de

import com.snowflake.examples.utils.WithLocalSession
import com.snowflake.snowpark.Session
import com.snowflake.snowpark.functions._

import scala.util.Failure
import scala.util.Success
import scala.util.Try

object Step04_CreatePOSView extends WithLocalSession {

  /** Tries to create a view on top of POS data.
    *
    * Can throw, so implemented as Try
    *
    * @param session
    *   Snowpark session
    */
  private def createPOSView(session: Session): Try[Unit] = Try {
    val orderDetail = session table "RAW_POS.ORDER_DETAIL" select Array(
      col("ORDER_DETAIL_ID"),
      col("LINE_NUMBER"),
      col("MENU_ITEM_ID"),
      col("QUANTITY"),
      col("UNIT_PRICE"),
      col("PRICE"),
      col("ORDER_ID")
    )

    val orderHeader = session table "RAW_POS.ORDER_HEADER" select Array(
      col("ORDER_ID"),
      col("TRUCK_ID"),
      col("ORDER_TS"),
      to_date(col("ORDER_TS")).alias("ORDER_TS_DATE"),
      col("ORDER_AMOUNT"),
      col("ORDER_TAX_AMOUNT"),
      col("ORDER_DISCOUNT_AMOUNT"),
      col("LOCATION_ID"),
      col("ORDER_TOTAL")
    )

    val truck = session table "RAW_POS.TRUCK" select Array(
      col("TRUCK_ID"),
      col("PRIMARY_CITY"),
      col("REGION"),
      col("COUNTRY"),
      col("FRANCHISE_FLAG"),
      col("FRANCHISE_ID")
    )

    val menu = session table "RAW_POS.MENU" select Array(
      col("MENU_ITEM_ID"),
      col("TRUCK_BRAND_NAME"),
      col("MENU_TYPE"),
      col("MENU_ITEM_NAME")
    )

    val franchise = session table "RAW_POS.FRANCHISE" select Array(
      col("FRANCHISE_ID"),
      col("FIRST_NAME").alias("FRANCHISEE_FIRST_NAME"),
      col("LAST_NAME").alias("FRANCHISEE_LAST_NAME")
    )

    val location = session table "RAW_POS.LOCATION" select Array(col("LOCATION_ID"))

    // Perform joins
    val tWithF = truck.join(franchise, truck.col("FRANCHISE_ID").equal_to(franchise.col("FRANCHISE_ID")))
    val ohWtAndL = orderHeader
      .join(tWithF, orderHeader.col("TRUCK_ID").equal_to(tWithF.col("TRUCK_ID")))
      .join(location, orderHeader.col("LOCATION_ID").equal_to(location.col("LOCATION_ID")))

    orderDetail
      .join(ohWtAndL, orderDetail.col("ORDER_ID").equal_to(ohWtAndL.col("ORDER_ID")))
      .join(menu, orderDetail.col("MENU_ITEM_ID").equal_to(menu.col("MENU_ITEM_ID")))
      .select(
        orderDetail.col("ORDER_ID"),
        orderHeader.col("TRUCK_ID"),
        col("ORDER_TS"),
        col("ORDER_TS_DATE"),
        col("ORDER_DETAIL_ID"),
        col("LINE_NUMBER"),
        col("TRUCK_BRAND_NAME"),
        col("MENU_TYPE"),
        col("PRIMARY_CITY"),
        col("REGION"),
        col("COUNTRY"),
        col("FRANCHISE_FLAG"),
        franchise.col("FRANCHISE_ID"),
        col("FRANCHISEE_FIRST_NAME"),
        col("FRANCHISEE_LAST_NAME"),
        orderHeader.col("LOCATION_ID"),
        orderDetail.col("MENU_ITEM_ID"),
        col("MENU_ITEM_NAME"),
        col("QUANTITY"),
        col("UNIT_PRICE"),
        col("PRICE"),
        col("ORDER_AMOUNT"),
        col("ORDER_TAX_AMOUNT"),
        col("ORDER_DISCOUNT_AMOUNT"),
        col("ORDER_TOTAL")
      )
      .createOrReplaceView("POS_FLATTENED_V")
  }

  /** Creates stream on the view
    *
    * @param session
    *   Snowpark session
    */
  private def createPOSViewStream(session: Session): Try[Unit] = Try {
    session sql "CREATE OR REPLACE STREAM POS_FLATTENED_V_STREAM ON VIEW POS_FLATTENED_V SHOW_INITIAL_ROWS = TRUE" collect
  }

  /** Executes the SQL to ingest data. Returns the success string.
    *
    * Modeled after com.snowflake.examples.de.Step04_CreatePOSView.execute in Java quickstart
    */
  def execute(session: Session) = {
    // Set session context
    session sql "use schema harmonized" collect ()
    // First create a view for joining POS data
    createPOSView(session) match {
      case Success(value) => logger info "Created POS View"
      case Failure(exception) => {
        logger error s"Could not create POS view, reason: $exception"
        System exit 1
      }
    }
    // Then create a stream on the view to enable incremental processing
    createPOSViewStream(session) match {
      case Success(value) => logger info "Created POS view stream"
      case Failure(exception) => {
        logger error s"Could not create POS view stream, reason: $exception"
        System exit 1
      }
    }
    "POS view objects created successfully"
  }

}
