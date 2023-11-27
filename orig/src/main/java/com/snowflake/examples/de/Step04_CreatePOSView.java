package com.snowflake.examples.de;

import com.snowflake.examples.utils.LocalSessionHelper;
import com.snowflake.snowpark_java.Functions;
import com.snowflake.snowpark_java.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.snowflake.snowpark_java.Functions.col;

public class Step04_CreatePOSView {

    private static final Logger logger = LoggerFactory.getLogger(Step04_CreatePOSView.class);

    public void createPOSView(Session session) {
        var orderDetail = session.table("RAW_POS.ORDER_DETAIL")
                .select(col("ORDER_DETAIL_ID"),
                        col("LINE_NUMBER"),
                        col("MENU_ITEM_ID"),
                        col("QUANTITY"),
                        col("UNIT_PRICE"),
                        col("PRICE"),
                        col("ORDER_ID"));

        var orderHeader = session.table("RAW_POS.ORDER_HEADER")
                .select(col("ORDER_ID"),
                        col("TRUCK_ID"),
                        col("ORDER_TS"),
                        Functions.to_date(col("ORDER_TS")).alias("ORDER_TS_DATE"),
                        col("ORDER_AMOUNT"),
                        col("ORDER_TAX_AMOUNT"),
                        col("ORDER_DISCOUNT_AMOUNT"),
                        col("LOCATION_ID"),
                        col("ORDER_TOTAL"));

        var truck = session.table("RAW_POS.TRUCK")
                .select(col("TRUCK_ID"),
                        col("PRIMARY_CITY"),
                        col("REGION"),
                        col("COUNTRY"),
                        col("FRANCHISE_FLAG"),
                        col("FRANCHISE_ID"));

        var menu = session.table("RAW_POS.MENU")
                .select(col("MENU_ITEM_ID"),
                        col("TRUCK_BRAND_NAME"),
                        col("MENU_TYPE"),
                        col("MENU_ITEM_NAME"));

        var franchise = session.table("RAW_POS.FRANCHISE")
                .select(col("FRANCHISE_ID"),
                        col("FIRST_NAME").alias("FRANCHISEE_FIRST_NAME"),
                        col("LAST_NAME").alias("FRANCHISEE_LAST_NAME"));

        var location = session.table("RAW_POS.LOCATION")
                .select(col("LOCATION_ID"));


        // Perform joins
        var tWithF = truck.join(franchise, truck.col("FRANCHISE_ID").equal_to(franchise.col("FRANCHISE_ID")));
        var ohWtAndL = orderHeader.join(tWithF, orderHeader.col("TRUCK_ID").equal_to(tWithF.col("TRUCK_ID")))
                .join(location, orderHeader.col("LOCATION_ID").equal_to(location.col("LOCATION_ID")));
        var finalDf = orderDetail.join(ohWtAndL, orderDetail.col("ORDER_ID").equal_to(ohWtAndL.col("ORDER_ID")))
                .join(menu, orderDetail.col("MENU_ITEM_ID").equal_to(menu.col("MENU_ITEM_ID")));

        // Select columns for the final view
        // NOTE: Specify the source DF for ambiguous columns
        finalDf = finalDf.select(
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
        );

        // Create or replace view
        finalDf.createOrReplaceView("POS_FLATTENED_V");
    }

    public void createPOSViewStream(Session session) {
        session.sql("CREATE OR REPLACE STREAM POS_FLATTENED_V_STREAM ON VIEW POS_FLATTENED_V SHOW_INITIAL_ROWS = TRUE").collect();
    }

    public String execute(Session session) {
        // Set session context
        session.sql("use schema harmonized").collect();
        // First create a view for joining POS data
        createPOSView(session);
        // Then create a stream on the view to enable incremental processing
        createPOSViewStream(session);

        return "POS view objects created successfully";
    }

    public static void main(String[] args) {
        Session localSession = LocalSessionHelper.buildSnowparkSession();
        Step04_CreatePOSView instance = new Step04_CreatePOSView();
        String output = instance.execute(localSession);
        logger.info("Received output: " + output);
        localSession.close();
    }


}
