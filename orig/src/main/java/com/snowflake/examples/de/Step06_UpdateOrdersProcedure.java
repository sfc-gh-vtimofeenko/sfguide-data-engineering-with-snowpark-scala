package com.snowflake.examples.de;

import com.snowflake.examples.utils.LocalSessionHelper;
import com.snowflake.examples.utils.TableUtils;
import com.snowflake.snowpark_java.Column;
import com.snowflake.snowpark_java.Functions;
import com.snowflake.snowpark_java.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class Step06_UpdateOrdersProcedure {

    private static final Logger logger = LoggerFactory.getLogger(Step06_UpdateOrdersProcedure.class);

    public void createOrdersTable(Session session) {
        session.sql("CREATE TABLE HARMONIZED.ORDERS LIKE HARMONIZED.POS_FLATTENED_V").collect();
        session.sql("ALTER TABLE HARMONIZED.ORDERS ADD COLUMN META_UPDATED_AT TIMESTAMP").collect();
    }

    public void createOrdersStream(Session session) {
        session.sql("CREATE STREAM HARMONIZED.ORDERS_STREAM ON TABLE HARMONIZED.ORDERS").collect();
    }

    public void mergeOrderUpdates(Session session) {
        session.sql("ALTER WAREHOUSE JAVA_DE_WH SET WAREHOUSE_SIZE = 'XLARGE' WAIT_FOR_COMPLETION = TRUE").collect();

        var source = session.table("HARMONIZED.POS_FLATTENED_V_STREAM");
        var target = session.table("HARMONIZED.ORDERS");

        Map<Column, Column> colsToUpdate = new HashMap<>();
        for (String sourceColName : source.schema().names()) {
            if (!sourceColName.contains("METADATA")) {
                colsToUpdate.put(target.col(sourceColName), source.col(sourceColName));
            }
        }
        colsToUpdate.put(target.col("META_UPDATED_AT"), Functions.current_timestamp());

        target.merge(source, target.col("ORDER_DETAIL_ID").equal_to(source.col("ORDER_DETAIL_ID")))
                .whenMatched().update(colsToUpdate)
                .whenNotMatched().insert(colsToUpdate)
                .collect();

        session.sql("ALTER WAREHOUSE JAVA_DE_WH SET WAREHOUSE_SIZE = XSMALL").collect();
    }

    public String execute(Session session) {
        if (!TableUtils.tableExists(session, "HARMONIZED", "ORDERS")) {
            createOrdersTable(session);
            createOrdersStream(session);
        }

        mergeOrderUpdates(session);
        return "Successfully processed ORDERS";
    }

    public static void main(String[] args) {
        Session localSession = LocalSessionHelper.buildSnowparkSession();
        Step06_UpdateOrdersProcedure instance = new Step06_UpdateOrdersProcedure();
        String output = instance.execute(localSession);
        logger.info("Received output: " + output);
        localSession.close();
    }

}