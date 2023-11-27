package com.snowflake.examples.de;


import com.snowflake.examples.utils.LocalSessionHelper;
import com.snowflake.examples.utils.TableUtils;
import com.snowflake.snowpark_java.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Step02_LoadRawData {

    private static final Logger logger = LoggerFactory.getLogger(Step02_LoadRawData.class);

    static final List<String> POS_TABLES = Arrays.asList("country", "franchise", "location", "menu", "truck", "order_header", "order_detail");
    @SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
    static final List<String> CUSTOMER_TABLES = Arrays.asList("customer_loyalty");
    static final Map<String, Map<String, Object>> S3_PATH_INGEST_CONFIG = new HashMap<>();

    static {
        Map<String, Object> posData = new HashMap<>();
        posData.put("schema", "RAW_POS");
        posData.put("tables", POS_TABLES);
        S3_PATH_INGEST_CONFIG.put("pos", posData);

        Map<String, Object> customerData = new HashMap<>();
        customerData.put("schema", "RAW_CUSTOMER");
        customerData.put("tables", CUSTOMER_TABLES);
        S3_PATH_INGEST_CONFIG.put("customer", customerData);
    }

    public void loadRawTable(Session session, String tableName, String s3Path, String yearPartition, String targetSchema) {
        session.sql("use schema " + targetSchema).collect();
        String stagedParquetLocation;
        if (yearPartition == null) {
            stagedParquetLocation = String.format("@external.frostbyte_raw_stage/%s/%s", s3Path, tableName);
        } else {
            logger.info("\tLoading year " + yearPartition);
            stagedParquetLocation = String.format("@external.frostbyte_raw_stage/%s/%s/year=%s", s3Path, tableName, yearPartition);
        }
        if (TableUtils.tableExists(session, targetSchema, tableName)) {
            logger.info("Table already exists: " + tableName);
        } else {
            logger.info("Creating table: " + tableName);
            TableUtils.createTableUsingParquetInference(session, tableName, stagedParquetLocation);
        }
        var df = session.read().option("compression", "snappy").option("match_by_column_name", "case_insensitive").parquet(stagedParquetLocation);
        df.copyInto(tableName);
    }
    
    @SuppressWarnings("unchecked")
    public String execute(Session session) {
        session.sql("ALTER WAREHOUSE JAVA_DE_WH SET WAREHOUSE_SIZE = 'XLARGE' WAIT_FOR_COMPLETION = TRUE").collect();

        for (Map.Entry<String, Map<String, Object>> entry : S3_PATH_INGEST_CONFIG.entrySet()) {
            String s3Path = entry.getKey();            
            List<String> tableNames = (List<String>) entry.getValue().get("tables");
            String targetSchema = (String) entry.getValue().get("schema");

            for (String tableName : tableNames) {
                logger.info("Loading " + tableName);
                if (tableName.equals("order_header") || tableName.equals("order_detail")) {
                    for (String year : Arrays.asList("2019", "2020", "2021")) {
                        loadRawTable(session, tableName, s3Path, year, targetSchema);
                    }
                } else {
                    loadRawTable(session, tableName, s3Path, null, targetSchema);
                }
            }
        }

        session.sql("ALTER WAREHOUSE JAVA_DE_WH SET WAREHOUSE_SIZE = XSMALL").collect();

        return "Processing complete";
    }

    public static void main(String[] args) {
        Session localSession = LocalSessionHelper.buildSnowparkSession();
        Step02_LoadRawData instance = new Step02_LoadRawData();
        String output = instance.execute(localSession);
        logger.info("Received output: " + output);
        localSession.close();
    }

}