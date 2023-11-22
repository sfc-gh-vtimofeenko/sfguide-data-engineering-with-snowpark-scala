package com.snowflake.examples.utils;

import com.snowflake.snowpark_java.Session;

public class TableUtils {
    public static boolean tableExists(Session session, String schema, String name) {
        String query = String.format("SELECT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s') AS TABLE_EXISTS", schema.toUpperCase(), name.toUpperCase());
        return session.sql(query).collect()[0].getBoolean(0);
    }

    public static void createTableUsingParquetInference(Session session, String tableName, String stagedParquetLocation) {
        String formatName = SqlUtils.addRandomSuffix("temp_parquet", 6);
        session.sql(SqlUtils.buildCreateTempParquetFormatSql(formatName)).collect();
        session.sql(SqlUtils.buildCreateTableInferSql(tableName, stagedParquetLocation, formatName)).collect();
    }

}
