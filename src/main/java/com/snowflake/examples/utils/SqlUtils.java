package com.snowflake.examples.utils;

import java.security.SecureRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SqlUtils {

    private static final String ALPHANUMERIC = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static final SecureRandom RANDOM = new SecureRandom();

    public static String addRandomSuffix(String inputString, int suffixLength) {
        String randomSuffix = IntStream.range(0, suffixLength).map(i -> ALPHANUMERIC.charAt(RANDOM.nextInt(ALPHANUMERIC.length()))).mapToObj(c -> String.valueOf((char) c)).collect(Collectors.joining());
        return inputString + randomSuffix;
    }

    public static String buildCreateTempParquetFormatSql(String formatName) {
        return String.format("CREATE TEMP FILE FORMAT %s TYPE = PARQUET", formatName);
    }

    public static String buildCreateTableInferSql(String tableName, String inferLocation, String formatName) {
        String ignoreCase = "TRUE";
        return String.format("CREATE TABLE %s USING TEMPLATE (SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) FROM TABLE(INFER_SCHEMA(LOCATION=>'%s', FILE_FORMAT=>'%s', IGNORE_CASE=>%s)))", tableName, inferLocation, formatName, ignoreCase);
    }

}
