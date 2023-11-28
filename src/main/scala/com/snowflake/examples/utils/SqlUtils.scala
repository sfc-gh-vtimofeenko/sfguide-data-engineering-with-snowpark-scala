package com.snowflake.examples.utils

import java.security.SecureRandom
import java.util.stream.Collectors
import java.util.stream.IntStream

object SqlUtils {

  val ALPHANUMERIC: String =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  val RANDOM: SecureRandom = new SecureRandom();

  def addRandomSuffix(inputString: String, suffixLength: Integer): String = {
    val randomSuffix = IntStream
      .range(0, suffixLength)
      .map(i => ALPHANUMERIC.charAt(RANDOM.nextInt(ALPHANUMERIC.length())))
      .mapToObj(c => String.valueOf(c))
      .collect(Collectors.joining())
    inputString + randomSuffix
  }

  def buildCreateTempParquetFormatSql(formatName: String): String =
    s"CREATE TEMP FILE FORMAT $formatName TYPE = PARQUET"

  def buildCreateTableInferSql(
      tableName: String,
      inferLocation: String,
      formatName: String
  ) = {
    val ignoreCase = "TRUE"
    s"""CREATE TABLE $tableName
        | USING TEMPLATE (SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
        | FROM TABLE(INFER_SCHEMA(
        | LOCATION=>'$inferLocation',
        | FILE_FORMAT=>'$formatName',
        | IGNORE_CASE=>$ignoreCase)))""".stripMargin
  }

}
