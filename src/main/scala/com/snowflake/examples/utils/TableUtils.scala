package com.snowflake.examples.utils

import com.snowflake.snowpark.Session

object TableUtils {

  def tableExists(session: Session, schema: String, name: String): Boolean = {
    session
      .sql(
        s"SELECT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='$schema' AND TABLE_NAME = '$name')"
          .toUpperCase()
      )
      .collect()
      .head
      .getBoolean(0)
  }

  def createTableUsingParquetInference(
      session: Session,
      tableName: String,
      stagedParquetLocation: String
  ) = {
    val formatName = SqlUtils.addRandomSuffix("temp_parquet", 6)
    session.sql(SqlUtils.buildCreateTempParquetFormatSql(formatName)).collect();
    session
      .sql(
        SqlUtils.buildCreateTableInferSql(
          tableName,
          stagedParquetLocation,
          formatName
        )
      )
      .collect();

  }

}
