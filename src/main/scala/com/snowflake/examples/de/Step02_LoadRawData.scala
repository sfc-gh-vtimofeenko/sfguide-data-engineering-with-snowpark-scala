package com.snowflake.examples.de

import com.snowflake.examples.utils.TableUtils
import com.snowflake.examples.utils.WithLocalSession
import com.snowflake.examples.utils.WithWHResize
import com.snowflake.snowpark.Session

import scala.collection.immutable.HashMap
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object Step02_LoadRawData extends WithLocalSession with WithWHResize {

  val POS_TABLES: Seq[String] = Seq(
    "country",
    "franchise",
    "location",
    "menu",
    "truck",
    "order_header",
    "order_detail"
  )

  /** Small dataclass to pass the schema of ingested data */
  case class IngestedSchema(schema: String, tables: Seq[String])

  val CUSTOMER_TABLES: Seq[String] = Seq("customer_loyalty")

  val S3_PATH_INGEST_CONFIG: HashMap[String, IngestedSchema] =
    HashMap(
      "pos" -> IngestedSchema("RAW_POS", POS_TABLES),
      "customer" -> IngestedSchema("RAW_CUSTOMER", CUSTOMER_TABLES)
    )

  // Some constants
  val FROSTBYTE_STAGE = "@external.frostbyte_raw_stage"
  val PARTITIONED_TABLES = "order_header" :: "order_detail" :: Nil

  /** Tries to create a table in Snowflake
    *
    * Can throw underlying exceptions, so implenented as Try
    *
    * @param session
    *   Snowpark session
    * @param targetSchema
    *   Name of the schema where the table will be created
    * @param tableName
    *   Name of the table to create
    * @param s3Path
    *   Path in s3 which will be used to infer schema for the table
    * @return
    *   the name of the created table wrapped in one of [[scala.util.{Success, Failure}]]
    */
  def maybeCreateTable(session: Session, targetSchema: String, tableName: String, s3Path: String): Try[String] = Try {
    if (TableUtils.tableExists(session, targetSchema, tableName)) {
      logger.info(s"Table already exists: $tableName")
    } else {
      logger.info(s"Creating table: $tableName")
      TableUtils.createTableUsingParquetInference(session, tableName, s3Path);
    }
    tableName
  }

  /** Loads the data into a table. Same as Step02_LoadRawData.loadRawTable in java example
    *
    * Can throw underlying exceptions, so implenented as Try
    *
    * @param session
    *   Snowpark session
    * @param tableName
    *   Name of the table where the data will be loaded
    * @param s3Path
    *   Path to data
    * @param targetSchema
    *   Schema name
    * @return
    *   [[scala.util.Success]] if load is successful, [[scala.util.Failure]] otherwise
    */
  def loadRawTable(
      session: Session,
      tableName: String,
      s3Path: String,
      targetSchema: String
  ): Try[Unit] = Try {
    session.sql(s"USE SCHEMA $targetSchema")
    maybeCreateTable(session, targetSchema, tableName, s3Path) match {
      case Success(value) => {
        logger.info(s"Loading data to $value")
        session.read
          .option("compression", "snappy")
          .option("match_by_column_name", "case_insensitive")
          .parquet(s3Path)
          .copyInto(value);
      }
      case Failure(f) => {
        logger.error(s"Could not create table $tableName, reason: $f")
        System.exit(1)
      }
    }
  }

  /** Generates paths for data loading
    *
    * @param tableName
    *   name of the target table
    * @param baseS3Path
    *   base part of the path in S3
    * @return
    *   list of locations for data to be loaded to @tableName
    */
  def mkTablePaths(tableName: String, baseS3Path: String): Seq[String] = {
    if (PARTITIONED_TABLES contains tableName)
      Range.inclusive(2019, 2021).map(year => s"$FROSTBYTE_STAGE/$baseS3Path/$tableName/year=$year").toSeq
    else Seq(s"$FROSTBYTE_STAGE/$baseS3Path/$tableName")

  }

  /** Executes the SQL to ingest data
    *
    * Modeled after com.snowflake.examples.de.Step02_LoadRawData.execute in Java quickstart
    */
  def execute(session: Session): String = withWHResize(
    session, {
      S3_PATH_INGEST_CONFIG foreach {
        case (s3Path, value) => {
          val tableNames: Seq[String] = value.tables
          val targetSchema: String = value.schema

          session sql s"USE SCHEMA $targetSchema" collect ()

          // Generate a map of table x (optionally) locations partitioned by year
          // Extracted from loadRawTable to make it more functional
          val tableS3Paths = Map(tableNames map { table => (table, mkTablePaths(table, s3Path)) }: _*)

          tableS3Paths.foreach {
            case (tableName, tablePaths) => {
              tablePaths.map(s3Path =>
                loadRawTable(
                  session = session,
                  tableName = tableName,
                  s3Path = s3Path,
                  targetSchema = targetSchema
                ) match {
                  case Success(_) => logger info s"Loaded table $tableName successfully"
                  case Failure(f) => logger error s"Could not load table $tableName. Reason: $f"
                }
              )
            }
          }
        }
      }

      "Processing complete"
    }
  )

}
