package com.snowflake.examples.utils

import com.snowflake.snowpark.Session

import java.io.FileInputStream
import java.io.FileNotFoundException
import java.util.Properties
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object LocalSession {

  /** Load properties from snowflake.properties file. If file not found -- return an expected [[scala.util.Failure]]
    * Otherwise -- rethrow exception
    */
  private def createSessionFromPropsFile(): Try[Session] = try {
    val prop = new Properties()
    prop.load(new FileInputStream("snowflake.properties"))
    Success(
      Session.builder
        .configs(
          Map(Seq("URL", "USER", "PASSWORD", "DB", "SCHEMA", "ROLE", "WAREHOUSE") map { key =>
            (key, prop.getProperty(key))
          }: _*)
        )
        .create
    )
  } catch {
    case e: FileNotFoundException => Failure(e) // OK Failure
    case t: Exception             => throw t // Not OK failure
  }

  def getLocalSession(): Session = {

    createSessionFromPropsFile() match {
      case Success(value) => value
      case Failure(_) => {
        // Fall back to environment variable
        try {
          createSessionFromSNOWSQLEnvVars()
        } catch {
          case e: NullPointerException =>
            println(
              "ERROR: Environment variable for Snowflake Connection not found. Please set the SNOWSQL_* environment variables"
            );
            e.printStackTrace()
            null
        }
      }
    }
  }

  private def createSessionFromSNOWSQLEnvVars(): Session = {
    val configMap: Map[String, String] = Map(
      "URL" -> (getEnv("SNOWSQL_ACCOUNT") + ".snowflakecomputing.com"),
      "USER" -> getEnv("SNOWSQL_USER"),
      "PASSWORD" -> getEnv("SNOWSQL_PWD"),
      "DB" -> getEnv("SNOWSQL_DATABASE"),
      "SCHEMA" -> getEnv("SNOWSQL_SCHEMA"),
      "ROLE" -> getEnv("SNOWSQL_ROLE"),
      "WAREHOUSE" -> getEnv("SNOWSQL_WAREHOUSE")
    )

    Session.builder.configs(configMap).create
  }

  @throws(classOf[NullPointerException])
  def getEnv(s: String): String = System.getenv(s) match {
    case null      => throw new NullPointerException(f"Environment variable, ${s}, not found")
    case v: String => v
  }

}

/** Allows running an object file on a local machine using sbt.
  */
trait WithLocalSession extends WithLogging {

  /** Abstract method to execute the main procedure of the object. Separate from main so it can be deployed as a stored
    * procedure in Snowflake
    *
    * @param session
    *   Snowpark session, requirement of Snowpark
    * @return
    *   string, so status can be reported when it's run as a stored procedure
    */
  def execute(session: Session): String

  /** Wrapper around .execute() that creates a session from environment variables */
  def main(args: Array[String]): Unit = {
    System getenv ("QUICKSTART_RUN_LOCALLY") match {
      case "TRUE" => {
        val session = LocalSession.getLocalSession()

        val output = execute(session)

        logger.info(s"Received output: $output")

        session.close()

      }
      case _ => {
        logger.error("This function should be run by sbt on a local machine")
        System exit 1
      }
    }
  }

}
