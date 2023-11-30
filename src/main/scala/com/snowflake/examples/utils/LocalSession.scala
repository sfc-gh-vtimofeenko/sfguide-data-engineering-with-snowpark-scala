package com.snowflake.examples.utils

import com.snowflake.snowpark.Session

object LocalSession {

  def getLocalSession(): Session = {

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
