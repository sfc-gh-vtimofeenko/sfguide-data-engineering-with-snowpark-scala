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
