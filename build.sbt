ThisBuild / scalaVersion := "2.12.18"

javacOptions ++= Seq("-source", "11", "-target", "11")

lazy val root = (project in file("."))
  .settings(
    name := "snowflake-data-engineering-in-scala"
  )

libraryDependencies ++= Seq(
  "com.snowflake" % "snowpark" % "1.9.0",
  "ch.qos.logback" % "logback-classic" % "1.4.11"
)

inThisBuild(
  List(
    scalaVersion := "2.12.18",
    semanticdbEnabled := true,
    semanticdbVersion := scalafixSemanticdb.revision
  )
)
