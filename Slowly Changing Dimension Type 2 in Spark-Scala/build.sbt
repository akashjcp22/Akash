name := "SparkPOC"

version := "0.1"

scalaVersion := "2.12.10"


val sparkVersion = "3.1.1"
val postgresVersion = "42.2.2"
val mysqlVersion = "8.0.17"
val deltaVersion = "1.0.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.postgresql" % "postgresql" % postgresVersion,
  "mysql" % "mysql-connector-java" % mysqlVersion,
  "io.delta" %% "delta-core" % deltaVersion
)

