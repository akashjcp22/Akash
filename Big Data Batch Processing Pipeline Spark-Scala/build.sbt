name := "Retail_Shop_Project_Batch_Pipeline"

version := "0.1"

scalaVersion := "2.12.10"


val sparkVersion = "3.1.1"
val typesafeVersion = "1.2.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.typesafe" % "config" % typesafeVersion
)
