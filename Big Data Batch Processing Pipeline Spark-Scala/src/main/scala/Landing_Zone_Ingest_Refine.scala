import org.apache.spark.sql.SparkSession
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions.{col, when}
import user_def_func._

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object Landing_Zone_Ingest_Refine extends App{

  val spark = SparkSession.builder()
    .appName("Landing_Zone_Ingest_Refine")
    .master("local[*]")
    .getOrCreate()
  

  val config: Config = ConfigFactory.load("application.conf")
  val landingFilePath = config.getString("paths.inputFilePath")
  val landingOutputFilePath = config.getString("paths.landingOutputFilePath")
  val landingSchemaFile = config.getString("schema.landingSchema")
  val landingSchema = read_schema(landingSchemaFile)
  val landingHoldSchemaFile = config.getString("schema.landingHoldSchema")
  val landingHoldSchema = read_schema(landingHoldSchemaFile)

  //Generate date dynamically
  val todayDate = LocalDate.now().format(DateTimeFormatter.ofPattern("ddMMyyyy"))
  val yesterdayDate = LocalDate.now().minusDays(1).format(DateTimeFormatter.ofPattern("ddMMyyyy"))


  val landingDF = spark.read
    .schema(landingSchema)
    .option("delimiter","|")
    .csv(landingFilePath+"Sales_Landing/" + "SalesDump_" + todayDate)

  val yesterdayHoldDF = spark.read
    .schema(landingHoldSchema)
    .option("delimiter","|")
    .option("header",true)
    .csv(landingOutputFilePath + "Hold/" + "HoldData_" + yesterdayDate)

  landingDF.createOrReplaceTempView("landingDF")
  yesterdayHoldDF.createOrReplaceTempView("yesterdayHoldDF")

  val combinedLandingDF = spark.sql(
    """ select a.Sale_ID, a.Product_ID,
      case when a.Quantity_Sold is null then b.Quantity_Sold else a.Quantity_Sold end as Quantity_Sold,
      case when a.Vendor_ID is null then b.Vendor_ID else a.Vendor_ID end as Vendor_ID,
      a.Sale_Date,a.Sale_Amount,a.Sale_Currency
      from landingDF a
      left outer join yesterdayHoldDF b
      on a.Sale_ID = b.Sale_ID""")


  val invalidCombinedLandingDF = combinedLandingDF.filter(col("Quantity_Sold").isNull || col("Vendor_ID").isNull)
    .withColumn("Hold_Reason",when(col("Quantity_Sold").isNull,"Qty sold is null")
    .otherwise(when(col("Vendor_ID").isNull,"Vendor_ID is null")))

  val previousHoldNotReleasedDF = spark.sql(
    """
      |select * from yesterdayHoldDF
      |where Sale_ID not in (select Sale_ID from landingDF)
      |""".stripMargin)

  val invalidLandingDF = invalidCombinedLandingDF.union(previousHoldNotReleasedDF)
  val validLandingDF = combinedLandingDF.filter(col("Quantity_Sold").isNotNull && col("Vendor_ID").isNotNull)


  validLandingDF.write
    .mode("overwrite")
    .option("delimiter","|")
    .option("header",true)
    .csv(landingOutputFilePath + "Valid/" + "ValidData_" + todayDate)

  invalidLandingDF.write
    .mode("overwrite")
    .option("delimiter","|")
    .option("header",true)
    .csv(landingOutputFilePath + "Hold/" + "HoldData_" + todayDate)

  println("Data written out to hold and valid directories")
}
