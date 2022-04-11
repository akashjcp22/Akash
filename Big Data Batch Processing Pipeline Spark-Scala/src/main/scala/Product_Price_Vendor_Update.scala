import org.apache.spark.sql.SparkSession
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions.{col, when}
import user_def_func._

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object Product_Price_Vendor_Update extends App{

  val spark = SparkSession.builder()
    .appName("Product_Price_Vendor_Update")
    .master("local[*]")
    .getOrCreate()

  val todayDate = LocalDate.now().format(DateTimeFormatter.ofPattern("ddMMyyyy"))
  val yesterdayDate = LocalDate.now().minusDays(1).format(DateTimeFormatter.ofPattern("ddMMyyyy"))

  val config: Config = ConfigFactory.load("application.conf")
  val afterLandingValidSchemaFile = config.getString("schema.afterLandingValidSchema")
  val afterLandingValidSchema = read_schema(afterLandingValidSchemaFile)
  val landingOutputFilePath = config.getString("paths.landingOutputFilePath")
  val productSchema = read_schema(config.getString("schema.productSchema"))
  val inputFilePath = config.getString("paths.inputFilePath")
  val usdSchema = read_schema(config.getString("schema.usdSchema"))
  val vendorsSchema = read_schema(config.getString("schema.vendorsSchema"))


  val afterLandingValidDF = spark.read
    .schema(afterLandingValidSchema)
    .option("header",true)
    .option("delimiter","|")
    .csv(landingOutputFilePath + "Valid/" + "ValidData_" + todayDate)

  val productDF = spark.read
    .schema(productSchema)
    .option("header",true)
    .option("delimiter","|")
    .csv(inputFilePath + "Products")


  afterLandingValidDF.createOrReplaceTempView("afterLandingValidDF")
  productDF.createOrReplaceTempView("productDF")

  val priceValidDF = spark.sql(
    """
      |select a.Sale_ID, a.Product_ID, b.Product_Name, a.Quantity_Sold, a.Vendor_ID, a.Sale_Date
      |, a.Quantity_Sold * b.Product_Price  as Sale_Amount, a.Sale_Currency
      |from afterLandingValidDF a
      |inner join productDF b
      |on a.Product_ID = b.Product_ID
      |""".stripMargin)


  val usdDF = spark.read
    .schema(usdSchema)
    .option("header",false)
    .option("delimiter","|")
    .csv(inputFilePath + "USD_Rates")



  priceValidDF.createOrReplaceTempView("priceValidDF")
  usdDF.createOrReplaceTempView("usdDF")

  val priceUsdValidDF = spark.sql(
    """
      |select a.Sale_ID, a.Product_ID, a.Product_Name, a.Vendor_ID, a.Quantity_Sold,a.Sale_Amount as Sale_Amount_Currency,
      |a.Sale_Currency, a.Sale_Amount / b.Exchange_Value as Sale_Amount_USD, a.Sale_Date
      |from priceValidDF a inner join usdDF b on a.Sale_Currency = b.Currency_Symbol
      |""".stripMargin)



  val vendorsDF = spark.read
    .schema(vendorsSchema)
    .option("header",false)
    .option("delimiter","|")
    .csv(inputFilePath + "Vendors")


  priceUsdValidDF.createOrReplaceTempView("priceUsdValidDF")
  vendorsDF.createOrReplaceTempView("vendorsDF")

  val finalDF = spark.sql(
    """
      |select a.Sale_ID, a.Product_ID, a.Product_Name, a.Vendor_ID, b.Vendor_Name, b.Vendor_Add_City,
      |b.Vendor_Add_State, b.Vendor_Add_Country, a.Quantity_Sold,a.Sale_Amount_Currency,
      |a.Sale_Currency, a.Sale_Amount_USD, a.Sale_Date
      |from priceUsdValidDF a inner join vendorsDF b on a.Vendor_ID = b.Vendor_ID
      |""".stripMargin)


  finalDF.write
    .mode("overwrite")
    .option("header",true)
    .option("delimiter","|")
    .csv(landingOutputFilePath + "Final")
  
  println("Final data is ready and written to Final folder")
}
