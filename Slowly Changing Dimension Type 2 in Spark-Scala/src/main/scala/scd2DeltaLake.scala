import org.apache.spark.sql.{SaveMode, SparkSession}
import io.delta.tables._
import org.apache.spark.sql.functions._

import java.util.Properties


object scd2DeltaLake extends App{

  val spark = SparkSession.builder()
    .appName("SCD 2 Delta Lake Approach")
    .config("spark.master","local")
    .getOrCreate()

  val url = "jdbc:mysql://localhost:3306/sparkpoc"
  val user = "root"
  val password = "admin"
  val targetTB = "sparkpoc.customer_target"

  val srcDF = spark.read
    .format("jdbc")
    .option("url",url)
    .option("user",user)
    .option("password",password)
    .option("dbtable","sparkpoc.customer_source")
    .load()

  val tgtDF = spark.read
    .format("jdbc")
    .option("url",url)
    .option("user",user)
    .option("password",password)
    .option("dbtable","sparkpoc.customer_target")
    .load()


  val wantedColumns = Seq("job","address")
  
  // Added 2 columns hash_value (unique value for a record) and version(to denote the version number of the record) for all the records in srcDF.
  //wantedColumns.map(col): _* --> here we are converting the strings from wantedColumns sequence to column objects and passing them to concat_ws function 
  // as individual arguments to generate a hash value.

  val srcWithVersionAndHash = srcDF.withColumn("hash_value",sha1(concat_ws("||",wantedColumns.map(col): _*))).withColumn("src_version",lit(0))


  tgtDF.write
    .format("delta")
    .mode("overwrite")
    .save("C:\\POC\\deltaFiles")

  val targetDelta: DeltaTable = DeltaTable.forPath("C:\\POC\\deltaFiles")

    val newInsertForUpdatedRows = srcWithVersionAndHash
      .as("updates")
      .join(targetDelta.toDF.as("target"), "id")
      .where("target.flag = 'N' AND target.hash_value <> updates.hash_value")
      .withColumn("src_version",col("version"))

      val stagedUpdates = newInsertForUpdatedRows
        .selectExpr("NULL as merge_key","updates.*","src_version")
        .union(
          srcWithVersionAndHash.as("updates").selectExpr("updates.id as merge_key","*")
        )
// merge() operation enables the scd2 functionality by inserting and updating records based on conditions.
        targetDelta
          .as("target")
          .merge(
            stagedUpdates.as("staged_updates"),
            "target.id = staged_updates.merge_key")
          .whenMatched("target.flag = 'N' AND target.hash_value <> staged_updates.hash_value")
          .update(Map(
            "flag" -> lit("Y"),
            "end_date" -> col("staged_updates.start_date") ))
          .whenNotMatched()
          .insert(Map(
            "id" -> col("staged_updates.id"),
            "job" -> col("staged_updates.job"),
            "address" -> col("staged_updates.address"),
            "flag" -> lit("N"),
            "version" -> expr("staged_updates.src_version + 1"),
            "hash_value" -> col("staged_updates.hash_value"),
            "start_date" -> col("staged_updates.start_date"),
            "end_date" -> lit("9999-12-31") ))
          .execute()

  val connectionProperties = new Properties()
  connectionProperties.put("user",user)
  connectionProperties.put("password",password)

  targetDelta.toDF.write.mode(SaveMode.Overwrite).jdbc(url,targetTB,connectionProperties)


}
