import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

import java.io.File
import java.util.Properties
import org.apache.commons.io.FileUtils

object egig_sls_oppty_hd_full_f_scd2 extends App{

  val spark = SparkSession.builder()
    .appName("EGIG_SLS_OPPTY_HD_FULL_F SCD2")
    .config("spark.master","local")
    .getOrCreate()

  import spark.implicits._

  // MySql DB connection details
  val url = "jdbc:mysql://localhost:3306/sparkpoc"
  val user = "root"
  val password = "admin"
  val parquetPath = "C:\\POC\\pocParquetFiles"
  val sourceTable = "sparkpoc.egig_sls_oppty_hd_full_f_src"
  val targetTable = "sparkpoc.egig_sls_oppty_hd_full_f_tgt"

  //Source Table as DataFrame
  val hd_full_f_src_DF = spark.read
    .format("jdbc")
    .option("url",url)
    .option("user",user)
    .option("password",password)
    .option("dbtable",sourceTable)
    .load()

 //Target Table as DataFrame
  val hd_full_f_tgt_DF = spark.read
    .format("jdbc")
    .option("url",url)
    .option("user",user)
    .option("password",password)
    .option("dbtable",targetTable)
    .load()


  // "SLS_OPPTY_SRC_OPPTY_SHT_UID" & "GEO_NM" & "OPPTY_CLOSE_CLDR_DT" are the primary keys
  // columns mentioned in wantedColumns Sequence are considered for genereating hash value.
  val wantedColumns = Seq("SLS_OPPTY_NM",
    "GEO_CD",
    "SLS_OPPTY_DN",
    "SLS_OPPTY_STAT_NM",
    "SLS_CHNL_RTE_NM",
    "SLS_OPPTY_FCST_CATG_NM",
    "OPPTY_WIN_OR_LOSS_FISC_YR_NM",
    "OPPTY_CLOSE_FISC_YR_QTR_DSPLY_CD",
    "TOV_USD_AM",
    "SLS_OPPTY_PIPELN_XCLSN_IND",
    "SRC_SYS_NM",
    "OTHR_PARTY_SITE_INSTNC_ID",
    "SLS_TTY_ID",
    "TOP_PRNT_SLS_TTY_ID")


// Generating additional columns like Inserter_Timestamp(the time when record is inserted into table, Hash_Value(a unique value for the record), 
// Version(denotes the version of record) 
  val src_insTime_hash_version_DF = hd_full_f_src_DF
    .withColumn("INSERTED_TIMESTAMP",current_timestamp())
    .withColumn("HASH_VALUE",sha1(concat_ws("||",wantedColumns.map(col): _*)))
    .withColumn("VERSION",lit(1))

  // All the records which are already historical
  val allHistoryData = hd_full_f_tgt_DF.filter(col("FLAG") === "Y")

// The records which are completely new and which have received updates
  val newAndUpdatedRows = src_insTime_hash_version_DF.as("left")
    .join(hd_full_f_tgt_DF.as("right").filter($"right.FLAG" === "N")
      .withColumnRenamed("SLS_OPPTY_SRC_OPPTY_SHT_UID","TGT_ID")
      .withColumnRenamed("VERSION","TGT_VERSION"),
      $"left.SLS_OPPTY_SRC_OPPTY_SHT_UID" === $"TGT_ID" && $"left.GEO_NM" === $"right.GEO_NM" && $"left.OPPTY_CLOSE_CLDR_DT" === $"right.OPPTY_CLOSE_CLDR_DT", "left")
    .where($"TGT_ID".isNull || $"left.HASH_VALUE" =!= $"right.HASH_VALUE")
    .withColumn("VERSION",when($"TGT_ID".isNotNull,expr("TGT_VERSION + 1")).otherwise($"VERSION"))
    .select("left.*","VERSION")

// Adding Flag('N' - for latest , 'Y' for all historical) and Updated_Timestamp(Denotes the timestamp when existing record got updated)
  val newAndUpdatedRowsAll = newAndUpdatedRows.withColumn("FLAG",lit("N"))
    .withColumn("UPDATED_TIMESTAMP",lit(null).cast(TimestampType))

// Updating Flag and Updated_Timestamp of records in target which have received updates
  val updatingFlagAndEndDate = hd_full_f_tgt_DF.as("left").filter($"left.FLAG" === "N")
    .join(src_insTime_hash_version_DF.as("right"),
      $"left.SLS_OPPTY_SRC_OPPTY_SHT_UID" === $"right.SLS_OPPTY_SRC_OPPTY_SHT_UID" && $"left.GEO_NM" === $"right.GEO_NM" && $"left.OPPTY_CLOSE_CLDR_DT" === $"right.OPPTY_CLOSE_CLDR_DT","inner")
    .where($"left.HASH_VALUE" =!= $"right.HASH_VALUE")
    .withColumn("FLAG",lit("Y"))
    .withColumn("UPDATED_TIMESTAMP",$"right.INSERTED_TIMESTAMP")
    .select("left.*","FLAG","UPDATED_TIMESTAMP")


// The rows already in target table which are latest and didnt receive any updates
  val remainingRows = hd_full_f_tgt_DF.as("left").filter($"left.FLAG" === "N")
    .join(src_insTime_hash_version_DF.as("right"),
      $"left.SLS_OPPTY_SRC_OPPTY_SHT_UID" === $"right.SLS_OPPTY_SRC_OPPTY_SHT_UID" && $"left.GEO_NM" === $"right.GEO_NM" && $"left.OPPTY_CLOSE_CLDR_DT" === $"right.OPPTY_CLOSE_CLDR_DT", "left")
    .where($"right.SLS_OPPTY_SRC_OPPTY_SHT_UID".isNull || $"left.HASH_VALUE" === $"right.HASH_VALUE")
    .select($"left.*")


// Final DataFram which will have all the new , updated, latest existing and historical data without any changes.
  val finalDF = allHistoryData
    .union(updatingFlagAndEndDate)
    .union(newAndUpdatedRowsAll)
    .union(remainingRows)



  val connectionProperties = new Properties()
  connectionProperties.put("user",user)
  connectionProperties.put("password",password)


  FileUtils.deleteQuietly(new File(parquetPath))

  finalDF.write.parquet(parquetPath)

  val parqueDF = spark.read.parquet(parquetPath)

  parqueDF.write.mode(SaveMode.Overwrite).jdbc(url,targetTable,connectionProperties)



}
