// This is a user defined function which takes in the string from application.conf file for schema and generates a StructType object
// which is retuened and can be passed as schema for dataframe

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType,TimestampType, StructField, StructType}

object user_def_func {

  def read_schema(schema_str:String)={
    var struct_template: StructType = new StructType
    var schema_str_space = schema_str.split(",").toList

    val data_types = Map(
      "StringType" -> StringType,
      "IntegerType" -> IntegerType,
      "DoubleType" ->DoubleType,
      "TimestampType" -> TimestampType
    )

    for(i <- schema_str_space){
      var fields = i.split(" ").toList
      struct_template = struct_template.add(fields(0),data_types(fields(1)),true)
    }

    struct_template
  }

}
