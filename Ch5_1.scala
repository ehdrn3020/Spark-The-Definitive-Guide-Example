import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/*
  구조적 API 기본 연산
 */

object Ch5_1 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("SELECT RDBMS DATA")
      .setMaster("local")
    val spark = SparkSession.builder.config(conf).getOrCreate()

    // 스키마 얻기
    val df = spark.read.format("json")
      .load("data/flight-data/json/2015-summary.json")
    df.printSchema()
    /*
    root
    |-- DEST_COUNTRY_NAME: string (nullable = true)
    |-- ORIGIN_COUNTRY_NAME: string (nullable = true)
    |-- count: long (nullable = true)
     */



    val df2 = spark.read.format("json")
      .load("data/flight-data/json/2015-summary.json").schema
    println(df2)
    /*
    StructType(StructField(DEST_COUNTRY_NAME,StringType,true),
    StructField(ORIGIN_COUNTRY_NAME,StringType,true),
    StructField(count,LongType,true))
    */

    

  }
}
