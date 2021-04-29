import org.apache.spark.{SparkConf, sql}
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

    // 스키마 얻기2
    val df2 = spark.read.format("json")
      .load("data/flight-data/json/2015-summary.json").schema
    println(df2)
    /*
    StructType(StructField(DEST_COUNTRY_NAME,StringType,true),
    StructField(ORIGIN_COUNTRY_NAME,StringType,true),
    StructField(count,LongType,true))
    */

    val df3 = spark.read.format("json").load("data/flight-data/json/2015-summary.json").columns
    println(df3)
    /*
    [Ljava.lang.String;@69ab2d6a
     */

    // 첫번째 로우 반환
    val df4 = spark.read.format("json")
      .load("data/flight-data/json/2015-summary.json")
    println(df4.first())
    /*
    [United States,Romania,15]
     */

    // 로우 생성 및 접근
    import org.apache.spark.sql.Row
    val myRow = Row("Hello", null, 1, false)

    println(myRow(0)) // Any type
    println(myRow(0).asInstanceOf[String]) // String type
    println(myRow.getString(0)) // String type
    println(myRow.getInt(2)) // Int type


  }
}
