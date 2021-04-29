import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object Ch5_2 {
  def main(args: Array[String]): Unit = {
    /*
    DataFrame을 다루는 방법
    - 로우나 컬럼 추가
    - 로우나 컬럼 제거
    - 로우를 컬럼으로 변환, 또는 반대
    - 컬럼값을 기준으로 로우 순서 변경
     */

    val conf = new SparkConf()
      .setAppName("SELECT RDBMS DATA")
      .setMaster("local")
    val spark = SparkSession.builder.config(conf).getOrCreate()

    // 외부스키마로 DataFrame 생성하기
    val df = spark.read.format("json").load("data/flight-data/json/2015-summary.json")
    df.createOrReplaceTempView("dfTable")
    df.show()

    // 직접구현한스키마로 DataFrame 생성하기
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}

    val myManualSchema = new StructType(Array(
      new StructField("some", StringType,true),
      new StructField("col", StringType, true),
      new StructField("names", LongType, false)
    ))

    val myRows = Seq(Row("Hello",null, 1L))
    val myRDD = spark.sparkContext.parallelize(myRows)
    val myDf = spark.createDataFrame(myRDD, myManualSchema)
    myDf.show()

    // SQL처럼 SELECT로 DataFrame 컨트롤
    import org.apache.spark.sql.functions.{expr, col, column}
    df.select(
      df.col("DEST_COUNTRY_NAME"),
      col("DEST_COUNTRY_NAME"),
      column("DEST_COUNTRY_NAME"),
      expr("DEST_COUNTRY_NAME as destination")
    ).show(3)

    df.selectExpr(
      "DEST_COUNTRY_NAME as destination",
      "*"
    ).show(2)

    df.selectExpr(
      "avg(count)",
      "count(distinct(DEST_COUNTRY_NAME))"
    ).show(4)

    //컬럼추가하기
    import org.apache.spark.sql.functions.lit
    df.withColumn("numberOne", lit(1)).show(2)

    // 도착지와 출발지가 같은지 비교 불리언 , withColumn메소드는 두개 인자(컬럼명, 생성표현식) 사용
    df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME==DEST_COUNTRY_NAME")).show(2)

    //컬럼명 변경
    df.withColumn("OGN",col("ORIGIN_COUNTRY_NAME")).show(3)

    //예약문자와 키워드
    val dfWithLongColName = df.withColumn(
     "This Long Column-name",
     expr("ORIGIN_COUNTRY_NAME")
    )
    dfWithLongColName.selectExpr(
      "`This Long Column-Name`",
      "`This Long COlumn-Name` as `new col`"
    ).show(3)
    dfWithLongColName.createOrReplaceTempView("dfTableLong")

    //컬럼 제거하기 (단일, 다수)
    df.drop("ORIGIN_COUNTRY_NAME")
    dfWithLongColName.drop("ORIGIN_COUNTRY_NAME","DEST_COUNTRY_NAME")

    //컬럼 데이터 타입 변경
    df.withColumn("count2", col("count").cast("string"))

    //로우 필터링하기 (where, filter)
    df.where("count < 200").show()
    //다수의 필터링, 순차적이 아니다
    df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") =!= "Croatia").show()

    //고유한 로우 얻기
    df.select("ORIGIN_COUNTRY_NAME","DEST_COUNTRY_NAME").distinct().count()

  }
}
