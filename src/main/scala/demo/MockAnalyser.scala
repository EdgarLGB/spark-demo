package demo

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Analyse the logs
  */
object MockAnalyser {

  def main(args: Array[String]): Unit = {
    val inputDirPath = args(0)
    val outputPath = args(1)
    val occurrence = args(2).toInt
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val inputRDDs = sc.wholeTextFiles(inputDirPath)

    val spark = SparkSession
      .builder()
      .appName(sc.appName)
      .getOrCreate()

    val df = MockParser.parse(spark, inputRDDs)

    // execute sql query
    df.createOrReplaceTempView("logs")
    val sqlQuery = "SELECT source, timestamp, COUNT(*) as occurrence FROM logs GROUP BY source, timestamp HAVING occurrence >= %d ORDER BY occurrence DESC".format(occurrence)
    val queryDF = spark.sql(sqlQuery)

    queryDF.show(10)

    // extract and construct the string
    import spark.implicits._
    val messages = queryDF.map(r => {
      val source = r.getString(0)
      val timestamp = r.getString(1)
      val num = r.getLong(2)
      "Source %s has %d requests in the moment %s".format(source, num, timestamp)
    }).collect()


    // recreate a dataframe with the generated message
    // convert array to df
    val rows = sc.parallelize(messages).map { x => Row(x) }.coalesce(1)
    val messageDF = spark.createDataFrame(rows, StructType(Seq(StructField("message", StringType, false))))

    messageDF.write.json(outputPath)
  }
}
