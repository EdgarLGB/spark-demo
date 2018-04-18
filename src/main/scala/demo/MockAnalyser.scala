package demo

import org.apache.spark.sql.{SparkSession}
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
    val sqlQuery = "SELECT source, timestamp, COUNT(*) as occurrence, 'The source attempted to request too many times in the same second' as message FROM logs GROUP BY source, timestamp HAVING occurrence >= %d ORDER BY occurrence DESC".format(occurrence)
    val queryDF = spark.sql(sqlQuery).coalesce(1)
    queryDF.write.json(outputPath)

  }
}
