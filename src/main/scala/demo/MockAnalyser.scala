package demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by bobo on 16/04/18.
  */
object MockAnalyser {

  def main(args: Array[String]): Unit = {
    val inputDirPath = "/home/bobo/Downloads/access_log_Aug95"
    val outputPath = ""
    val occurence = "10"
    val conf = new SparkConf().setMaster("local").setAppName("MockAnalyser")
    val sc = new SparkContext(conf)
    val inputRDDs = sc.wholeTextFiles(inputDirPath)

    val spark = SparkSession
      .builder()
      .appName(sc.appName)
      .getOrCreate()

    val df = MockParser.parse(spark, inputRDDs)

    // For implicit conversions like converting RDDs to DataFrames
    df.createOrReplaceTempView("logs")
    val sqlQuery = "SELECT source, timestamp, count(*) FROM logs WHERE status == 403 GROUP BY source, timestamp"
    val resultDF = spark.sql(sqlQuery)
    resultDF.show(10)
    
  }
}
