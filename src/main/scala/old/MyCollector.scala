package old

import org.apache.spark.{SparkConf, SparkContext}

/**
  * A simple collector which takes a log file and generates the output into hdfs
  */
object MyCollector {

  def main(args: Array[String]): Unit = {
    val appName = args(0)
    val inputFilePath = args(1)
    val outputFilePath = args(2)
    var sleepTime = "0"
    if (args.size >= 4) {
      sleepTime = args(3)
    }
    val conf = new SparkConf().setAppName(appName)
    val spark = new SparkContext(conf)
    val inputRDD = spark.textFile(inputFilePath)
    Thread.sleep(sleepTime.toLong * 1000)
    inputRDD.saveAsTextFile(outputFilePath)
  }
}
