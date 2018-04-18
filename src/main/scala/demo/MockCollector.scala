package demo

import java.util.UUID

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Collect data and filter the valid data
  */
object MockCollector {

  val regex = "^([^\\s]+\\s).*\\[(\\d\\d\\/\\w{3}\\/\\d{4}:\\d{2}:\\d{2}:\\d{2} -\\d{4})].*\"\\w+\\s+([^\\s]+)\\s+HTTP.*\"\\s+([^\\s]+)\\s+(\\d+)$"

  def main(args: Array[String]): Unit = {
    val inputDirPath = args(0)
    val outputFilePath = args(1)
    val errorFilePath = args(2)
    val conf = new SparkConf()
    val spark = new SparkContext(conf)
    val inputRDDs = spark.wholeTextFiles(inputDirPath)

    val flatRDD = inputRDDs.map(_._2).flatMap(_.split("\n"))
    val validRDD = flatRDD.filter(_.matches(regex))
    validRDD.coalesce(1).saveAsTextFile(outputFilePath)

    // send back the invalid data
    val invalidRDD = flatRDD.filter(!_.matches(regex))
    if (!flatRDD.isEmpty()) {
      invalidRDD.coalesce(1).saveAsTextFile(errorFilePath + "/invalid-" + UUID.randomUUID().toString)
    }
  }

}
