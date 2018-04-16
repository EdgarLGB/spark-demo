package demo

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by bobo on 16/04/18.
  */
object MockCollector {
  
  def main(args: Array[String]): Unit = {
    val inputDirPath = args(0)
    val outputFilePath = args(1)
    val conf = new SparkConf()
    val spark = new SparkContext(conf)
    val inputRDDs = spark.wholeTextFiles(inputDirPath)
    inputRDDs.map(_._2).flatMap(_.split("\n")).coalesce(1).saveAsTextFile(outputFilePath)
  }

}
