package demo

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark

/**
  * Write json to ES
  */
object MockWriter {

  def main(args: Array[String]): Unit = {
    val inputDirPath = args(0)
    val esURL = args(1)
    val indexName = args(2)
    val typeName = args(3)
    printf("inputPath: %s \n esURL: %s \n indexName: %s \n typeName: %s ", inputDirPath, esURL, indexName, typeName)

    val conf = new SparkConf()
    conf.set("es.index.auto.create", "true")
      .set("es.nodes", esURL)
    val sc = new SparkContext(conf)
    val inputRDDs = sc.wholeTextFiles(inputDirPath)

    val resultRDD = inputRDDs.map(_._2).flatMap(_.split("\n"))

    try {
      EsSpark.saveJsonToEs(resultRDD, indexName + "/" + typeName)
    } catch {
      case e: Throwable => println(e)
    }
  }
}
