package demo

import org.apache.spark.{SparkConf, SparkContext}

import scalaj.http.Http

/**
  * Created by bobo on 16/04/18.
  */
object MockWriter {

  def main(args: Array[String]): Unit = {
    val inputDirPath = "/home/bobo/Downloads/output"
    val esURL = "http://34.240.106.68:9200"
    val indexName = "index1"
    val typeName = "type1"
    val conf = new SparkConf().setMaster("local").setAppName("Writer1")
    val spark = new SparkContext(conf)
    val inputRDDs = spark.wholeTextFiles(inputDirPath)

    val req = Http(esURL + "/" + indexName + "/" + typeName).method("POST").header("Content-Type", "application/json")
    inputRDDs.map(_._2).flatMap(_.split("\n")).foreach(s => {
      req.postData(s).asString
    })
  }
}
