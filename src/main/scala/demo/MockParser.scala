package demo

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.functions.regexp_extract
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex

/**
  * A parser to convert data to json format
  */
object MockParser {

  def main(args: Array[String]): Unit = {

    val inputPath = args(0)
    val outputPath = args(1)
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val log_file = sc.wholeTextFiles(inputPath)
    val spark = SparkSession
      .builder()
      .appName(sc.appName)
      .getOrCreate()
    val split_df = parse(spark, log_file)
    
    split_df.write.json(outputPath)
  }

  def parse(spark: SparkSession, rdd: RDD[(String, String)]) = {
    // regular expression patterns
    val reg_ex_host =
      """^([^\s]+\s)""".r
    val reg_ex_timestamp ="""^.*\[(\d\d/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]""".r
    val reg_ex_path ="""^.*"\w+\s+([^\s]+)\s+HTTP.*"""".r
    val reg_ex_status ="""^.*"\s+([^\s]+)""".r
    val reg_ex_content_size ="""^.*\s+(\d+)$""".r
    
    import spark.implicits._
    val df = rdd.map(_._2).flatMap(_.split("\n")).toDF()

    val split_df = df.select(regexp_extract(df("value"), reg_ex_host.toString(), 1) as ("source"), regexp_extract(df("value"), reg_ex_timestamp.toString(), 1) as ("timestamp"), regexp_extract(df("value"), reg_ex_path.toString(), 1) as ("path"), regexp_extract(df("value"), reg_ex_status.toString(), 1) as ("status"), regexp_extract(df("value"), reg_ex_content_size.toString(), 1) as ("content_size"))
    split_df
  }
}