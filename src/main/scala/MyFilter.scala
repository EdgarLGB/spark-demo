import org.apache.spark.{SparkConf, SparkContext}

/**
  * An app for filtering the words
  */
object MyFilter {

  def main(args: Array[String]): Unit = {

    val appName = args(0)
    val inputDicPath = args(1)
    val outputFilePath = args(2)
    val regex = args(3)
    val conf = new SparkConf().setAppName(appName)
    val spark = new SparkContext(conf)

    val logRDD = spark.wholeTextFiles(inputDicPath)
    val result = logRDD.map(p => p._2).filter(l => l.matches(regex))
    result.coalesce(1).saveAsTextFile(outputFilePath)
    println(s"$appName has written ${result.count()} result into the output file $outputFilePath")

  }
}
