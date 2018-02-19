import org.apache.spark.{SparkConf, SparkContext}

/**
  * An app for filtering the logs
  */
object MyFilter {

  def main(args: Array[String]): Unit = {

    val appName = args(0)
    val inputDicPath = args(1)
    val outputFilePath = args(2)
    val regex = args(3)
    var sleepTime = "0"
    if (args.size >= 5) {
      sleepTime = args(4)
    }
    val conf = new SparkConf().setAppName(appName)
    val spark = new SparkContext(conf)
    val logRDD = spark.wholeTextFiles(inputDicPath)
    Thread.sleep(sleepTime.toLong * 1000)
    val result = logRDD.map(_._2).flatMap(_.split("\n")).filter(l => l.matches(regex))
    result.coalesce(1).saveAsTextFile(outputFilePath)
    println(s"$appName has written ${result.count()} result into the output file $outputFilePath")

  }
}
