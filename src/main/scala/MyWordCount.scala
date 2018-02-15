import org.apache.spark.{SparkConf, SparkContext}

/**
  * An app for counting the word
  */
object MyWordCount {

  def main(args: Array[String]): Unit = {
    val appName = args(0)
    val inputFilePath = args(1)
    val outputFilePath = args(2)
    val regex = args(3)
    val conf = new SparkConf().setAppName(appName)
    val spark = new SparkContext(conf)

    val inputRDD = spark.textFile(inputFilePath)
    val resultRDD = inputRDD.flatMap(_.split(" "))
      .filter(s => s.matches(regex))
      .map(s => (s, 1))
      .reduceByKey((i1, i2) => i1 + i2)
      .sortBy(pair => pair._2, false)

    resultRDD.saveAsTextFile(outputFilePath)
    println(s"$appName has written ${resultRDD.count()} result into the output file $outputFilePath")

  }
}
