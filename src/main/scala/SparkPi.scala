import org.apache.spark.sql.SparkSession

import scala.math.random

object SparkPi {

  def main(args: Array[String]): Unit = {
    if (args(0).isEmpty) {
      println("Please provide the name of app.")
      return
    }
    val appName = args(0)

    val spark = SparkSession
      .builder
      .appName(appName)
      .getOrCreate()

    var count = 0
    for (i <- 1 to 100000) {
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x * x + y * y <= 1) count += 1
    }
    println(s"\n----------Pi is roughly ${4 * count / 100000.0}-----------\n")
    spark.stop()
  }
}
