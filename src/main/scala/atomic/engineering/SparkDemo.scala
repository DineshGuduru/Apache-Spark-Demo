package atomic.engineering

import org.apache.spark.sql.SparkSession

object SparkDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("AtomicEngineering")
      .getOrCreate()

    println("Printing Spark Session Variables:")
    println("App Name: " + spark.sparkContext.appName)
    println("Deployment Mode: " + spark.sparkContext.deployMode)
    println("Master: " + spark.sparkContext.master)

    val spark2 = SparkSession.builder()
      .master("local[1]")
      .appName("AtomicEngineering - New")
      .getOrCreate()

    println("Printing Spark Session 2 Variables:")
    println("App Name: " + spark2.sparkContext.appName)
    println("Deployment Mode: " + spark2.sparkContext.deployMode)
    println("Master: " + spark2.sparkContext.master)

    spark.stop()
  }
}