package atomic.engineering

import org.apache.spark.sql.SparkSession

object WriteToCsv {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Write To Csv").master("local[1]").getOrCreate()

    import spark.implicits._

    val data = Seq(
      (16, "Alice", "20-09-2004"),
      (17, "Bob", "20-08-1998"),
      (18, "Charlie", "10-09-2001"),
      (19, "Danny", "02-09-1990")
    ).toDF("id", "name", "dob")

    data
      .write
      .option("header", value = true)
      .mode(saveMode = "ignore")
      .csv(path = "src/main/scala/atomic/target/csv_output")


    /*
    * Write Modes in Spark
    * overwrite
    * append
    * errorifexists or error : Default Mode
    * ignore
    * */

    val dataDF = spark.read.option("header", value = true).csv("src/main/scala/atomic/target/csv_output")

    dataDF.show()

    spark.stop()
  }

}
