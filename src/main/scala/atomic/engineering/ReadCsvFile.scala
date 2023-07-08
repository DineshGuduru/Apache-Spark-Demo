package atomic.engineering

import org.apache.spark.sql.SparkSession

object ReadCsvFile {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Read Csv File").master("local[1]").getOrCreate()

    val file_paths = List("/Users/dineshvarmaguduru/IdeaProjects/Apache-Spark-Demo/src/main/scala/atomic/engineering/csv_files/data.csv",
      "/Users/dineshvarmaguduru/IdeaProjects/Apache-Spark-Demo/src/main/scala/atomic/engineering/csv_files/data1.csv")

    val folder = "/Users/dineshvarmaguduru/IdeaProjects/Apache-Spark-Demo/src/main/scala/atomic/engineering/csv_files"

    val options = Map("header" -> "true",
                      "delimiter" -> "|",
                      "lineSep" -> "\n",
                      "maxColumns" -> "4")
    /*
    Read Multiple Files
    val csvDF = spark.read
      .options(options)
      .csv(paths = file_paths:_*)
    */

    val csvDF = spark.read
      .options(options)
      .csv(path = folder)

    csvDF.printSchema()

    csvDF.show()

    spark.stop()
  }

}
