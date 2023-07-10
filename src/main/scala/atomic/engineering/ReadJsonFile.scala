package atomic.engineering

import org.apache.spark.sql.SparkSession

object ReadJsonFile {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Working with JSON").master("local[1]").getOrCreate()

    // Reading all JSON Files in a Folder
    val jsonDF = spark.read.json("src/main/scala/atomic/data/jsonl_files/*.json")

    /*
    * Reading Multi Line JSON
    * val jsonDF = spark.read.option("multiline", value = true).json("src/main/scala/atomic/data/json_files/multiline_data.json")
    *
    * Reading Single JSON File
    * val jsonDF = spark.read.json("src/main/scala/atomic/data/jsonl_files/data.json")
    *
    * Reading Multiple JSON Files
    * val jsonDF = spark.read.json(List("src/main/scala/atomic/data/jsonl_files/data.json", "src/main/scala/atomic/data/jsonl_files/data1.json"):_*)
    * */

    jsonDF.show()

    spark.stop()
  }

}
