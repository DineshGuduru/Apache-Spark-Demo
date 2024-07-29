package atomic.engineering

import org.apache.spark.sql.SparkSession

object ParquetFiles {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Parquet Files").master("local[1]").getOrCreate()

    import spark.implicits._

    val file_list = List("src/main/scala/atomic/data/parquet_files/id_1_to_4.parquet", "src/main/scala/atomic/data/parquet_files/id_5_to_8.parquet")

    val dir_path = "src/main/scala/atomic/data/parquet_files/*.parquet"

    val df = spark.read.parquet(dir_path) // vararg

    val filtered_data = df.filter($"age" < 30)

    filtered_data.show()

    /*
    * Write Modes in Spark
    * overwrite
    * append
    * errorifexists or error : Default Mode
    * ignore
    * */

    filtered_data.write.mode("overwrite").option("compression", "gzip").parquet("src/main/scala/atomic/target/parquet_output")

    spark.stop()
  }

}
