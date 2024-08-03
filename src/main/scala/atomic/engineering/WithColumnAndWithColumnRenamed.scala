package atomic.engineering

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col


object WithColumnAndWithColumnRenamed {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("WithColumnAndWithColumnRenamed")
      .master("local")
      .getOrCreate

    val data = Seq(
      ("James", 45),
      ("Alice", 12),
      ("Bob", 16)
    )

    val columns = Seq("name", "age")

    val df = spark.createDataFrame(data).toDF(columns: _*)

    df.show()

    val dfWithColumn = df.withColumn("is_adult", col("age") >= 18)

    dfWithColumn.show()

    val dfWithColumnRenamed = dfWithColumn.withColumnRenamed("name", "customer_name")

    dfWithColumnRenamed.show()

    spark.stop()
  }

}
