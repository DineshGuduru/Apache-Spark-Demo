package atomic.engineering

import org.apache.spark.sql.SparkSession

object SparkSql {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SPark SQL example").master("local[1]").getOrCreate()

    import spark.implicits._

    val data = Seq(
      (12, "Alice", "20-09-2004", 20),
      (13, "Bob", "20-08-1998", 26),
      (14, "Charlie", "10-09-2001", 23),
      (15, "Danny", "02-09-1990", 34),
      (16, "Danny", "02-09-1991", 33)
    ).toDF("id", "name", "dob", "age")

    data.show()

    data.printSchema()

    data.select("name").show()

    data.select($"name", $"age" + 1).show()

    data.filter($"age" > 22).show()

    data.groupBy("name").count().show()

    // Register the Dataframe as a SQL temporary VIEW
    data.createOrReplaceTempView("people")

    spark.sql("SELECT * FROM people").show()

    spark.sql("SELECT name from people").show()

    spark.sql("SELECT name, age + 1 from people").show()

    spark.sql("SELECT name, count(1) from people group by name").show()

    data.createOrReplaceGlobalTempView("people")

    spark.newSession().sql("SELECT * FROM global_temp.people").show()

    spark.stop()

  }

}
