package atomic.engineering

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object SelectAndDrop {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("Select and Drop").master("local[*]").getOrCreate()

    val data = Seq(
      (1, "John Doe", 30, "HR", 50000.0, "2020-01-15"),
      (2, "Jane Smith", 28, "Engineering", 75000.0, "2019-03-20"),
      (3, "Bob Johnson", 35, "Sales", 65000.0, "2018-11-10"),
      (4, "Alice Brown", 32, "Marketing", 55000.0, "2021-05-01"),
      (5, "Charlie Wilson", 40, "Engineering", 80000.0, "2017-09-12")
    )

    val columns = List("id", "name", "age", "department", "salary", "join_date")

    val employee_df = spark.createDataFrame(data).toDF(columns: _*)

    employee_df.show()

    val employee_id_and_name_df = employee_df.select(col("id"), col("name"))

    employee_id_and_name_df.show()

    employee_df.show()

    val employee_age_next_next_year_df = employee_df.select(col("id"), col("name"), (col("age") + 1).as("next_year_age"))

    employee_age_next_next_year_df.show()

    val employee_df_without_salary = employee_df.drop(col("salary"), col("department"))

    employee_df_without_salary.show()

    val select_and_drop_df = employee_df.select(col("id"), col("name"), col("department"), (col("age") + 1).as("next_year_age"), col("age"))
      .drop(col("age"))

    select_and_drop_df.show()

    spark.stop()

  }

}
