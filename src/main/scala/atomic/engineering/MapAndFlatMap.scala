package atomic.engineering

import org.apache.spark.sql.SparkSession

object MapAndFlatMap {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Map and Flat Map").master("local[1]").getOrCreate()

    val nums = List(1,2,3,4)

    val doubledNums = nums.map(x => x + 2)

    println(doubledNums)

    val input = List("Hello World", "Atomic Engineering")

    val output = input.map(x => x.split(" ").toList)

    println(output)

    val flatmapOutput = input.flatMap(x => x.split(" "))

    println(flatmapOutput)

    spark.stop()
  }
}
