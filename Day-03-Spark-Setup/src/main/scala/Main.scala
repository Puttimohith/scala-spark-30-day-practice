import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {

    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("Day 3 - Spark Setup")
      .master("local[2]")
      .getOrCreate()

    // Get SparkContext from SparkSession
    val sc = spark.sparkContext

    println("=== Spark Setup ===")
    println(s"Spark Version: ${spark.version}")
    println(s"Application Name: ${spark.sparkContext.appName}")
    println(s"Master: ${spark.sparkContext.master}")

    // Read the text file
    val lines = sc.textFile("data/input.txt")

    println("\n=== Input File Contents ===")
    lines.collect().foreach(println)

    println("\n=== SparkContext ===")
    println(s"SparkContext created successfully: ${sc != null}")

    // Stop Spark
    spark.stop()
  }
}
