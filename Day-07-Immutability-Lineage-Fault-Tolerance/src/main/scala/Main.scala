import org.apache.spark.{SparkConf, SparkContext}

object Main {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day 7 - Immutability, Lineage and Fault Tolerance")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    println("=== Day 7 - Immutability, Lineage and Fault Tolerance ===")

    // Original RDD
    val numbersRDD = sc.parallelize(
      List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    )

    println("\n=== Original RDD ===")
    println(s"Numbers: ${numbersRDD.collect().mkString(", ")}")

    // Step 1: map transformation
    val doubledRDD = numbersRDD.map(_ * 2)

    // Step 2: filter transformation
    val evenRDD = doubledRDD.filter(_ % 4 == 0)

    // Step 3: another map transformation
    val squaredRDD = evenRDD.map(n => n * n)

    println("\n=== Multi-Step Transformation Chain ===")
    println("Original RDD")
    println("     ↓ map(_ * 2)")
    println("Doubled RDD")
    println("     ↓ filter(_ % 4 == 0)")
    println("Even RDD")
    println("     ↓ map(_ * _)")
    println("Squared RDD")

    println("\nFinal Result:")
    println(squaredRDD.collect().mkString(", "))

    // Display RDD lineage
    println("\n=== RDD Lineage ===")
    println(squaredRDD.toDebugString)

    // Demonstrate immutability
    println("\n=== RDD Immutability ===")
    println(s"Original RDD: ${numbersRDD.collect().mkString(", ")}")
    println(s"Doubled RDD: ${doubledRDD.collect().mkString(", ")}")
    println("The original RDD remains unchanged after transformations.")

    // Fault tolerance concept
    println("\n=== Fault Tolerance ===")
    println("RDDs store lineage information instead of modifying existing data.")
    println("If a partition is lost, Spark can recompute it using the lineage.")
    println("Only the lost partition needs to be recomputed.")

    // Conceptual executor loss simulation
    println("\n=== Executor Loss Simulation ===")
    println("Assume the executor containing one squaredRDD partition is lost.")
    println("Spark identifies the lost partition.")
    println("Spark follows the lineage for that partition:")
    println("numbersRDD -> doubledRDD -> evenRDD -> squaredRDD")
    println("Spark recomputes only the lost partition.")

    println("\n=== Application Completed Successfully ===")

    sc.stop()
  }
}