import org.apache.spark.{SparkConf, SparkContext}

object Main {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day 8 - DAG and Spark Execution")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    println("=== Day 8 - DAG and Spark Execution ===")

    // Create the input RDD
    val numbersRDD = sc.parallelize(
      List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    )

    println("\n=== Input RDD ===")
    println(s"Numbers: ${numbersRDD.collect().mkString(", ")}")
    println(s"Partitions: ${numbersRDD.getNumPartitions}")

    // Narrow transformation 1
    val doubledRDD = numbersRDD.map(n => n * 2)

    // Narrow transformation 2
    val filteredRDD = doubledRDD.filter(n => n % 4 == 0)

    // Wide transformation - causes shuffle
    val pairRDD = filteredRDD.map(n => (n % 8, n))

    val reducedRDD = pairRDD.reduceByKey(_ + _)

    // Another narrow transformation after shuffle
    val finalRDD = reducedRDD.map {
      case (key, value) => (key, value * 10)
    }

    println("\n=== Transformation Pipeline ===")
    println("numbersRDD")
    println("    ↓ map")
    println("doubledRDD")
    println("    ↓ filter")
    println("filteredRDD")
    println("    ↓ map to (key, value)")
    println("pairRDD")
    println("    ↓ reduceByKey  ← SHUFFLE BOUNDARY")
    println("reducedRDD")
    println("    ↓ map")
    println("finalRDD")

    println("\n=== Final Result ===")
    println(
      finalRDD
        .collect()
        .sortBy(_._1)
        .map {
          case (key, value) => s"$key -> $value"
        }
        .mkString(", ")
    )

    println("\n=== RDD Lineage / DAG ===")
    println(finalRDD.toDebugString)

    println("\n=== Jobs, Stages, Tasks and Partitions ===")
    println("Job      -> Created when an action is called.")
    println("Stage    -> A group of tasks separated by shuffle boundaries.")
    println("Task     -> Work performed on one partition.")
    println("Partition-> A division of an RDD's data.")

    println("\n=== Narrow vs Wide Transformations ===")
    println("Narrow transformations:")
    println("map, filter")
    println("Each output partition depends on a small number of input partitions.")

    println("\nWide transformations:")
    println("reduceByKey")
    println("Data is shuffled across partitions.")

    println("\n=== Shuffle Boundary ===")
    println("reduceByKey creates a shuffle boundary.")
    println("Transformations before reduceByKey belong to the first stage.")
    println("Transformations after reduceByKey belong to the next stage.")

    println("\n=== Stage Prediction ===")
    println("Pipeline: map -> filter -> map -> reduceByKey -> map -> collect")
    println("Predicted stages for the final action: 2")
    println("Stage 0: map -> filter -> map")
    println("Shuffle: reduceByKey")
    println("Stage 1: reduceByKey -> map -> collect")

    println("\n=== Multiple Actions ===")

    val countResult = finalRDD.count()
    println(s"count(): $countResult")

    val firstResult = finalRDD.first()
    println(s"first(): $firstResult")

    println("\nEach action creates a separate Spark job.")

    println("\n=== Application Completed Successfully ===")

    sc.stop()
  }
}