import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Main {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day 10 - Partitioning")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    println("=== Day 10 - Partitioning ===")

    // --------------------------------------------------
    // 1. Inspect Partition Counts
    // --------------------------------------------------

    val numbersRDD = sc.parallelize(
      1 to 10,
      2
    )

    println("\n=== Initial Partition Count ===")
    println(s"Initial partitions: ${numbersRDD.getNumPartitions}")

    numbersRDD
      .mapPartitionsWithIndex {
        case (partitionId, values) =>
          Iterator(
            s"Partition $partitionId: ${values.mkString(", ")}"
          )
      }
      .collect()
      .foreach(println)

    // --------------------------------------------------
    // 2. repartition
    // --------------------------------------------------

    val repartitionedRDD = numbersRDD.repartition(4)

    println("\n=== repartition(4) ===")
    println(
      s"Partitions after repartition: ${repartitionedRDD.getNumPartitions}"
    )

    repartitionedRDD
      .mapPartitionsWithIndex {
        case (partitionId, values) =>
          Iterator(
            s"Partition $partitionId: ${values.mkString(", ")}"
          )
      }
      .collect()
      .foreach(println)

    // --------------------------------------------------
    // 3. coalesce
    // --------------------------------------------------

    val coalescedRDD = repartitionedRDD.coalesce(2)

    println("\n=== coalesce(2) ===")
    println(
      s"Partitions after coalesce: ${coalescedRDD.getNumPartitions}"
    )

    coalescedRDD
      .mapPartitionsWithIndex {
        case (partitionId, values) =>
          Iterator(
            s"Partition $partitionId: ${values.mkString(", ")}"
          )
      }
      .collect()
      .foreach(println)

    // --------------------------------------------------
    // 4. When Increasing and Decreasing Partitions Helps
    // --------------------------------------------------

    println("\n=== Partitioning Guidelines ===")

    println("Increase partitions when:")
    println("- The dataset has too few partitions.")
    println("- Tasks are processing too much data.")
    println("- More parallelism is needed.")
    println("- Cluster resources are not fully utilized.")

    println("\nDecrease partitions when:")
    println("- The dataset has too many small partitions.")
    println("- Task scheduling overhead is high.")
    println("- The workload does not need high parallelism.")

    println("\nrepartition() can increase or decrease partitions.")
    println("repartition() causes a shuffle.")

    println("coalesce() is mainly used to decrease partitions.")
    println("coalesce() can reduce partitions with less data movement.")

    // --------------------------------------------------
    // 5. partitionBy on Pair RDD
    // --------------------------------------------------

    println("\n=== partitionBy on Pair RDD ===")

    val salesRDD = sc.parallelize(
      List(
        ("Electronics", 1000),
        ("Furniture", 2000),
        ("Electronics", 1500),
        ("Clothing", 800),
        ("Furniture", 1200),
        ("Clothing", 700),
        ("Electronics", 900),
        ("Furniture", 1800)
      ),
      2
    )

    println(
      s"Pair RDD partitions before partitionBy: ${salesRDD.getNumPartitions}"
    )

    val partitionedSalesRDD = salesRDD.partitionBy(
      new HashPartitioner(4)
    )

    println(
      s"Pair RDD partitions after partitionBy: ${partitionedSalesRDD.getNumPartitions}"
    )

    partitionedSalesRDD
      .mapPartitionsWithIndex {
        case (partitionId, values) =>
          Iterator(
            s"Partition $partitionId: ${values.mkString(", ")}"
          )
      }
      .collect()
      .foreach(println)

    // --------------------------------------------------
    // 6. Scenario - Dataset with Too Few Partitions
    // --------------------------------------------------

    println("\n=== Too Few Partitions Scenario ===")

    val smallPartitionRDD = sc.parallelize(
      1 to 20,
      1
    )

    println(
      s"Original partitions: ${smallPartitionRDD.getNumPartitions}"
    )

    println("Problem: Too much data is handled by one partition.")

    val optimizedRDD = smallPartitionRDD.repartition(4)

    println(
      s"Optimized partitions: ${optimizedRDD.getNumPartitions}"
    )

    println(
      "Increasing partitions allows the workload to be processed in parallel."
    )

    optimizedRDD
      .mapPartitionsWithIndex {
        case (partitionId, values) =>
          Iterator(
            s"Optimized Partition $partitionId: ${values.mkString(", ")}"
          )
      }
      .collect()
      .foreach(println)

    // --------------------------------------------------
    // 7. Summary
    // --------------------------------------------------

    println("\n=== Day 10 Concepts ===")

    println("Partition -> A logical division of an RDD's data.")
    println("repartition -> Changes partitions using a shuffle.")
    println("coalesce -> Reduces partitions with less data movement.")
    println("partitionBy -> Partitions Pair RDD data using a partitioner.")
    println("Too few partitions -> Increase partitions for more parallelism.")

    println("\n=== Application Completed Successfully ===")

    sc.stop()
  }
}