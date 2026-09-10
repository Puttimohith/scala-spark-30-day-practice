import org.apache.spark.{SparkConf, SparkContext}

object Main {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day 5 - Transformations and Actions")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    println("=== Day 5 - Transformations and Actions ===")

    // --------------------------------------------------
    // 1. RDD Transformations
    // --------------------------------------------------

    val numbersRDD = sc.parallelize(List(1, 2, 3, 4, 5))

    // map
    val mappedRDD = numbersRDD.map(n => n * 2)

    println("\n=== map() ===")
    println(s"Result: ${mappedRDD.collect().mkString(", ")}")

    // filter
    val filteredRDD = numbersRDD.filter(n => n % 2 == 0)

    println("\n=== filter() ===")
    println(s"Result: ${filteredRDD.collect().mkString(", ")}")

    // flatMap
    val sentencesRDD = sc.parallelize(
      List(
        "Scala Spark",
        "RDD transformations",
        "Big Data"
      )
    )

    val flatMappedRDD = sentencesRDD.flatMap(_.split(" "))

    println("\n=== flatMap() ===")
    println(s"Result: ${flatMappedRDD.collect().mkString(", ")}")

    // distinct
    val duplicateRDD = sc.parallelize(
      List(1, 2, 2, 3, 3, 3, 4, 5, 5)
    )

    val distinctRDD = duplicateRDD.distinct()

    println("\n=== distinct() ===")
    println(s"Result: ${distinctRDD.collect().sorted.mkString(", ")}")

    // union
    val firstRDD = sc.parallelize(List(1, 2, 3))
    val secondRDD = sc.parallelize(List(4, 5, 6))

    val unionRDD = firstRDD.union(secondRDD)

    println("\n=== union() ===")
    println(s"Result: ${unionRDD.collect().mkString(", ")}")

    // --------------------------------------------------
    // 2. RDD Actions
    // --------------------------------------------------

    println("\n=== RDD Actions ===")

    println(s"count(): ${numbersRDD.count()}")

    println(s"collect(): ${numbersRDD.collect().mkString(", ")}")

    println(s"first(): ${numbersRDD.first()}")

    println(s"take(3): ${numbersRDD.take(3).mkString(", ")}")

    val reduceResult = numbersRDD.reduce(_ + _)

    println(s"reduce(): $reduceResult")

    // --------------------------------------------------
    // 3. Transformations vs Actions
    // --------------------------------------------------

    println("\n=== Transformations vs Actions ===")

    println("Transformations:")
    println("map, filter, flatMap, distinct, union")

    println("\nActions:")
    println("count, collect, first, take, reduce")

    println("\nTransformations are lazy.")
    println("Actions trigger execution of the RDD computation.")

    // --------------------------------------------------
    // 4. Log Analyzer Scenario
    // --------------------------------------------------

    println("\n=== Log Analyzer ===")

    val logsRDD = sc.textFile("data/logs.txt")

    val errorLogsRDD = logsRDD.filter(line => line.startsWith("ERROR"))

    val errorCount = errorLogsRDD.count()

    println(s"Total log messages: ${logsRDD.count()}")

    println(s"ERROR messages: $errorCount")

    println("\nERROR Log Entries:")

    errorLogsRDD.collect().foreach(println)

    // --------------------------------------------------
    // 5. Identify Lazy Operations
    // --------------------------------------------------

    println("\n=== Lazy Operations ===")

    println("map()      -> Lazy transformation")
    println("filter()   -> Lazy transformation")
    println("flatMap()  -> Lazy transformation")
    println("distinct() -> Lazy transformation")
    println("union()    -> Lazy transformation")

    println("\nActions that trigger execution:")

    println("count()")
    println("collect()")
    println("first()")
    println("take()")
    println("reduce()")

    sc.stop()

    println("\n=== Application Completed Successfully ===")
  }
}