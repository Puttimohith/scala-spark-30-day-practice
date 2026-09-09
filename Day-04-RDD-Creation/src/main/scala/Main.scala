import org.apache.spark.{SparkConf, SparkContext}

object Main {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day 4 - RDD Creation")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    println("=== Day 4 - RDD Creation ===")

    // 1. Create RDD from a collection
    val numbers = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val numberRDD = sc.parallelize(numbers)

    println("\n=== RDD from Collection ===")
    println(s"Numbers: ${numberRDD.collect().mkString(", ")}")

    // 2. map()
    val squaredRDD = numberRDD.map(n => n * n)

    println("\n=== map() ===")
    println(s"Squared: ${squaredRDD.collect().mkString(", ")}")

    // 2. filter()
    val evenRDD = numberRDD.filter(n => n % 2 == 0)

    println("\n=== filter() ===")
    println(s"Even numbers: ${evenRDD.collect().mkString(", ")}")

    // 2. flatMap()
    val sentences = List(
      "Scala Spark",
      "RDD Programming",
      "Big Data"
    )

    val sentencesRDD = sc.parallelize(sentences)

    val wordsRDD = sentencesRDD.flatMap(line => line.split(" "))

    println("\n=== flatMap() ===")
    println(s"Words: ${wordsRDD.collect().mkString(", ")}")

    // 3. Calculate total sales
    val transactions = List(
      ("Laptop", 2, 50000.0),
      ("Mouse", 5, 800.0),
      ("Keyboard", 3, 1500.0),
      ("Monitor", 2, 12000.0),
      ("Headphones", 4, 2500.0)
    )

    val transactionRDD = sc.parallelize(transactions)

    val totalSales = transactionRDD
      .map {
        case (_, quantity, price) => quantity * price
      }
      .reduce(_ + _)

    println("\n=== Total Sales ===")
    println(f"Total Sales: ₹$totalSales%.2f")

    // 4. Inspect partitions and default parallelism
    println("\n=== Partition Information ===")
    println(s"Number of RDD partitions: ${numberRDD.getNumPartitions}")
    println(s"Default parallelism: ${sc.defaultParallelism}")

    numberRDD
      .mapPartitionsWithIndex {
        case (partitionId, values) =>
          Iterator(
            s"Partition $partitionId: ${values.mkString(", ")}"
          )
      }
      .collect()
      .foreach(println)

    // 5. Read customer text file
    val customerRDD = sc.textFile("data/customers.txt", 2)

    println("\n=== Customer File RDD ===")
    println(s"Customer RDD partitions: ${customerRDD.getNumPartitions}")

    customerRDD.collect().foreach(println)

    // Process customer records
    val customerDetailsRDD = customerRDD.map { line =>
      val fields = line.split(",")

      (
        fields(0).toInt,
        fields(1),
        fields(2)
      )
    }

    println("\n=== Customer Details ===")

    customerDetailsRDD.collect().foreach {
      case (id, name, city) =>
        println(s"ID: $id, Name: $name, City: $city")
    }

    // Scenario: process customer file across partitions
    println("\n=== Customer Partition Processing ===")

    customerRDD
      .mapPartitionsWithIndex {
        case (partitionId, records) =>
          Iterator(
            s"Partition $partitionId processed ${records.size} customer records"
          )
      }
      .collect()
      .foreach(println)

    sc.stop()

    println("\n=== Application Completed Successfully ===")
  }
}