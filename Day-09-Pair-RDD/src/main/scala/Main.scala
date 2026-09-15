import org.apache.spark.{SparkConf, SparkContext}

object Main {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day 9 - Pair RDD")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    println("=== Day 9 - Pair RDD ===")

    // --------------------------------------------------
    // 1. Create a key-value RDD
    // --------------------------------------------------

    val salesRDD = sc.parallelize(
      List(
        ("Laptop", 2, 50000.0, "Electronics"),
        ("Mouse", 5, 800.0, "Electronics"),
        ("Keyboard", 3, 1500.0, "Electronics"),
        ("Chair", 4, 3000.0, "Furniture"),
        ("Table", 2, 7000.0, "Furniture"),
        ("Desk", 3, 5000.0, "Furniture")
      )
    )

    println("\n=== Key-Value RDD ===")

    val productRevenueRDD = salesRDD.map {
      case (product, quantity, price, department) =>
        (product, quantity * price)
    }

    productRevenueRDD.collect().foreach {
      case (product, revenue) =>
        println(f"$product%-10s -> ₹$revenue%.2f")
    }

    // --------------------------------------------------
    // 2. reduceByKey
    // --------------------------------------------------

    println("\n=== reduceByKey ===")

    val productSales = sc.parallelize(
      List(
        ("Laptop", 100000.0),
        ("Mouse", 4000.0),
        ("Laptop", 50000.0),
        ("Mouse", 1600.0),
        ("Chair", 12000.0),
        ("Chair", 6000.0)
      )
    )

    val reducedProductRevenue = productSales
      .reduceByKey(_ + _)

    reducedProductRevenue
      .collect()
      .sortBy(_._1)
      .foreach {
        case (product, revenue) =>
          println(f"$product%-10s -> ₹$revenue%.2f")
      }

    // --------------------------------------------------
    // 3. groupByKey
    // --------------------------------------------------

    println("\n=== groupByKey ===")

    val groupedProductSales = productSales
      .groupByKey()

    groupedProductSales
      .collect()
      .sortBy(_._1)
      .foreach {
        case (product, values) =>
          println(
            f"$product%-10s -> ${values.mkString(", ")}"
          )
      }

    // --------------------------------------------------
    // 4. mapValues
    // --------------------------------------------------

    println("\n=== mapValues ===")

    val productQuantities = sc.parallelize(
      List(
        ("Laptop", 2),
        ("Mouse", 5),
        ("Keyboard", 3),
        ("Chair", 4)
      )
    )

    val doubledQuantities = productQuantities
      .mapValues(quantity => quantity * 2)

    doubledQuantities
      .collect()
      .sortBy(_._1)
      .foreach {
        case (product, quantity) =>
          println(s"$product -> $quantity")
      }

    // --------------------------------------------------
    // 5. Revenue by department
    // --------------------------------------------------

    println("\n=== Revenue by Department ===")

    val departmentRevenueRDD = salesRDD
      .map {
        case (_, quantity, price, department) =>
          (department, quantity * price)
      }
      .reduceByKey(_ + _)

    departmentRevenueRDD
      .collect()
      .sortBy(_._1)
      .foreach {
        case (department, revenue) =>
          println(f"$department%-12s -> ₹$revenue%.2f")
      }

    // --------------------------------------------------
    // 6. reduceByKey vs groupByKey performance
    // --------------------------------------------------

    println("\n=== reduceByKey vs groupByKey ===")

    val largeSalesRDD = sc.parallelize(
      (1 to 10000).map { i =>
        ("Product" + (i % 10), i.toDouble)
      }
    )

    val reduceStart = System.nanoTime()

    val reduceResult = largeSalesRDD
      .reduceByKey(_ + _)
      .collect()

    val reduceTime = (System.nanoTime() - reduceStart) / 1e6

    val groupStart = System.nanoTime()

    val groupResult = largeSalesRDD
      .groupByKey()
      .mapValues(_.sum)
      .collect()

    val groupTime = (System.nanoTime() - groupStart) / 1e6

    println(s"reduceByKey result count: ${reduceResult.length}")
    println(s"groupByKey result count: ${groupResult.length}")

    println(f"reduceByKey time: $reduceTime%.2f ms")
    println(f"groupByKey time: $groupTime%.2f ms")

    println("\nPerformance Explanation:")
    println("reduceByKey performs local aggregation before shuffle.")
    println("groupByKey moves all values for a key before aggregation.")
    println("reduceByKey is generally more efficient for aggregation.")

    // --------------------------------------------------
    // 7. Bank transaction scenario
    // --------------------------------------------------

    println("\n=== Bank Transaction Aggregation ===")

    val transactionsRDD = sc.parallelize(
      List(
        ("A101", 5000.0),
        ("A102", 3000.0),
        ("A101", -1000.0),
        ("A103", 7000.0),
        ("A102", -500.0),
        ("A101", 2000.0),
        ("A103", -2000.0),
        ("A104", 4000.0)
      )
    )

    val accountBalances = transactionsRDD
      .reduceByKey(_ + _)

    accountBalances
      .collect()
      .sortBy(_._1)
      .foreach {
        case (accountId, balance) =>
          println(f"$accountId -> ₹$balance%.2f")
      }

    println("\n=== Day 9 Concepts ===")
    println("Pair RDD -> RDD containing key-value pairs")
    println("reduceByKey -> Aggregates values by key")
    println("groupByKey -> Groups all values belonging to each key")
    println("mapValues -> Transforms only the values")
    println("reduceByKey is generally preferred for aggregation")

    println("\n=== Application Completed Successfully ===")

    sc.stop()
  }
}