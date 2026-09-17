import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Main {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day 12 - Cache and Persist")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    println("=== Day 12 - Cache and Persist ===")

    // --------------------------------------------------
    // 1. Create and clean transaction dataset
    // --------------------------------------------------

    val transactionsRDD = sc.parallelize(
      List(
        ("T001", "A101", "Electronics", 1200.0),
        ("T002", "A102", "Furniture", 2500.0),
        ("T003", "A101", "Electronics", 800.0),
        ("T004", "A103", "Clothing", 1500.0),
        ("T005", "A102", "Furniture", 3000.0),
        ("T006", "A104", "Electronics", 2200.0),
        ("T007", "A101", "Clothing", 900.0),
        ("T008", "A103", "Electronics", 1800.0),
        ("T009", "A104", "Furniture", 2700.0),
        ("T010", "A102", "Clothing", 1100.0)
      ),
      2
    )

    val cleanedTransactions = transactionsRDD.filter {
      case (_, _, _, amount) =>
        amount > 0
    }

    println("\n=== Cleaned Transaction Dataset ===")

    cleanedTransactions.collect().foreach {
      case (transactionId, accountId, department, amount) =>
        println(
          f"$transactionId -> $accountId -> $department -> ₹$amount%.2f"
        )
    }

    // --------------------------------------------------
    // 2. Cache the cleaned dataset
    // --------------------------------------------------

    cleanedTransactions.cache()

    println("\n=== Cache ===")
    println("Cleaned transaction dataset cached using cache().")
    println(
      s"Storage level after cache(): ${cleanedTransactions.getStorageLevel}"
    )

    // --------------------------------------------------
    // 3. First report - total transaction amount
    // --------------------------------------------------

    println("\n=== Report 1: Total Transaction Amount ===")

    val totalAmount = cleanedTransactions
      .map {
        case (_, _, _, amount) =>
          amount
      }
      .sum

    println(f"Total Amount: ₹$totalAmount%.2f")

    // --------------------------------------------------
    // 4. Second report - revenue by department
    // --------------------------------------------------

    println("\n=== Report 2: Revenue by Department ===")

    val departmentRevenue = cleanedTransactions
      .map {
        case (_, _, department, amount) =>
          (department, amount)
      }
      .reduceByKey(_ + _)

    departmentRevenue
      .collect()
      .sortBy(_._1)
      .foreach {
        case (department, revenue) =>
          println(f"$department%-12s -> ₹$revenue%.2f")
      }

    // --------------------------------------------------
    // 5. Third report - transaction count by account
    // --------------------------------------------------

    println("\n=== Report 3: Transaction Count by Account ===")

    val accountTransactionCount = cleanedTransactions
      .map {
        case (_, accountId, _, _) =>
          (accountId, 1)
      }
      .reduceByKey(_ + _)

    accountTransactionCount
      .collect()
      .sortBy(_._1)
      .foreach {
        case (accountId, count) =>
          println(s"$accountId -> $count transactions")
      }

    // --------------------------------------------------
    // 6. Show cache reuse
    // --------------------------------------------------

    println("\n=== Cached Dataset Reuse ===")

    println(
      "The same cleaned transaction dataset was reused for three reports."
    )

    println(
      "Because the dataset is cached, Spark can reuse the computed data."
    )

    // --------------------------------------------------
    // 7. Compare cache and persist
    // --------------------------------------------------

    println("\n=== cache() vs persist() ===")

    println(
      "cache() stores an RDD using Spark's default storage level."
    )

    println(
      "persist() allows an explicit storage level to be selected."
    )

    println(
      "cache() is convenient when the default storage level is sufficient."
    )

    println(
      "persist() is useful when specific memory or disk behavior is required."
    )

    // --------------------------------------------------
    // 8. Experiment with different storage levels
    // --------------------------------------------------

    val memoryOnlyRDD = sc.parallelize(1 to 10)

    memoryOnlyRDD.persist(StorageLevel.MEMORY_ONLY)

    println("\n=== Storage Level: MEMORY_ONLY ===")
    println(s"Storage level: ${memoryOnlyRDD.getStorageLevel}")
    println(s"Count: ${memoryOnlyRDD.count()}")

    memoryOnlyRDD.unpersist()

    val memoryAndDiskRDD = sc.parallelize(1 to 10)

    memoryAndDiskRDD.persist(StorageLevel.MEMORY_AND_DISK)

    println("\n=== Storage Level: MEMORY_AND_DISK ===")
    println(s"Storage level: ${memoryAndDiskRDD.getStorageLevel}")
    println(s"Count: ${memoryAndDiskRDD.count()}")

    memoryAndDiskRDD.unpersist()

    // --------------------------------------------------
    // 9. When caching can hurt performance
    // --------------------------------------------------

    println("\n=== When Caching Can Hurt Performance ===")

    println(
      "Caching can hurt performance when the dataset is used only once."
    )

    println(
      "Caching can consume valuable executor memory."
    )

    println(
      "Caching very large datasets can cause memory pressure."
    )

    println(
      "If the cached dataset does not fit in memory, recomputation or disk I/O may occur."
    )

    println(
      "Caching should be used when an RDD is expensive to compute and reused by multiple actions."
    )

    // --------------------------------------------------
    // 10. Scenario summary
    // --------------------------------------------------

    println("\n=== Three-Report Scenario ===")

    println("Clean transaction dataset")
    println("        ↓")
    println("      cache()")
    println("        ↓")
    println("  +-----+-----+-----+")
    println("  |           |     |")
    println("Report 1   Report 2  Report 3")
    println("Total      Department Account")
    println("Amount     Revenue   Count")

    println(
      "\nThe cleaned transaction dataset is reused across all three reports."
    )

    println("\n=== Day 12 Concepts ===")

    println("cache() -> Caches an RDD using the default storage level.")
    println("persist() -> Caches an RDD using a selected storage level.")
    println("MEMORY_ONLY -> Stores partitions in memory.")
    println("MEMORY_AND_DISK -> Stores in memory and uses disk when needed.")
    println("Caching -> Useful for expensive datasets reused multiple times.")

    println("\n=== Application Completed Successfully ===")

    cleanedTransactions.unpersist()

    sc.stop()
  }
}