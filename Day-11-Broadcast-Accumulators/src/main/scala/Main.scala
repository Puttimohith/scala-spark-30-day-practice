import org.apache.spark.{SparkConf, SparkContext}

object Main {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day 11 - Broadcast and Accumulators")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    println("=== Day 11 - Broadcast and Accumulators ===")

    // --------------------------------------------------
    // 1. Broadcast a small product reference map
    // --------------------------------------------------

    val productMaster = Map(
      "P101" -> "Laptop",
      "P102" -> "Mouse",
      "P103" -> "Keyboard",
      "P104" -> "Monitor"
    )

    val broadcastProducts = sc.broadcast(productMaster)

    println("\n=== Broadcast Product Master ===")

    broadcastProducts.value
      .toSeq
      .sortBy(_._1)
      .foreach {
        case (productId, productName) =>
          println(s"$productId -> $productName")
      }

    // --------------------------------------------------
    // 2. Create transaction RDD
    // --------------------------------------------------

    val transactionsRDD = sc.parallelize(
      List(
        ("T001", "P101", 2),
        ("T002", "P102", 5),
        ("T003", "P103", 3),
        ("T004", "P999", 1),
        ("T005", "P104", 2),
        ("T006", "P102", 4),
        ("T007", "P888", 2),
        ("T008", "P101", 1)
      )
    )

    println("\n=== Transactions ===")

    transactionsRDD.collect().foreach {
      case (transactionId, productId, quantity) =>
        println(
          s"$transactionId -> Product: $productId -> Quantity: $quantity"
        )
    }

    // --------------------------------------------------
    // 3. Accumulator for bad records
    // --------------------------------------------------

    val badRecordAccumulator = sc.longAccumulator("Bad Records")

    // --------------------------------------------------
    // 4. Validate transactions using broadcast data
    // --------------------------------------------------

    val validatedTransactions = transactionsRDD.flatMap {
      case (transactionId, productId, quantity) =>

        val productName =
          broadcastProducts.value.get(productId)

        productName match {

          case Some(name) =>
            Some(
              (transactionId, productId, name, quantity)
            )

          case None =>
            badRecordAccumulator.add(1)
            None
        }
    }

    println("\n=== Validated Transactions ===")

    validatedTransactions
      .collect()
      .foreach {
        case (transactionId, productId, productName, quantity) =>
          println(
            s"$transactionId -> $productId -> $productName -> Quantity: $quantity"
          )
      }

    // --------------------------------------------------
    // 5. Display bad records
    // --------------------------------------------------

    println("\n=== Bad Record Count ===")
    println(
      s"Invalid transactions: ${badRecordAccumulator.value}"
    )

    // --------------------------------------------------
    // 6. Explain normal driver variables
    // --------------------------------------------------

    println("\n=== Driver Variables vs Accumulators ===")

    println(
      "Normal driver variables should not be used for distributed updates."
    )

    println(
      "Executors run tasks independently and their local updates are not reliably sent back to the driver."
    )

    println(
      "Accumulators are designed for counters and aggregated information from distributed tasks."
    )

    // --------------------------------------------------
    // 7. Product quantity aggregation
    // --------------------------------------------------

    println("\n=== Product Quantity Summary ===")

    val productQuantitySummary = validatedTransactions
      .map {
        case (_, productId, productName, quantity) =>
          (productId, (productName, quantity))
      }
      .mapValues {
        case (productName, quantity) =>
          (productName, quantity)
      }
      .reduceByKey {
        case ((name1, quantity1), (_, quantity2)) =>
          (name1, quantity1 + quantity2)
      }

    productQuantitySummary
      .collect()
      .sortBy(_._1)
      .foreach {
        case (productId, (productName, totalQuantity)) =>
          println(
            s"$productId -> $productName -> Total Quantity: $totalQuantity"
          )
      }

    // --------------------------------------------------
    // 8. Scenario explanation
    // --------------------------------------------------

    println("\n=== Transaction Validation Scenario ===")

    println(
      "The broadcast product master is distributed to executors."
    )

    println(
      "Each transaction is checked against the broadcast master table."
    )

    println(
      "Transactions with unknown product IDs are counted as bad records."
    )

    println(
      "Valid transactions are processed further using RDD transformations."
    )

    // --------------------------------------------------
    // 9. Summary
    // --------------------------------------------------

    println("\n=== Day 11 Concepts ===")

    println(
      "Broadcast -> Efficiently shares a small read-only dataset with executors."
    )

    println(
      "Accumulator -> Collects aggregated information such as counters from tasks."
    )

    println(
      "Driver variable -> Should not be used for distributed updates."
    )

    println(
      "Broadcast + RDD -> Small master data can be used while processing distributed RDD data."
    )

    println("\n=== Application Completed Successfully ===")

    broadcastProducts.destroy()

    sc.stop()
  }
}