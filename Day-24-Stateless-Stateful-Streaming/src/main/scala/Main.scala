import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Main {

  def main(args: Array[String]): Unit = {

    // --------------------------------------------------
    // 1. Spark Configuration
    // --------------------------------------------------

    val conf = new SparkConf()
      .setAppName("Day-24-Stateless-Stateful-Streaming")
      .setMaster("local[2]")

    // --------------------------------------------------
    // 2. Create Streaming Context
    // --------------------------------------------------

    val ssc = new StreamingContext(conf, Seconds(5))

    // Checkpoint is required for updateStateByKey
    ssc.checkpoint("data/checkpoint")

    println("\n========== DAY 24 CONFIGURATION ==========")
    println("Batch interval: 5 seconds")
    println("Streaming Context created successfully")

    // --------------------------------------------------
    // 3. Read Transaction Stream
    // --------------------------------------------------

    val transactionStream =
      ssc.textFileStream("data/stream_input")

    println("\n========== STREAM SOURCE ==========")
    println("Input directory: data/stream_input")
    println("Format: account_id,amount")

    // --------------------------------------------------
    // 4. Validate Transactions
    // --------------------------------------------------

    val validTransactions = transactionStream.filter { line =>

      val parts = line.split(",")

      if (parts.length == 2 && parts(0).nonEmpty) {

        try {
          parts(1).toDouble
          true
        } catch {
          case _: NumberFormatException => false
        }

      } else {
        false
      }
    }

    // --------------------------------------------------
    // 5. Create Account -> Transaction Count Pairs
    // --------------------------------------------------

    val accountTransactions = validTransactions.map { line =>

      val parts = line.split(",")

      val account = parts(0)

      (account, 1)
    }

    // --------------------------------------------------
    // 6. STATELESS PROCESSING
    // --------------------------------------------------
    // Counts transactions only in the current batch.

    val currentBatchCounts =
      accountTransactions.reduceByKey(_ + _)

    currentBatchCounts.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println("\n========== STATELESS RESULT ==========")
        println("Current-batch transaction counts:")

        rdd.collect()
          .sortBy(_._1)
          .foreach {
            case (account, count) =>
              println(s"$account -> $count")
          }
      }
    }

    // --------------------------------------------------
    // 7. STATEFUL PROCESSING
    // --------------------------------------------------
    // Maintains transaction counts across batches.

    val runningCounts =
      accountTransactions.updateStateByKey[Int] {

        (newValues: Seq[Int], previousState: Option[Int]) =>

          val newCount = newValues.sum
          val previousCount = previousState.getOrElse(0)

          Some(previousCount + newCount)
      }

    runningCounts.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println("\n========== STATEFUL RESULT ==========")
        println("Accumulated transaction counts:")

        rdd.collect()
          .sortBy(_._1)
          .foreach {
            case (account, count) =>
              println(s"$account -> $count")
          }
      }
    }

    // --------------------------------------------------
    // 8. Concepts
    // --------------------------------------------------

    println("\n========== CONCEPT ==========")
    println("Stateless processing considers only the current batch.")
    println("Stateful processing maintains information across batches.")
    println("Running counts are maintained separately for each account.")

    // --------------------------------------------------
    // 9. Start Streaming
    // --------------------------------------------------

    ssc.start()

    println("\n========== STREAMING STARTED ==========")
    println("Waiting for transaction files...")

    // --------------------------------------------------
    // 10. Keep the application alive
    // --------------------------------------------------

    Thread.sleep(30000)

    println("\n========== STREAMING STOPPED ==========")

    ssc.stop(
      stopSparkContext = true,
      stopGracefully = true
    )

    println("\n========== DAY 24 COMPLETED ==========")
  }
}