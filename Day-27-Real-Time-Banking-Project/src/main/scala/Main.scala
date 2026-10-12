import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

case class Transaction(
  transactionId: String,
  accountId: String,
  branchId: String,
  amount: Double,
  time: String
)

object Main {

  def main(args: Array[String]): Unit = {

    println("========== DAY 27 - REAL-TIME BANKING PROJECT ==========")

    // ---------------------------------------------------------
    // 1. TRANSACTION EVENT SCHEMA
    // ---------------------------------------------------------

    println("========== TRANSACTION EVENT SCHEMA ==========")
    println(
      "Transaction(transactionId, accountId, branchId, amount, time)"
    )

    // ---------------------------------------------------------
    // 2. SPARK CONFIGURATION
    // ---------------------------------------------------------

    val conf = new SparkConf()
      .setAppName("Day-27-Real-Time-Banking-Project")
      .setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(5))

    // ---------------------------------------------------------
    // 3. TRANSACTION DATA
    // ---------------------------------------------------------

    val batch1 = Seq(
      "TX001,ACC001,BR001,500,10:00",
      "TX002,ACC002,BR002,1200,10:01",
      "TX003,ACC001,BR001,700,10:02",
      "TX004,ACC003,BR003,300,10:03"
    )

    val batch2 = Seq(
      "TX005,ACC001,BR001,900,10:04",
      "TX006,ACC002,BR002,1500,10:05",
      "TX007,ACC001,BR001,1100,10:06",
      "TX008,ACC003,BR003,400,10:07"
    )

    val batch3 = Seq(
      "TX009,ACC001,BR001,1300,10:08",
      "TX010,ACC002,BR002,800,10:09",
      "TX011,ACC001,BR001,1400,10:10",
      "TX012,ACC003,BR003,600,10:11"
    )

    // Two partitions to demonstrate partitioning
    val rdd1 = ssc.sparkContext.parallelize(batch1, 2)
    val rdd2 = ssc.sparkContext.parallelize(batch2, 2)
    val rdd3 = ssc.sparkContext.parallelize(batch3, 2)

    val inputStream = ssc.queueStream(
      scala.collection.mutable.Queue(rdd1, rdd2, rdd3)
    )

    // ---------------------------------------------------------
    // 4. PARSE TRANSACTIONS
    // ---------------------------------------------------------

    val transactions = inputStream.flatMap { line =>

      val parts = line.split(",")

      if (parts.length == 5) {
        try {
          Some(
            Transaction(
              parts(0),
              parts(1),
              parts(2),
              parts(3).toDouble,
              parts(4)
            )
          )
        } catch {
          case _: Exception => None
        }
      } else {
        None
      }
    }

    // ---------------------------------------------------------
    // 5. AGGREGATE TRANSACTIONS BY ACCOUNT
    // ---------------------------------------------------------

    val accountTotals = transactions
      .map(t => (t.accountId, t.amount))
      .reduceByKey(_ + _)

    accountTotals.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println("========== TRANSACTION TOTALS BY ACCOUNT ==========")

        rdd.collect()
          .sortBy(_._1)
          .foreach {
            case (account, total) =>
              println(
                f"$account -> total transaction amount = $total%.2f"
              )
          }
      }
    }

    // ---------------------------------------------------------
    // 6. SUSPICIOUS BURST DETECTION USING WINDOWS
    // ---------------------------------------------------------

    val transactionCounts = transactions
      .map(t => (t.accountId, 1))

    val burstCounts = transactionCounts.reduceByKeyAndWindow(
      (a: Int, b: Int) => a + b,
      Seconds(10),
      Seconds(5)
    )

    burstCounts.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println("========== SUSPICIOUS BURST WINDOW ==========")

        rdd.collect()
          .sortBy(_._1)
          .foreach {
            case (account, count) =>

              println(
                s"$account -> $count transactions in 10-second window"
              )

              if (count >= 3) {
                println(
                  s"SUSPICIOUS BURST ALERT: $account has $count transactions in the window."
                )
              }
          }
      }
    }

    // ---------------------------------------------------------
    // 7. BRANCH / RISK REFERENCE DATA
    // ---------------------------------------------------------

    val branchRiskData = Seq(
      ("BR001", "LOW"),
      ("BR002", "MEDIUM"),
      ("BR003", "HIGH")
    )

    val branchRiskRDD =
      ssc.sparkContext.parallelize(branchRiskData, 2)

    // ---------------------------------------------------------
    // 8. JOIN TRANSACTIONS WITH BRANCH RISK DATA
    // ---------------------------------------------------------

    val transactionWithBranch =
      transactions.map(t => (t.branchId, t))

    val branchRisk = branchRiskRDD

    val joinedTransactions =
      transactionWithBranch.transform { rdd =>
        rdd.join(branchRisk)
      }

    joinedTransactions.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println("========== TRANSACTION + BRANCH RISK ==========")

        rdd.collect()
          .sortBy(_._2._1.transactionId)
          .foreach {
            case (_, (transaction, risk)) =>

              println(
                s"${transaction.transactionId} -> " +
                  s"${transaction.accountId}, " +
                  s"${transaction.branchId}, " +
                  f"${transaction.amount}%.2f, " +
                  s"Risk=$risk"
              )
          }
      }
    }

    // ---------------------------------------------------------
    // 9. CACHE / PERSIST
    // ---------------------------------------------------------

    val cachedTransactions =
      transactions.transform { rdd =>
        rdd.persist(StorageLevel.MEMORY_ONLY)
        rdd
      }

    cachedTransactions.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {
        println(
          s"Cached transaction RDD partitions: ${rdd.getNumPartitions}"
        )
      }
    }

    // ---------------------------------------------------------
    // 10. PARTITIONING
    // ---------------------------------------------------------

    transactions.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println(
          s"Current transaction RDD partitions: ${rdd.getNumPartitions}"
        )
      }
    }

    // ---------------------------------------------------------
    // 11. CONCEPT EXPLANATIONS
    // ---------------------------------------------------------

    println("========== CONCEPTS ==========")

    println(
      "Aggregation: Transactions are grouped by account and total amount is calculated."
    )

    println(
      "Window Processing: A 10-second window with a 5-second sliding interval detects transaction bursts."
    )

    println(
      "Join: Transactions are joined with small branch risk reference data using branch ID."
    )

    println(
      "Cache/Persist: Transaction data is persisted in memory to avoid repeated computation."
    )

    println(
      "Partitioning: Transaction RDDs use multiple partitions for parallel processing."
    )

    println(
      "YARN: In a production cluster, Spark can run on YARN where YARN manages cluster resources and Spark executors process the application."
    )

    println(
      "YARN Flow: Client submits application -> YARN ResourceManager allocates resources -> ApplicationMaster starts -> Executors run Spark tasks."
    )

    // ---------------------------------------------------------
    // 12. START STREAMING
    // ---------------------------------------------------------

    ssc.start()

    println("StreamingContext: StreamingContext started")

    // Allow all batches and window operations to execute
    ssc.awaitTerminationOrTimeout(25000)

    // ---------------------------------------------------------
    // 13. STOP STREAMING
    // ---------------------------------------------------------

    ssc.stop(
      stopSparkContext = true,
      stopGracefully = true
    )

    println("========== DAY 27 COMPLETED ==========")
  }
}
