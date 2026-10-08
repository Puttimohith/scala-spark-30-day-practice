import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.mutable

object Main {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day-25-Window-Operations")
      .setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(5))

    ssc.checkpoint("data/checkpoint")

    println("\n========== DAY 25 CONFIGURATION ==========")
    println("Batch interval: 5 seconds")
    println("Window size: 10 minutes")
    println("Sliding interval: 5 seconds")

    val transactions = Seq(
      "ACC001,100",
      "ACC002,200",
      "ACC001,150",
      "ACC003,300",
      "ACC002,250",
      "ACC001,200",
      "ACC004,400",
      "ACC002,350",
      "ACC001,500",
      "ACC003,450",
      "ACC002,600",
      "ACC004,550"
    )

    val batch1 = transactions.take(4)
    val batch2 = transactions.slice(4, 8)
    val batch3 = transactions.slice(8, 12)

    val queue = mutable.Queue(
      ssc.sparkContext.parallelize(batch1),
      ssc.sparkContext.parallelize(batch2),
      ssc.sparkContext.parallelize(batch3)
    )

    val transactionStream = ssc.queueStream(queue)

    println("\n========== DSTREAM CREATED ==========")
    println("Transaction stream created successfully")

    val accountTransactions = transactionStream.map { line =>
      val parts = line.split(",")
      val account = parts(0)
      val amount = parts(1).toDouble
      (account, amount)
    }

    val transactionCountWindow =
      transactionStream.countByWindow(
        Seconds(600),
        Seconds(5)
      )

    transactionCountWindow.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        val count = rdd.first()

        println("\n========== COUNT BY WINDOW ==========")
        println(s"Transactions in 10-minute window: $count")

        if (count >= 10) {
          println("ALERT: Sudden increase in transactions detected!")
        } else {
          println("Normal transaction activity.")
        }
      }
    }

    val rollingSales =
      accountTransactions.reduceByKeyAndWindow(
        (a: Double, b: Double) => a + b,
        Seconds(600),
        Seconds(5)
      )

    rollingSales.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {

        println("\n========== REDUCE BY KEY AND WINDOW ==========")
        println("Rolling sales totals:")

        rdd.collect()
          .sortBy(_._1)
          .foreach {
            case (account, total) =>
              println(f"$account -> $$${total}%.2f")
          }
      }
    }

    println("\n========== WINDOW CONCEPTS ==========")

    println("Batch interval:")
    println("The frequency at which Spark creates micro-batches.")

    println("\nWindow size:")
    println("The amount of historical data included in each window.")

    println("\nSliding interval:")
    println("How frequently the window moves forward.")

    println("\ncountByWindow:")
    println("Counts the number of records inside the window.")

    println("\nreduceByKeyAndWindow:")
    println("Aggregates values by key over the window.")

    ssc.start()

    println("\n========== STREAMING STARTED ==========")

    ssc.awaitTerminationOrTimeout(30000)

    println("\n========== STREAMING STOPPED ==========")

    ssc.stop(
      stopSparkContext = true,
      stopGracefully = true
    )

    println("\n========== DAY 25 COMPLETED ==========")
  }
}