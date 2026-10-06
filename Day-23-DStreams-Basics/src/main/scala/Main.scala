import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Main {

  def main(args: Array[String]): Unit = {

    // --------------------------------------------------
    // 1. Create Spark configuration
    // --------------------------------------------------

    val conf = new SparkConf()
      .setAppName("Day-23-DStreams-Basics")
      .setMaster("local[2]")

    // --------------------------------------------------
    // 2. Create Streaming Context
    // --------------------------------------------------

    val ssc = new StreamingContext(conf, Seconds(5))

    println("\n========== DSTREAM CONFIGURATION ==========")
    println("Batch interval: 5 seconds")
    println("Streaming Context created successfully")

    // --------------------------------------------------
    // 3. Read text stream
    // --------------------------------------------------

    val logStream = ssc.textFileStream("data/stream_input")

    println("\n========== STREAM SOURCE ==========")
    println("Text stream source: data/")
    println("Waiting for new log files...")

    // --------------------------------------------------
    // 4. Apply flatMap
    // --------------------------------------------------

    val words = logStream.flatMap(_.split("\\s+"))

    // --------------------------------------------------
    // 5. Apply filter
    // --------------------------------------------------

    val errorMessages = logStream.filter(
      line => line.contains("ERROR")
    )

    // --------------------------------------------------
    // 6. Apply map
    // --------------------------------------------------

    val errorCount = errorMessages.map(_ => 1)
      
    errorCount.print()  

    // --------------------------------------------------
    // 7. Print ERROR count for every batch
    // --------------------------------------------------

    errorCount.foreachRDD { rdd =>

      val count = if (rdd.isEmpty()) {
        0
      } else {
        rdd.collect().sum
      }

      println("\n========== MICRO-BATCH RESULT ==========")
      println(s"ERROR messages in this batch: $count")
    }

    // --------------------------------------------------
    // 8. Explain micro-batch processing
    // --------------------------------------------------

    println("\n========== MICRO-BATCH PROCESSING ==========")
    println("DStreams process incoming data in small time-based batches.")
    println("Batch interval: 5 seconds")
    println("Each batch is processed as an RDD.")

    // --------------------------------------------------
    // 9. Start streaming
    // --------------------------------------------------

    ssc.start()

    println("\n========== STREAMING STARTED ==========")
    println("Streaming application is running...")
    println("Waiting for incoming log files...")

    // Run long enough to demonstrate streaming.
    ssc.awaitTerminationOrTimeout(60000)

    println("\n========== STREAMING STOPPED ==========")

    ssc.stop(stopSparkContext = true, stopGracefully = true)

    println("\n========== DAY 23 COMPLETED ==========")
  }
}