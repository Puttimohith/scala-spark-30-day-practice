import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

// Patient vital event schema
case class PatientVital(
  patientId: String,
  temperature: Double,
  heartRate: Int,
  systolicBP: Int,
  diastolicBP: Int,
  spo2: Int
)

object Main {

  def main(args: Array[String]): Unit = {

    println("========== DAY 26 - REAL-TIME HEALTHCARE PROJECT ==========")

    // ---------------------------------------------------------
    // 1. PATIENT VITAL EVENT SCHEMA
    // ---------------------------------------------------------

    println("========== PATIENT VITAL EVENT SCHEMA ==========")
    println("PatientVital(patientId, temperature, heartRate, systolicBP, diastolicBP, spo2)")

    // ---------------------------------------------------------
    // 2. SPARK CONFIGURATION
    // ---------------------------------------------------------

    val conf = new SparkConf()
      .setAppName("Day-26-Real-Time-Healthcare-Project")
      .setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(5))

    // Checkpoint is required for stateful processing
    ssc.checkpoint("data/checkpoint")

    // ---------------------------------------------------------
    // 3. BROADCAST THRESHOLDS
    // ---------------------------------------------------------

    val thresholds = Map(
      "maxTemperature" -> 100.4,
      "maxHeartRate" -> 100.0,
      "maxSystolicBP" -> 140.0,
      "maxDiastolicBP" -> 90.0,
      "minSpO2" -> 92.0
    )

    val broadcastThresholds = ssc.sparkContext.broadcast(thresholds)

    println("========== BROADCAST THRESHOLDS ==========")
    println(s"Maximum Temperature: ${thresholds("maxTemperature")} F")
    println(s"Maximum Heart Rate: ${thresholds("maxHeartRate").toInt} bpm")
    println(s"Maximum Systolic BP: ${thresholds("maxSystolicBP").toInt}")
    println(s"Maximum Diastolic BP: ${thresholds("maxDiastolicBP").toInt}")
    println(s"Minimum SpO2: ${thresholds("minSpO2").toInt}%")

    // ---------------------------------------------------------
    // 4. ACCUMULATOR
    // ---------------------------------------------------------

    val abnormalVitalAccumulator =
      ssc.sparkContext.longAccumulator("Total Abnormal Vital Events")

    println("========== ACCUMULATOR ==========")
    println("Accumulator: Counts abnormal vital events detected across batches.")

    // ---------------------------------------------------------
    // 5. SAMPLE STREAMING DATA
    // ---------------------------------------------------------

    val batch1 = Seq(
      "P001,98.6,78,120,80,98",
      "P002,99.1,82,125,82,97",
      "P003,101.8,118,145,95,91",
      "P004,98.4,72,118,76,99"
    )

    val batch2 = Seq(
      "P001,102.2,125,150,100,89",
      "P002,98.9,80,122,80,98",
      "P003,102.5,122,148,96,90",
      "P004,98.7,75,119,78,98"
    )

    val batch3 = Seq(
      "P001,103.1,130,155,102,87",
      "P002,99.0,81,124,81,97",
      "P003,103.0,128,152,99,88",
      "P004,98.5,73,117,77,99"
    )

    // Create RDDs with 2 partitions
    val rdd1 = ssc.sparkContext.parallelize(batch1, 2)
    val rdd2 = ssc.sparkContext.parallelize(batch2, 2)
    val rdd3 = ssc.sparkContext.parallelize(batch3, 2)

    // Queue stream simulates incoming real-time batches
    val inputStream = ssc.queueStream(
      scala.collection.mutable.Queue(rdd1, rdd2, rdd3)
    )

    // ---------------------------------------------------------
    // 6. PARSE PATIENT VITAL EVENTS
    // ---------------------------------------------------------

    val parsedVitals = inputStream.flatMap { line =>

      val parts = line.split(",")

      if (parts.length == 6) {
        try {
          Some(
            PatientVital(
              parts(0),
              parts(1).toDouble,
              parts(2).toInt,
              parts(3).toInt,
              parts(4).toInt,
              parts(5).toInt
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
    // 7. ABNORMAL VITAL FILTER
    // ---------------------------------------------------------

    // IMPORTANT:
    // Do NOT update the accumulator inside this transformation.
    // Spark transformations can be recomputed multiple times.

    val abnormalVitals = parsedVitals.filter { vital =>

      val t = broadcastThresholds.value

      vital.temperature > t("maxTemperature") ||
      vital.heartRate > t("maxHeartRate") ||
      vital.systolicBP > t("maxSystolicBP") ||
      vital.diastolicBP > t("maxDiastolicBP") ||
      vital.spo2 < t("minSpO2")
    }

    // ---------------------------------------------------------
    // 8. CURRENT BATCH ABNORMAL ALERTS
    // ---------------------------------------------------------

    abnormalVitals.foreachRDD { rdd =>

      // Count once per batch and update accumulator once
      val abnormalCount = rdd.count()

      if (abnormalCount > 0) {
        abnormalVitalAccumulator.add(abnormalCount)

        println("========== ABNORMAL VITAL ALERTS ==========")

        rdd.collect().foreach { vital =>

          println(
            s"${vital.patientId} -> " +
              f"Temp=${vital.temperature}%.1f F, " +
              s"HR=${vital.heartRate} bpm, " +
              s"BP=${vital.systolicBP}/${vital.diastolicBP}, " +
              s"SpO2=${vital.spo2}%"
          )
        }
      }
    }

    // ---------------------------------------------------------
    // 9. STATEFUL PROCESSING
    // ---------------------------------------------------------

    val abnormalByPatient = abnormalVitals.map { vital =>
      (vital.patientId, 1)
    }

    val runningAbnormalCounts =
      abnormalByPatient.updateStateByKey[Int] {
        (newValues: Seq[Int], previousState: Option[Int]) =>

          val currentCount = previousState.getOrElse(0)

          Some(currentCount + newValues.sum)
      }

    runningAbnormalCounts.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println("========== STATEFUL ABNORMAL COUNTS ==========")

        val results = rdd.collect().sortBy(_._1)

        results.foreach {
          case (patientId, count) =>

            println(
              s"$patientId -> $count abnormal readings"
            )

            if (count >= 2) {
              println(
                s"REPEATED ABNORMAL ALERT: $patientId has repeated abnormal readings."
              )
            }
        }
      }
    }

    // ---------------------------------------------------------
    // 10. WINDOW PROCESSING
    // ---------------------------------------------------------

    val windowedAbnormalCounts =
      abnormalByPatient.reduceByKeyAndWindow(
        (a: Int, b: Int) => a + b,
        Seconds(10),
        Seconds(5)
      )

    windowedAbnormalCounts.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println("========== WINDOW PROCESSING ==========")

        val results = rdd.collect().sortBy(_._1)

        results.foreach {
          case (patientId, count) =>
            println(
              s"$patientId -> $count abnormal readings in 10-second window"
            )
        }
      }
    }

    // ---------------------------------------------------------
    // 11. PARTITIONS
    // ---------------------------------------------------------

    abnormalVitals.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {
        println(
          s"Current RDD partitions: ${rdd.getNumPartitions}"
        )
      }
    }

    // ---------------------------------------------------------
    // 12. CONCEPT EXPLANATIONS
    // ---------------------------------------------------------

    println("========== CONCEPTS ==========")

    println(
      "Broadcast: Sends common threshold values efficiently to worker nodes."
    )

    println(
      "Accumulator: Tracks the total number of abnormal vital events."
    )

    println(
      "Stateful Processing: Maintains abnormal reading counts for each patient across batches."
    )

    println(
      "Window Processing: Analyzes abnormal readings over a fixed time window."
    )

    println(
      "Partitions: Divide RDD data into smaller parts that can be processed in parallel."
    )

    println(
      "DAG: Spark creates a Directed Acyclic Graph of transformations before execution."
    )

    println(
      "Fault Tolerance: Spark can recompute lost RDD partitions using lineage."
    )

    // ---------------------------------------------------------
    // 13. START STREAMING
    // ---------------------------------------------------------

    ssc.start()

    println("StreamingContext: StreamingContext started")

    // Allow all three batches to execute
    ssc.awaitTerminationOrTimeout(25000)

    // ---------------------------------------------------------
    // 14. FINAL ACCUMULATOR RESULT
    // ---------------------------------------------------------

    println("========== FINAL ACCUMULATOR RESULT ==========")

    println(
      s"Total abnormal vital events detected: ${abnormalVitalAccumulator.value}"
    )

    // ---------------------------------------------------------
    // 15. STOP STREAMING
    // ---------------------------------------------------------

    ssc.stop(
      stopSparkContext = true,
      stopGracefully = true
    )

    println("========== DAY 26 COMPLETED ==========")
  }
}