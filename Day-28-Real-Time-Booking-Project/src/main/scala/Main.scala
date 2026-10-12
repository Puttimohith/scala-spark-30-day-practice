import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.SparkSession

case class BookingEvent(
  bookingId: String,
  customerId: String,
  hotelId: String,
  roomId: String,
  eventType: String,
  rooms: Int,
  amount: Double,
  time: String
)

case class HotelInfo(
  hotelId: String,
  hotelName: String,
  totalRooms: Int
)

object Main {

  def main(args: Array[String]): Unit = {

    // ---------------------------------------------------------
    // 1. SPARK CONFIGURATION
    // ---------------------------------------------------------

    val conf = new SparkConf()
      .setAppName("Day-28-Real-Time-Booking-Project")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val ssc = new StreamingContext(sc, Seconds(5))

    

    val spark = SparkSession.builder()
      .config(sc.getConf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("========== DAY 28 - REAL-TIME BOOKING PROJECT ==========")

    // ---------------------------------------------------------
    // 2. BOOKING EVENT SCHEMA
    // ---------------------------------------------------------

    println("========== BOOKING EVENT SCHEMA ==========")
    println(
      "bookingId, customerId, hotelId, roomId, eventType, rooms, amount, time"
    )
    println("Event types: BOOK and CANCEL")

    // ---------------------------------------------------------
    // 3. HOTEL REFERENCE DATA
    // ---------------------------------------------------------

    val hotelReference: Map[String, HotelInfo] = Map(
      "H001" -> HotelInfo("H001", "Grand Plaza", 5),
      "H002" -> HotelInfo("H002", "City Heights", 4),
      "H003" -> HotelInfo("H003", "Lake View", 3)
    )

    val broadcastHotels: Broadcast[Map[String, HotelInfo]] =
      sc.broadcast(hotelReference)

    println("========== BROADCAST HOTEL REFERENCE ==========")

    broadcastHotels.value.toSeq.sortBy(_._1).foreach {
      case (hotelId, hotel) =>
        println(
          s"$hotelId -> ${hotel.hotelName}, Total Rooms=${hotel.totalRooms}"
        )
    }

    // ---------------------------------------------------------
    // 4. LOAD BOOKING EVENTS
    // ---------------------------------------------------------

    val bookingRDD: RDD[BookingEvent] =
      sc.textFile("data/booking_events.txt")
        .filter(_.trim.nonEmpty)
        .map { line =>

          val parts = line.split(",")

          BookingEvent(
            bookingId = parts(0),
            customerId = parts(1),
            hotelId = parts(2),
            roomId = parts(3),
            eventType = parts(4),
            rooms = parts(5).toInt,
            amount = parts(6).toDouble,
            time = parts(7)
          )
        }

    // ---------------------------------------------------------
    // 5. CREATE STREAM BATCHES
    // ---------------------------------------------------------

    val allBookings = bookingRDD.collect().toSeq

    val batches = allBookings.grouped(4).toSeq

    val queueStream = ssc.queueStream(
      scala.collection.mutable.Queue(
        batches.map(batch => sc.parallelize(batch, 2)): _*
      )
    )

    // ---------------------------------------------------------
    // 6. PAIR RDD + WINDOW OPERATIONS
    // ---------------------------------------------------------

    val bookingDStream = queueStream

    val windowedBookings = bookingDStream
      .map { booking =>
        (booking.hotelId, 1)
      }
      .reduceByKeyAndWindow(
        (a: Int, b: Int) => a + b,
        Seconds(10),
        Seconds(5)
      )

    // ---------------------------------------------------------
    // RUNNING OCCUPANCY STATE
    // ---------------------------------------------------------

    val occupancyState =
      scala.collection.mutable.Map[String, Int]()

    // ---------------------------------------------------------
    // 7. PROCESS STREAM
    // ---------------------------------------------------------

    bookingDStream.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        val persistedRDD =
          rdd.persist(StorageLevel.MEMORY_ONLY)

        println("========== CURRENT BOOKING BATCH ==========")

        persistedRDD.collect().foreach { booking =>
          println(
            f"${booking.bookingId} -> ${booking.hotelId}, ${booking.roomId}, ${booking.eventType}, rooms=${booking.rooms}, amount=${booking.amount}%.2f"
          )
        }

        // -----------------------------------------------------
        // BOOKING / CANCELLATION STATE
        // -----------------------------------------------------

        val stateChanges = persistedRDD
          .map { booking =>

            val change =
              if (booking.eventType == "BOOK")
                booking.rooms
              else
                -booking.rooms

            (booking.hotelId, change)
          }
          .reduceByKey(_ + _)

        println("========== BOOKING / CANCELLATION STATE ==========")

        stateChanges.collect().sortBy(_._1).foreach {
          case (hotelId, change) =>

            val hotel =
              broadcastHotels.value(hotelId)

            val status =
              if (change > 0)
                "BOOKING"
              else
                "CANCELLATION"

            println(
              s"$hotelId (${hotel.hotelName}) -> $status change=${math.abs(change)}"
            )
        }

        // -----------------------------------------------------
        // OCCUPANCY AND AVAILABILITY
        // -----------------------------------------------------

        val occupancyChanges = persistedRDD
          .map { booking =>

            val change =
              if (booking.eventType == "BOOK")
                booking.rooms
              else
                -booking.rooms

            (booking.hotelId, change)
          }
          .reduceByKey(_ + _)

        println("========== OCCUPANCY AND AVAILABILITY ==========")

        occupancyChanges.collect().sortBy(_._1).foreach {
          case (hotelId, change) =>
            val hotel =
              broadcastHotels.value(hotelId)
            val previousOccupied =
              occupancyState.getOrElse(hotelId, 0)

            val currentOccupied =
                math.min(
                  math.max(previousOccupied + change, 0),
                  hotel.totalRooms
                )

            occupancyState.update(
              hotelId,
              currentOccupied
            )

            

            val availableRooms =
              math.max(
                hotel.totalRooms - currentOccupied,
                0
              )

            val occupancyPercentage =
              if (hotel.totalRooms == 0)
                0.0
              else
                currentOccupied.toDouble /
                  hotel.totalRooms *
                  100

            println(
              f"$hotelId (${hotel.hotelName}) -> Occupied=$currentOccupied, Available=$availableRooms, Occupancy=$occupancyPercentage%.2f%%"
            )
        }

        // -----------------------------------------------------
        // REVENUE BY HOTEL
        // -----------------------------------------------------

        val revenueByHotel = persistedRDD
          .filter(_.eventType == "BOOK")
          .map { booking =>
            (booking.hotelId, booking.amount)
          }
          .reduceByKey(_ + _)

        println("========== REVENUE BY HOTEL ==========")

        revenueByHotel.collect().sortBy(_._1).foreach {
          case (hotelId, revenue) =>
            println(
              f"$hotelId -> Revenue=$revenue%.2f"
            )
        }

        // -----------------------------------------------------
        // PARTITIONING
        // -----------------------------------------------------

        println(
          s"Current booking RDD partitions: ${persistedRDD.getNumPartitions}"
        )

        persistedRDD.unpersist()

        println()
      }
    }

    // ---------------------------------------------------------
    // 8. WINDOW RESULT
    // ---------------------------------------------------------

    windowedBookings.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println("========== BOOKING WINDOW ==========")

        rdd.collect().sortBy(_._1).foreach {
          case (hotelId, count) =>

            println(
              s"$hotelId -> $count booking events in 10-second window"
            )

            if (count >= 4) {
              println(
                s"HIGH BOOKING ACTIVITY: $hotelId has $count events in the window."
              )
            }
        }
      }
    }

    // ---------------------------------------------------------
    // 9. SPARK SQL REPORTING
    // ---------------------------------------------------------

    val bookingDF = spark.read
      .option("header", "false")
      .option("inferSchema", "true")
      .csv("data/booking_events.txt")
      .toDF(
        "bookingId",
        "customerId",
        "hotelId",
        "roomId",
        "eventType",
        "rooms",
        "amount",
        "time"
      )

    bookingDF.createOrReplaceTempView("booking_events")

    val bookingReport = spark.sql(
      """
        SELECT
          hotelId,
          COUNT(*) AS events,
          SUM(
            CASE
              WHEN eventType = 'BOOK' THEN rooms
              ELSE 0
            END
          ) AS bookedRooms,
          SUM(
            CASE
              WHEN eventType = 'CANCEL' THEN rooms
              ELSE 0
            END
          ) AS cancelledRooms,
          SUM(
            CASE
              WHEN eventType = 'BOOK' THEN amount
              ELSE 0
            END
          ) AS booking_revenue
        FROM booking_events
        GROUP BY hotelId
        ORDER BY hotelId
      """
    )

    println("========== SPARK SQL REPORT ==========")

    bookingReport.collect().foreach { row =>

      println(
        s"${row.getString(0)} -> events=${row.getLong(1)}, bookedRooms=${row.getLong(2)}, cancelledRooms=${row.getLong(3)}, revenue=${row.getLong(4)}"
      )
    }

    // ---------------------------------------------------------
    // 10. CONCEPT EXPLANATIONS
    // ---------------------------------------------------------

    println("========== CONCEPTS ==========")

    println(
      "Pair RDD: Booking events are converted into key-value pairs using hotel ID as the key."
    )

    println(
      "Window Operations: A 10-second window with a 5-second sliding interval is used to detect booking activity."
    )

    println(
      "Broadcast: Small hotel reference data is broadcast to Spark executors to avoid repeatedly sending the same data."
    )

    println(
      "State: Booking and cancellation changes are maintained using a running occupancy state for each hotel."
    )

    println(
      "Occupancy: Current occupied rooms are calculated from previous occupancy plus booking or cancellation changes."
    )

    println(
      "Spark SQL: Booking events are converted into a DataFrame and queried using SQL for reporting."
    )

    println(
      "Partitioning: Booking RDDs use multiple partitions so Spark can process data in parallel."
    )

    println(
      "Streaming: Spark Streaming processes booking events as micro-batches."
    )

    // ---------------------------------------------------------
    // 11. START STREAMING
    // ---------------------------------------------------------

    println("========== STREAMING STARTED ==========")

    ssc.start()

    ssc.awaitTerminationOrTimeout(25000)

    // ---------------------------------------------------------
    // 12. STOP APPLICATION
    // ---------------------------------------------------------

    if (!ssc.awaitTerminationOrTimeout(1000)) {
      ssc.stop(
        stopSparkContext = false,
        stopGracefully = true
      )
    }

    broadcastHotels.destroy()

    spark.stop()

    sc.stop()

    println("========== DAY 28 COMPLETED ==========")
  }
}