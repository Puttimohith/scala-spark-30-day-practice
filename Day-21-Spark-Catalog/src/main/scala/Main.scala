import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day-21-Spark-Catalog")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // 1. Create a small analytics database
    spark.sql("CREATE DATABASE IF NOT EXISTS hotel_analytics")

    println("\n========== DATABASES ==========")
    spark.sql("SHOW DATABASES").show(false)

    // Use the hotel analytics database
    spark.sql("USE hotel_analytics")

    // 2. Read hotel booking data
    val bookingsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/hotel_bookings.csv")

    println("\n========== HOTEL BOOKINGS DATA ==========")
    bookingsDF.show(false)

    println(s"Booking records: ${bookingsDF.count()}")

    // 3. Create a temporary view
    bookingsDF.createOrReplaceTempView("hotel_bookings_view")

    println("\n========== TEMPORARY VIEW ==========")
    println("Temporary view created: hotel_bookings_view")

    spark.sql("""
      SELECT booking_id, customer_name, hotel, city, room_type, amount, status
      FROM hotel_bookings_view
      ORDER BY amount DESC
    """).show(false)

    // 4. Create a managed Spark SQL table
    spark.sql("DROP TABLE IF EXISTS hotel_bookings")

    bookingsDF.write
      .mode("overwrite")
      .saveAsTable("hotel_bookings")

    println("\n========== TABLE CREATED ==========")
    println("Managed table created: hotel_analytics.hotel_bookings")

    // 5. List tables from Spark Catalog
    println("\n========== TABLES IN HOTEL_ANALYTICS ==========")
    spark.sql("SHOW TABLES").show(false)

    // 6. Register/query the table using Spark SQL
    println("\n========== QUERY REGISTERED TABLE ==========")

    spark.sql("""
      SELECT hotel,
             COUNT(*) AS booking_count,
             SUM(amount) AS total_revenue,
             AVG(amount) AS average_booking_amount
      FROM hotel_bookings
      GROUP BY hotel
      ORDER BY total_revenue DESC
    """).show(false)

    // 7. Inspect table schema
    println("\n========== TABLE SCHEMA ==========")
    spark.sql("DESCRIBE hotel_bookings").show(false)

    // 8. Inspect table metadata using Catalog API
    println("\n========== TABLE METADATA ==========")

    val tableMetadata = spark.catalog.getTable("hotel_analytics", "hotel_bookings")

    println(s"Table name: ${tableMetadata.name}")
    println(s"Database: ${tableMetadata.database}")
    println(s"Table type: ${tableMetadata.tableType}")
    println(s"Is temporary: ${tableMetadata.isTemporary}")

    // 9. List columns using Catalog API
    println("\n========== COLUMN METADATA ==========")

    spark.catalog
      .listColumns("hotel_analytics", "hotel_bookings")
      .select(
        "name",
        "description",
        "dataType",
        "nullable"
      )
      .show(false)

    // 10. Query bookings by city
    println("\n========== BOOKINGS BY CITY ==========")

    spark.sql("""
      SELECT city,
             COUNT(*) AS booking_count,
             SUM(amount) AS total_revenue
      FROM hotel_bookings
      GROUP BY city
      ORDER BY total_revenue DESC
    """).show(false)

    // 11. Query confirmed bookings
    println("\n========== CONFIRMED BOOKINGS ==========")

    spark.sql("""
      SELECT booking_id,
             customer_name,
             hotel,
             city,
             room_type,
             nights,
             amount
      FROM hotel_bookings
      WHERE status = 'Confirmed'
      ORDER BY amount DESC
    """).show(false)

    // 12. Catalog database information
    println("\n========== CURRENT DATABASE ==========")
    println(s"Current database: ${spark.catalog.currentDatabase}")

    println("\n========== DATABASE EXISTS ==========")
    println(
      s"hotel_analytics exists: ${spark.catalog.databaseExists("hotel_analytics")}"
    )

    println("\n========== TABLE EXISTS ==========")
    println(
      s"hotel_bookings exists: ${spark.catalog.tableExists("hotel_analytics.hotel_bookings")}"
    )

    println("\n========== DAY 21 COMPLETED ==========")

    spark.stop()
  }
}