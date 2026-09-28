import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day-20-File-Formats-and-Output")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val inputPath = "data/daily_sales.csv"

    // 1. Read CSV
    val salesDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(inputPath)

    println("\n========== INPUT SALES DATA ==========")
    salesDF.show(false)

    println(s"Input records: ${salesDF.count()}")

    // Add year, month and day columns for partitioning
    val salesWithDateParts = salesDF
      .withColumn("sale_date", to_date(col("sale_date")))
      .withColumn("year", year(col("sale_date")))
      .withColumn("month", month(col("sale_date")))
      .withColumn("day", dayofmonth(col("sale_date")))

    println("\n========== SALES WITH DATE PARTITIONS ==========")
    salesWithDateParts.show(false)

    // 2. Write CSV output
    val csvOutput = "data/output/csv_sales"

    salesWithDateParts
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(csvOutput)

    println("\nCSV output written to: " + csvOutput)

    // 3. Write JSON output
    val jsonOutput = "data/output/json_sales"

    salesWithDateParts
      .coalesce(1)
      .write
      .mode("overwrite")
      .json(jsonOutput)

    println("JSON output written to: " + jsonOutput)

    // 4. Write Parquet output
    val parquetOutput = "data/output/parquet_sales"

    salesWithDateParts
      .coalesce(1)
      .write
      .mode("overwrite")
      .parquet(parquetOutput)

    println("Parquet output written to: " + parquetOutput)

    // 5. Repartition before writing
    val repartitionedSales = salesWithDateParts
      .repartition(3)

    println("\n========== REPARTITION ==========")
    println(s"Partitions after repartition(3): ${repartitionedSales.rdd.getNumPartitions}")

    val repartitionOutput = "data/output/repartitioned_sales"

    repartitionedSales
      .write
      .mode("overwrite")
      .parquet(repartitionOutput)

    println("Repartitioned Parquet output written to: " + repartitionOutput)

    // 6. Write partitioned output by year/month/day
    val partitionedOutput = "data/output/daily_sales_partitioned"

    salesWithDateParts
      .repartition(3, col("year"), col("month"), col("day"))
      .write
      .mode("overwrite")
      .partitionBy("year", "month", "day")
      .parquet(partitionedOutput)

    println("\n========== PARTITIONED OUTPUT ==========")
    println("Partitioned output written to: " + partitionedOutput)
    println("Partition columns: year/month/day")

    // 7. Explain partition layout and number of output partitions
    println("\n========== FILE LAYOUT EXPLANATION ==========")
    println("CSV, JSON and Parquet are written as directories containing part files.")
    println("Each Spark partition can produce an output part file.")
    println("coalesce(1) reduces the output to approximately one part file.")
    println("repartition(3) creates three Spark partitions before writing.")
    println("partitionBy(year, month, day) creates directory partitions based on date.")

    println("\n========== PARTITIONED DATA LAYOUT ==========")
    println("daily_sales_partitioned/")
    println("  year=2026/")
    println("    month=9/")
    println("      day=1/")
    println("      day=2/")
    println("      day=3/")
    println("      day=4/")
    println("      day=5/")

    // 8. Read Parquet back
    val parquetReadDF = spark.read.parquet(parquetOutput)

    println("\n========== READ PARQUET OUTPUT ==========")
    parquetReadDF.show(false)
    println(s"Parquet records read: ${parquetReadDF.count()}")

    // 9. Read JSON back
    val jsonReadDF = spark.read.json(jsonOutput)

    println("\n========== READ JSON OUTPUT ==========")
    jsonReadDF.show(5, false)
    println(s"JSON records read: ${jsonReadDF.count()}")

    // 10. Read partitioned Parquet output
    val partitionedReadDF = spark.read
      .parquet(partitionedOutput)

    println("\n========== READ PARTITIONED PARQUET ==========")
    partitionedReadDF
      .select(
        "sale_id",
        "sale_date",
        "product",
        "amount",
        "year",
        "month",
        "day"
      )
      .show(false)

    println(s"Partitioned records read: ${partitionedReadDF.count()}")

    println("\n========== DAY 20 COMPLETED ==========")

    spark.stop()
  }
}