import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel

object Main {

  def main(args: Array[String]): Unit = {

    // ============================================================
    // 1. CREATE SPARK SESSION
    // ============================================================

    val spark = SparkSession.builder()
      .appName("Day-29-End-to-End-E-Commerce-Project")
      .master("local[2]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // ============================================================
    // 2. RAW DATA INGESTION
    // ============================================================

    println("======================================================")
    println("DAY 29 - END-TO-END E-COMMERCE PROJECT")
    println("======================================================")

    val customersRaw = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/customers.csv")

    val productsRaw = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/products.csv")

    val ordersRaw = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/orders.csv")

    println("\nRAW DATA COUNTS")
    println(s"Customers: ${customersRaw.count()}")
    println(s"Products : ${productsRaw.count()}")
    println(s"Orders   : ${ordersRaw.count()}")

    // ============================================================
    // 3. CLEAN CUSTOMERS
    // ============================================================

    val customersClean = customersRaw
      .select(
        trim(col("customer_id")).alias("customer_id"),
        trim(col("name")).alias("name"),
        trim(col("city")).alias("city"),
        upper(trim(col("segment"))).alias("segment")
      )
      .filter(
        col("customer_id").isNotNull &&
        col("name").isNotNull
      )

    // ============================================================
    // 4. CLEAN PRODUCTS
    // ============================================================

    val productsClean = productsRaw
      .select(
        trim(col("product_id")).alias("product_id"),
        trim(col("product_name")).alias("product_name"),
        trim(col("category")).alias("category"),
        col("price").cast("double").alias("price")
      )
      .filter(
        col("product_id").isNotNull &&
        col("price").isNotNull &&
        col("price") > 0
      )

    // ============================================================
    // 5. CLEAN ORDERS
    // ============================================================

    val ordersClean = ordersRaw
      .select(
        trim(col("order_id")).alias("order_id"),
        trim(col("customer_id")).alias("customer_id"),
        trim(col("product_id")).alias("product_id"),
        col("quantity").cast("int").alias("quantity"),
        to_date(
          trim(col("order_date")),
          "yyyy-MM-dd"
        ).alias("order_date"),
        upper(trim(col("status"))).alias("status")
      )
      .filter(
        col("order_id").isNotNull &&
        col("customer_id").isNotNull &&
        col("product_id").isNotNull &&
        col("quantity").isNotNull &&
        col("quantity") > 0 &&
        col("order_date").isNotNull
      )

    println("\nCLEAN DATA COUNTS")
    println(s"Customers: ${customersClean.count()}")
    println(s"Products : ${productsClean.count()}")
    println(s"Orders   : ${ordersClean.count()}")

    // ============================================================
    // 6. PAIR RDD COMPONENT
    // ============================================================

    val categoryPriceRDD = productsClean.rdd
      .map { row =>
        (
          row.getAs[String]("category"),
          row.getAs[Double]("price")
        )
      }
      .reduceByKey(_ + _)

    println("\nPAIR RDD RESULT")

    categoryPriceRDD
      .sortByKey()
      .collect()
      .foreach {
        case (category, totalPrice) =>
          println(
            f"$category%-15s -> $totalPrice%.2f"
          )
      }

    println(
      s"Pair RDD partitions: ${categoryPriceRDD.getNumPartitions}"
    )

    // ============================================================
    // 7. JOIN ORDERS WITH CUSTOMERS
    // ============================================================

    val ordersWithCustomers = ordersClean
      .join(
        customersClean,
        ordersClean("customer_id") ===
          customersClean("customer_id"),
        "inner"
      )
      .select(
        ordersClean("order_id"),
        ordersClean("customer_id"),
        ordersClean("product_id"),
        ordersClean("quantity"),
        ordersClean("order_date"),
        ordersClean("status"),
        customersClean("name"),
        customersClean("city"),
        customersClean("segment")
      )

    // ============================================================
    // 8. JOIN WITH PRODUCTS
    // ============================================================

    val enrichedOrders = ordersWithCustomers
      .join(
        productsClean,
        ordersWithCustomers("product_id") ===
          productsClean("product_id"),
        "inner"
      )
      .select(
        ordersWithCustomers("order_id"),
        ordersWithCustomers("customer_id"),
        ordersWithCustomers("name"),
        ordersWithCustomers("city"),
        ordersWithCustomers("segment"),
        ordersWithCustomers("product_id"),
        productsClean("product_name"),
        productsClean("category"),
        productsClean("price"),
        ordersWithCustomers("quantity"),
        ordersWithCustomers("order_date"),
        ordersWithCustomers("status")
      )
      .withColumn(
        "order_value",
        col("quantity") * col("price")
      )

    println("\nENRICHED ORDERS")

    enrichedOrders.show(false)

    // ============================================================
    // 9. FILTER COMPLETED ORDERS
    // ============================================================

    val completedOrders = enrichedOrders
      .filter(col("status") === "COMPLETED")
      .persist(StorageLevel.MEMORY_ONLY)

    println(
      s"\nCompleted Orders: ${completedOrders.count()}"
    )

    // ============================================================
    // 10. WINDOW FUNCTIONS
    // ============================================================

    val customerWindow =
      Window
        .partitionBy("customer_id")
        .orderBy(
          col("order_date"),
          col("order_id")
        )

    val windowedOrders = completedOrders
      .withColumn(
        "running_customer_sales",
        sum("order_value").over(customerWindow)
      )
      .withColumn(
        "customer_order_number",
        row_number().over(customerWindow)
      )

    println("\nWINDOW FUNCTION RESULT")

    windowedOrders
      .select(
        "order_id",
        "customer_id",
        "name",
        "order_date",
        "order_value",
        "running_customer_sales",
        "customer_order_number"
      )
      .orderBy(
        "customer_id",
        "order_date",
        "order_id"
      )
      .show(false)

    // ============================================================
    // 11. CUSTOMER AGGREGATION
    // ============================================================

    val customerSales = completedOrders
      .groupBy(
        "customer_id",
        "name",
        "segment"
      )
      .agg(
        count("order_id").alias("total_orders"),
        sum("quantity").alias("total_quantity"),
        round(
          sum("order_value"),
          2
        ).alias("total_sales")
      )
      .orderBy(desc("total_sales"))

    println("\nCUSTOMER SALES")

    customerSales.show(false)

    // ============================================================
    // 12. CATEGORY AGGREGATION
    // ============================================================

    val categorySales = completedOrders
      .groupBy("category")
      .agg(
        count("order_id").alias("total_orders"),
        sum("quantity").alias("total_quantity"),
        round(
          sum("order_value"),
          2
        ).alias("total_sales")
      )
      .orderBy(desc("total_sales"))

    println("\nCATEGORY SALES")

    categorySales.show(false)

    // ============================================================
    // 13. PARTITIONING
    // ============================================================

    val partitionedOrders = completedOrders
      .repartition(
        2,
        col("customer_id")
      )
      .persist(StorageLevel.MEMORY_ONLY)

    println(
      s"\nPartitioned Orders Partitions: ${partitionedOrders.rdd.getNumPartitions}"
    )

    // ============================================================
    // 14. PARTITION DISTRIBUTION
    // ============================================================
    // Important:
    // Do not convert the DateType rows to external Java objects.
    // We only count records inside each partition.

    println("\nPARTITION DISTRIBUTION")

    val partitionCounts = partitionedOrders
      .select("order_id", "customer_id")
      .rdd
      .mapPartitionsWithIndex {
        case (partitionId, iterator) =>

          var count = 0

          while (iterator.hasNext) {
            iterator.next()
            count += 1
          }

          Iterator(
            s"Partition $partitionId -> $count records"
          )
      }
      .collect()

    partitionCounts.foreach(println)

    // ============================================================
    // 15. SPARK SQL REPORTING
    // ============================================================

    partitionedOrders
      .createOrReplaceTempView("completed_orders")

    val sqlReport = spark.sql(
      """
        SELECT
          category,
          COUNT(order_id) AS total_orders,
          SUM(quantity) AS total_quantity,
          ROUND(SUM(order_value), 2) AS total_sales
        FROM completed_orders
        GROUP BY category
        ORDER BY total_sales DESC
      """
    )

    println("\nSPARK SQL REPORT")

    sqlReport.show(false)

    // ============================================================
    // 16. EXECUTION PLAN
    // ============================================================

    println("\n======================================================")
    println("EXECUTION PLAN - CATEGORY SALES")
    println("======================================================")

    categorySales.explain("formatted")

    // ============================================================
    // 17. SHUFFLE / STAGE / OPTIMIZATION EXPLANATION
    // ============================================================

    println("\n======================================================")
    println("SHUFFLE, STAGE AND OPTIMIZATION EXPLANATION")
    println("======================================================")

    println(
      """
1. SHUFFLES
   - reduceByKey causes a shuffle because records with the same
     category key must be brought together.
   - Joins may cause shuffle when Spark redistributes records
     according to join keys.
   - groupBy operations cause shuffle because records must be
     grouped by their keys.
   - Window functions require partitioning and sorting based on
     the window specification.
   - repartition(2, customer_id) intentionally redistributes
     records using customer_id.

2. STAGES
   - Spark divides execution into stages at shuffle boundaries.
   - Narrow transformations can execute within the same stage.
   - Shuffle operations create stage boundaries.
   - Each stage is executed using tasks.

3. PERSISTENCE
   - completedOrders is persisted because it is reused by
     multiple downstream operations.
   - This avoids recomputing the same upstream transformations.

4. PARTITIONING
   - repartition(2, customer_id) distributes records according
     to customer_id.
   - This can help later customer-based processing.

5. OPTIMIZATION
   - Only required columns are selected.
   - Invalid records are filtered during cleaning.
   - Data types are converted before processing.
   - COMPLETED orders are filtered before downstream analytics.
   - Persistence is used for reused data.
   - Spark SQL/DataFrame operations benefit from Catalyst
     optimization and Tungsten execution.

6. UDF
   - No custom UDF is required.
   - Built-in Spark functions are sufficient for this project.
"""
    )

    // ============================================================
    // 18. FINAL SUMMARY
    // ============================================================

    println("\n======================================================")
    println("FINAL PROJECT SUMMARY")
    println("======================================================")

    println("Pipeline:")
    println("RAW -> CLEAN -> ENRICH -> AGGREGATE")

    println("\nRDD Component:")
    println("Product category price aggregation using Pair RDD")

    println("\nDataFrame Component:")
    println("Cleaning, joins, enrichment, aggregation and windows")

    println("\nWindow Operations:")
    println("Running customer sales and customer order number")

    println("\nPartitioning:")
    println("repartition(2, customer_id)")

    println("\nPersistence:")
    println("MEMORY_ONLY")

    println("\nSQL Reporting:")
    println("Category-wise completed order sales")

    println("\nUDF:")
    println("Not required; built-in Spark functions are sufficient")

    println("\nProject completed successfully.")

    // ============================================================
    // 19. CLEANUP
    // ============================================================

    completedOrders.unpersist()
    partitionedOrders.unpersist()

    spark.stop()
  }
}