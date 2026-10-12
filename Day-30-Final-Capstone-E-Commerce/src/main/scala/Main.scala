import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object Main {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day-30-Final-Capstone-E-Commerce")
      .master("local[2]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("=" * 70)
    println("DAY 30 - FINAL CAPSTONE E-COMMERCE DATA ENGINEERING")
    println("=" * 70)

    // ================================================================
    // 1. READ BATCH DATA
    // ================================================================

    val customers = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/customers.csv")

    val products = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/products.csv")

    val orders = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/orders.csv")

    println("\n--- RAW DATA COUNTS ---")
    println(s"Customers: ${customers.count()}")
    println(s"Products : ${products.count()}")
    println(s"Orders   : ${orders.count()}")

    // ================================================================
    // 2. CLEAN DATA
    // ================================================================

    val cleanCustomers = customers
      .dropDuplicates("customer_id")
      .na.drop()

    val cleanProducts = products
      .dropDuplicates("product_id")
      .na.drop()

    val cleanOrders = orders
      .dropDuplicates("order_id")
      .filter(col("quantity") > 0)
      .na.drop()

    println("\n--- CLEAN DATA COUNTS ---")
    println(s"Customers: ${cleanCustomers.count()}")
    println(s"Products : ${cleanProducts.count()}")
    println(s"Orders   : ${cleanOrders.count()}")

    // ================================================================
    // 3. ACCUMULATOR
    // ================================================================

    val cancelledAccumulator =
      spark.sparkContext.longAccumulator("Cancelled Orders")

    cleanOrders.rdd.foreach { row =>
      if (row.getAs[String]("status") == "CANCELLED") {
        cancelledAccumulator.add(1)
      }
    }

    println("\n--- ACCUMULATOR ---")
    println(
      s"Cancelled orders counted by accumulator: ${cancelledAccumulator.value}"
    )

    // ================================================================
    // 4. BROADCAST VARIABLE
    // ================================================================

    val productPriceMap = cleanProducts.collect()
      .map { row =>
        val productId = row.getAs[String]("product_id")
        val price = row.getAs[Number]("price").doubleValue()
        productId -> price
      }
      .toMap

    val broadcastPrices =
      spark.sparkContext.broadcast(productPriceMap)

    println("\n--- BROADCAST VARIABLE ---")
    println(s"Broadcast product prices: ${broadcastPrices.value}")

    // ================================================================
    // 5. JOIN CUSTOMERS WITH ORDERS
    // ================================================================

    val orderCustomer = cleanOrders.alias("o")
      .join(
        cleanCustomers.alias("c"),
        col("o.customer_id") === col("c.customer_id"),
        "inner"
      )
      .select(
        col("o.order_id").alias("order_id"),
        col("o.customer_id").alias("customer_id"),
        col("c.name").alias("name"),
        col("c.city").alias("city"),
        col("c.segment").alias("segment"),
        col("o.product_id").alias("product_id"),
        col("o.quantity").alias("quantity"),
        col("o.order_date").alias("order_date"),
        col("o.status").alias("status")
      )

    // ================================================================
    // 6. JOIN PRODUCTS
    // ================================================================

    val enrichedOrders = orderCustomer.alias("oc")
      .join(
        cleanProducts.alias("p"),
        col("oc.product_id") === col("p.product_id"),
        "inner"
      )
      .select(
        col("oc.order_id").alias("order_id"),
        col("oc.customer_id").alias("customer_id"),
        col("oc.name").alias("name"),
        col("oc.city").alias("city"),
        col("oc.segment").alias("segment"),
        col("oc.product_id").alias("product_id"),
        col("p.product_name").alias("product_name"),
        col("p.category").alias("category"),
        col("p.price").alias("price"),
        col("oc.quantity").alias("quantity"),
        col("oc.order_date").alias("order_date"),
        col("oc.status").alias("status")
      )
      .withColumn(
        "order_value",
        col("quantity") * col("price")
      )

    println("\n--- ENRICHED ORDERS ---")
    enrichedOrders.show(false)

    // ================================================================
    // 7. UDF
    // ================================================================

    val customerTierUdf = udf { segment: String =>
      if (segment == "Premium") {
        "VIP"
      } else {
        "STANDARD"
      }
    }

    val enrichedWithUdf = enrichedOrders
      .withColumn(
        "customer_tier",
        customerTierUdf(col("segment"))
      )

    println("\n--- UDF RESULT ---")
    enrichedWithUdf
      .select(
        "customer_id",
        "name",
        "segment",
        "customer_tier"
      )
      .show(false)

    // ================================================================
    // 8. FILTER + CACHE/PERSIST
    // ================================================================

    val completedOrders = enrichedWithUdf
      .filter(col("status") === "COMPLETED")
      .persist(StorageLevel.MEMORY_AND_DISK)

    println("\n--- CACHE / PERSIST ---")
    println(s"Completed orders: ${completedOrders.count()}")
    println("Completed orders persisted using MEMORY_AND_DISK")

    // ================================================================
    // 9. WINDOW FUNCTION
    // ================================================================

    val customerWindow = Window
      .partitionBy("customer_id")
      .orderBy(
        col("order_date"),
        col("order_id")
      )

    val windowResult = completedOrders
      .withColumn(
        "running_sales",
        sum("order_value").over(customerWindow)
      )
      .withColumn(
        "customer_order_rank",
        row_number().over(customerWindow)
      )

    println("\n--- WINDOW FUNCTION RESULT ---")

    windowResult
      .select(
        "order_id",
        "customer_id",
        "order_date",
        "order_value",
        "running_sales",
        "customer_order_rank"
      )
      .show(false)

    // ================================================================
    // 10. CUSTOMER AGGREGATION
    // ================================================================

    val customerSales = completedOrders
      .groupBy(
        "customer_id",
        "name",
        "customer_tier"
      )
      .agg(
        sum("order_value").alias("total_sales"),
        count("order_id").alias("total_orders"),
        sum("quantity").alias("total_quantity")
      )
      .orderBy(desc("total_sales"))

    println("\n--- CUSTOMER SALES ---")
    customerSales.show(false)

    // ================================================================
    // 11. CATEGORY AGGREGATION
    // ================================================================

    val categorySales = completedOrders
      .groupBy("category")
      .agg(
        sum("order_value").alias("category_sales"),
        count("order_id").alias("order_count"),
        sum("quantity").alias("quantity_sold")
      )
      .orderBy(desc("category_sales"))

    println("\n--- CATEGORY SALES ---")
    categorySales.show(false)

    // ================================================================
    // 12. PARTITION TUNING
    // ================================================================

    val partitionedOrders = completedOrders
      .repartition(2, col("customer_id"))

    println("\n--- PARTITION TUNING ---")
    println(
      s"Partitions after repartition: ${partitionedOrders.rdd.getNumPartitions}"
    )

    val partitionDistribution =
      partitionedOrders
        .select("order_id", "customer_id")
        .rdd
        .mapPartitionsWithIndex {
          case (partitionId, iterator) =>
            val count = iterator.size
            Iterator(
              s"Partition $partitionId -> $count records"
            )
        }
        .collect()

    partitionDistribution.foreach(println)

    // ================================================================
    // 13. SPARK SQL
    // ================================================================

    completedOrders.createOrReplaceTempView("completed_orders")

    val sqlReport = spark.sql(
      """
        |SELECT
        |    category,
        |    SUM(order_value) AS total_sales,
        |    COUNT(order_id) AS total_orders,
        |    SUM(quantity) AS total_quantity
        |FROM completed_orders
        |GROUP BY category
        |ORDER BY total_sales DESC
        |""".stripMargin
    )

    println("\n--- SPARK SQL REPORT ---")
    sqlReport.show(false)

    // ================================================================
    // 14. BROADCAST JOIN
    // ================================================================
    // Rename product_id on the product side to avoid ambiguity.

    val broadcastProducts = cleanProducts
      .select(
        col("product_id").alias("product_key"),
        col("category").alias("product_category")
      )

    val broadcastJoinResult =
      completedOrders.alias("o")
        .join(
          broadcast(broadcastProducts).alias("p"),
          col("o.product_id") === col("p.product_key"),
          "inner"
        )
        .select(
          col("o.order_id"),
          col("o.product_id"),
          col("o.order_value"),
          col("p.product_category")
        )

    println("\n--- BROADCAST JOIN ---")
    broadcastJoinResult.show(false)

    // ================================================================
    // 15. PHYSICAL PLAN
    // ================================================================

    println("\n--- PHYSICAL PLAN ---")
    categorySales.explain("formatted")

    // ================================================================
    // 16. SPARK EXECUTION CONCEPTS
    // ================================================================

    println("\n--- SPARK EXECUTION CONCEPTS ---")
    println("DAG       : Spark builds a Directed Acyclic Graph of transformations.")
    println("Lineage   : Spark remembers how each dataset was created.")
    println("Stage     : A stage contains tasks separated by shuffle boundaries.")
    println("Shuffle   : Data is redistributed between partitions.")
    println("Executor  : Executes tasks and stores cached data.")
    println("YARN      : Cluster manager that allocates resources in Hadoop environments.")
    println("Optimization: Filter early, persist reused data, tune partitions and use broadcast joins.")

    // ================================================================
    // 17. RELEASE CACHE
    // ================================================================

    completedOrders.unpersist()

    // ================================================================
    // 18. STRUCTURED STREAMING
    // ================================================================

    println("\n--- STRUCTURED STREAMING ---")
    println("Starting streaming processing from data/stream_events")

    val streamSchema = StructType(
      Seq(
        StructField("order_id", StringType, true),
        StructField("customer_id", StringType, true),
        StructField("product_id", StringType, true),
        StructField("quantity", IntegerType, true),
        StructField("event_date", StringType, true),
        StructField("status", StringType, true)
      )
    )

    val streamingOrders = spark.readStream
      .schema(streamSchema)
      .option("header", "true")
      .option("maxFilesPerTrigger", "1")
      .csv("data/stream_events")

    val streamingCompleted = streamingOrders
      .filter(col("status") === "COMPLETED")
      .withColumn(
        "event_value",
        col("quantity") * lit(1)
      )

    val streamingQuery = streamingCompleted
      .select(
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "event_date",
        "event_value"
      )
      .writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")
      .trigger(Trigger.AvailableNow())
      .start()

    streamingQuery.awaitTermination()

    // ================================================================
    // 19. COMPLETION
    // ================================================================

    println("\n" + "=" * 70)
    println("DAY 30 CAPSTONE COMPLETED SUCCESSFULLY")
    println("=" * 70)

    spark.stop()
  }
}