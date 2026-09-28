import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day-18-Joins")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // ------------------------------------------------------------
    // 1. Read the three datasets
    // ------------------------------------------------------------

    val ordersDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/orders.csv")

    val customersDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/customers.csv")

    val paymentsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/payments.csv")

    println("\n========== ORDERS ==========")
    ordersDF.show(false)

    println("\n========== CUSTOMERS ==========")
    customersDF.show(false)

    println("\n========== PAYMENTS ==========")
    paymentsDF.show(false)

    // ------------------------------------------------------------
    // 2. INNER JOIN
    // ------------------------------------------------------------

    val innerJoin = ordersDF
      .join(
        customersDF,
        ordersDF("customer_id") === customersDF("customer_id"),
        "inner"
      )
      .select(
        ordersDF("order_id"),
        ordersDF("customer_id"),
        customersDF("customer_name"),
        customersDF("city"),
        ordersDF("product"),
        ordersDF("order_amount")
      )

    println("\n========== INNER JOIN: ORDERS + CUSTOMERS ==========")
    innerJoin.show(false)

    // ------------------------------------------------------------
    // 3. LEFT JOIN
    // ------------------------------------------------------------

    val leftJoin = ordersDF
      .join(
        customersDF,
        ordersDF("customer_id") === customersDF("customer_id"),
        "left"
      )
      .select(
        ordersDF("order_id"),
        ordersDF("customer_id"),
        customersDF("customer_name"),
        customersDF("city"),
        ordersDF("product"),
        ordersDF("order_amount")
      )

    println("\n========== LEFT JOIN: ORDERS + CUSTOMERS ==========")
    leftJoin.show(false)

    // ------------------------------------------------------------
    // 4. Null handling after LEFT JOIN
    // ------------------------------------------------------------

    val leftJoinNullHandled = leftJoin
      .withColumn(
        "customer_name",
        coalesce(col("customer_name"), lit("Unknown Customer"))
      )
      .withColumn(
        "city",
        coalesce(col("city"), lit("Unknown City"))
      )

    println("\n========== LEFT JOIN WITH NULL HANDLING ==========")
    leftJoinNullHandled.show(false)

    // ------------------------------------------------------------
    // 5. RIGHT JOIN
    // ------------------------------------------------------------

    val rightJoin = ordersDF
      .join(
        customersDF,
        ordersDF("customer_id") === customersDF("customer_id"),
        "right"
      )
      .select(
        customersDF("customer_id").alias("customer_id"),
        customersDF("customer_name"),
        customersDF("city"),
        ordersDF("order_id"),
        ordersDF("product"),
        ordersDF("order_amount")
      )

    println("\n========== RIGHT JOIN: ORDERS + CUSTOMERS ==========")
    rightJoin.show(false)

    // ------------------------------------------------------------
    // 6. FULL OUTER JOIN
    // ------------------------------------------------------------

    val fullJoin = ordersDF
      .join(
        customersDF,
        ordersDF("customer_id") === customersDF("customer_id"),
        "full"
      )
      .select(
        coalesce(
          ordersDF("customer_id"),
          customersDF("customer_id")
        ).alias("customer_id"),
        ordersDF("order_id"),
        customersDF("customer_name"),
        customersDF("city"),
        ordersDF("product"),
        ordersDF("order_amount")
      )

    println("\n========== FULL OUTER JOIN ==========")
    fullJoin.show(false)

    // ------------------------------------------------------------
    // 7. Aliases for ambiguous column names
    // ------------------------------------------------------------

    val orders = ordersDF.alias("o")
    val customers = customersDF.alias("c")

    val aliasedJoin = orders
      .join(
        customers,
        col("o.customer_id") === col("c.customer_id"),
        "inner"
      )
      .select(
        col("o.order_id").alias("order_id"),
        col("o.customer_id").alias("order_customer_id"),
        col("c.customer_id").alias("customer_customer_id"),
        col("c.customer_name"),
        col("o.product"),
        col("o.order_amount")
      )

    println("\n========== JOIN USING ALIASES ==========")
    aliasedJoin.show(false)

    // ------------------------------------------------------------
    // 8. Join orders + customers + payments
    // ------------------------------------------------------------

    val ordersAlias = ordersDF.alias("o")
    val customersAlias = customersDF.alias("c")
    val paymentsAlias = paymentsDF.alias("p")

    val completeJoin = ordersAlias
      .join(
        customersAlias,
        col("o.customer_id") === col("c.customer_id"),
        "left"
      )
      .join(
        paymentsAlias,
        col("o.order_id") === col("p.order_id"),
        "left"
      )
      .select(
        col("o.order_id"),
        col("o.customer_id"),
        col("c.customer_name"),
        col("c.city"),
        col("o.product"),
        col("o.quantity"),
        col("o.order_amount"),
        col("p.payment_mode"),
        col("p.payment_status"),
        col("p.payment_amount")
      )

    println("\n========== ORDERS + CUSTOMERS + PAYMENTS ==========")
    completeJoin.show(false)

    // ------------------------------------------------------------
    // 9. Handle missing customer and payment information
    // ------------------------------------------------------------

    val finalOrderData = completeJoin
      .withColumn(
        "customer_name",
        coalesce(col("customer_name"), lit("Unknown Customer"))
      )
      .withColumn(
        "city",
        coalesce(col("city"), lit("Unknown City"))
      )
      .withColumn(
        "payment_mode",
        coalesce(col("payment_mode"), lit("Not Available"))
      )
      .withColumn(
        "payment_status",
        coalesce(col("payment_status"), lit("Not Available"))
      )
      .withColumn(
        "payment_amount",
        coalesce(col("payment_amount"), lit(0))
      )

    println("\n========== FINAL ORDER DATA WITH NULL HANDLING ==========")
    finalOrderData.show(false)

    // ------------------------------------------------------------
    // 10. Orders without payment
    // ------------------------------------------------------------

    val unpaidOrders = finalOrderData
      .filter(col("payment_status") === "Not Available")

    println("\n========== ORDERS WITHOUT PAYMENT RECORD ==========")
    unpaidOrders.show(false)

    // ------------------------------------------------------------
    // 11. Payment status summary
    // ------------------------------------------------------------

    val paymentSummary = finalOrderData
      .groupBy("payment_status")
      .agg(
        count("*").alias("order_count"),
        sum("order_amount").alias("total_order_amount"),
        sum("payment_amount").alias("total_payment_amount")
      )
      .orderBy("payment_status")

    println("\n========== PAYMENT STATUS SUMMARY ==========")
    paymentSummary.show(false)

    // ------------------------------------------------------------
    // 12. Shuffle Sort Merge Join explanation
    // ------------------------------------------------------------

    println("\n========== SHUFFLE SORT MERGE JOIN ==========")
    println("Shuffle Sort Merge Join is a common Spark join strategy.")
    println("Spark shuffles matching join keys across partitions.")
    println("The shuffled data is sorted by the join key.")
    println("Spark then merges matching sorted records.")
    println("It is useful for large datasets when a broadcast join is not suitable.")

    println("\n========== JOIN TYPES ==========")
    println("Inner Join : Returns only matching records from both datasets.")
    println("Left Join  : Returns all records from the left dataset and matching records from the right.")
    println("Right Join : Returns all records from the right dataset and matching records from the left.")
    println("Full Join  : Returns all records from both datasets.")
    println("Aliases    : Help avoid ambiguous column references after joins.")
    println("coalesce() : Replaces NULL values with a specified fallback value.")

    println("\n========== DAY 18 COMPLETED ==========")

    spark.stop()
  }
}