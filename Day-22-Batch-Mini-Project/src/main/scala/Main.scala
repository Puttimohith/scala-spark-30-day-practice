import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day-22-Batch-Mini-Project")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // --------------------------------------------------
    // 1. Read raw transaction data
    // --------------------------------------------------

    val transactionsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/transactions.csv")

    val customersDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/customers.csv")

    val productsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/products.csv")

    println("\n========== RAW TRANSACTIONS ==========")
    transactionsDF.show(false)

    println(s"Raw transaction count: ${transactionsDF.count()}")

    println("\n========== CUSTOMERS ==========")
    customersDF.show(false)

    println("\n========== PRODUCTS ==========")
    productsDF.show(false)

    // --------------------------------------------------
    // 2. Clean invalid transaction records
    // --------------------------------------------------

    val cleanedTransactionsDF = transactionsDF
      .filter(col("status") === "COMPLETED")
      .filter(col("quantity") > 0)
      .filter(col("unit_price") > 0)
      .withColumn(
        "revenue",
        col("quantity") * col("unit_price")
      )

    println("\n========== CLEANED TRANSACTIONS ==========")
    cleanedTransactionsDF.show(false)

    println(
      s"Cleaned transaction count: ${cleanedTransactionsDF.count()}"
    )

    println(
      s"Invalid records removed: ${transactionsDF.count() - cleanedTransactionsDF.count()}"
    )

    // --------------------------------------------------
    // 3. Join transactions with customer data
    // --------------------------------------------------

    val transactionCustomerDF = cleanedTransactionsDF
      .join(
        customersDF,
        Seq("customer_id"),
        "inner"
      )

    println("\n========== TRANSACTIONS + CUSTOMERS ==========")
    transactionCustomerDF.show(false)

    // --------------------------------------------------
    // 4. Join with product data
    // --------------------------------------------------

    val enrichedSalesDF = transactionCustomerDF
      .join(
        productsDF,
        Seq("product_id"),
        "inner"
      )

    println("\n========== ENRICHED SALES DATA ==========")
    enrichedSalesDF.show(false)

    // --------------------------------------------------
    // 5. Aggregate revenue
    // --------------------------------------------------

    val dailyRevenueDF = enrichedSalesDF
      .groupBy(
        col("transaction_date"),
        col("city")
      )
      .agg(
        sum("quantity").alias("total_quantity"),
        sum("revenue").alias("total_revenue"),
        countDistinct("transaction_id").alias("transaction_count")
      )
      .orderBy(
        col("transaction_date"),
        desc("total_revenue")
      )

    println("\n========== DAILY REVENUE BY CITY ==========")
    dailyRevenueDF.show(false)

    // --------------------------------------------------
    // 6. Product category revenue
    // --------------------------------------------------

    val categoryRevenueDF = enrichedSalesDF
      .groupBy("category")
      .agg(
        sum("quantity").alias("total_quantity"),
        sum("revenue").alias("total_revenue"),
        countDistinct("transaction_id").alias("transaction_count")
      )
      .orderBy(desc("total_revenue"))

    println("\n========== REVENUE BY CATEGORY ==========")
    categoryRevenueDF.show(false)

    // --------------------------------------------------
    // 7. Customer revenue
    // --------------------------------------------------

    val customerRevenueDF = enrichedSalesDF
      .groupBy(
        "customer_id",
        "customer_name",
        "segment"
      )
      .agg(
        sum("revenue").alias("total_revenue"),
        countDistinct("transaction_id").alias("transaction_count")
      )
      .orderBy(desc("total_revenue"))

    println("\n========== REVENUE BY CUSTOMER ==========")
    customerRevenueDF.show(false)

    // --------------------------------------------------
    // 8. Write partitioned Parquet output
    // --------------------------------------------------

    dailyRevenueDF
      .write
      .mode("overwrite")
      .partitionBy("transaction_date")
      .parquet("data/output/daily_revenue")

    println("\n========== OUTPUT ==========")
    println(
      "Partitioned Parquet output written to: data/output/daily_revenue"
    )

    // --------------------------------------------------
    // 9. Read the generated Parquet output
    // --------------------------------------------------

    val outputDF = spark.read
      .parquet("data/output/daily_revenue")

    println("\n========== PARQUET OUTPUT ==========")
    outputDF.show(false)

    println(
      s"Parquet output records: ${outputDF.count()}"
    )

    // --------------------------------------------------
    // 10. Pipeline completion
    // --------------------------------------------------

    println("\n========== PIPELINE SUMMARY ==========")
    println(s"Raw transactions       : ${transactionsDF.count()}")
    println(s"Clean transactions     : ${cleanedTransactionsDF.count()}")
    println(
      s"Invalid records removed: ${transactionsDF.count() - cleanedTransactionsDF.count()}"
    )
    println(s"Customers joined       : ${customersDF.count()}")
    println(s"Products joined        : ${productsDF.count()}")
    println(
      s"Enriched sales records : ${enrichedSalesDF.count()}"
    )

    println("\n========== DAY 22 COMPLETED ==========")

    spark.stop()
  }
}