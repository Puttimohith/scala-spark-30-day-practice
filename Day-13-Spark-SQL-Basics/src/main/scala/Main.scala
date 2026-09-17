import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 13 - Spark SQL Basics")
      .master("local[2]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("=== Day 13 - Spark SQL Basics ===")

    // --------------------------------------------------
    // 1. Create DataFrame from CSV
    // --------------------------------------------------

    val customersDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/customers.csv")

    println("\n=== Customer DataFrame ===")
    customersDF.show()

    // --------------------------------------------------
    // 2. Inspect Schema
    // --------------------------------------------------

    println("\n=== DataFrame Schema ===")
    customersDF.printSchema()

    // --------------------------------------------------
    // Select Columns
    // --------------------------------------------------

    println("\n=== Selected Columns ===")

    customersDF
      .select("customer_id", "name", "city", "total_spend")
      .show()

    // --------------------------------------------------
    // Filter Customers
    // --------------------------------------------------

    println("\n=== Customers with Spend > ₹50000 ===")

    customersDF
      .filter(col("total_spend") > 50000)
      .select("customer_id", "name", "city", "total_spend")
      .show()

    // --------------------------------------------------
    // 3. withColumn and Expressions
    // --------------------------------------------------

    val customersWithCategoryDF = customersDF
      .withColumn(
        "spend_category",
        when(col("total_spend") >= 70000, "High")
          .when(col("total_spend") >= 50000, "Medium")
          .otherwise("Low")
      )

    println("\n=== Customers with Spend Category ===")

    customersWithCategoryDF
      .select(
        "customer_id",
        "name",
        "total_spend",
        "spend_category"
      )
      .show()

    val customersWithAgeGroupDF = customersWithCategoryDF
      .withColumn(
        "age_group",
        when(col("age") < 25, "Young")
          .when(col("age") <= 35, "Adult")
          .otherwise("Senior")
      )

    println("\n=== Customers with Age Group ===")

    customersWithAgeGroupDF
      .select(
        "customer_id",
        "name",
        "age",
        "age_group"
      )
      .show()

    // --------------------------------------------------
    // 4. Temporary View and SQL
    // --------------------------------------------------

    customersWithAgeGroupDF.createOrReplaceTempView("customers")

    println("\n=== SQL: Customers from Hyderabad ===")

    val hyderabadCustomers = spark.sql(
      """
        SELECT customer_id, name, city, total_spend
        FROM customers
        WHERE city = 'Hyderabad'
      """
    )

    hyderabadCustomers.show()

    println("\n=== SQL: Top Spending Customers ===")

    val topCustomers = spark.sql(
      """
        SELECT customer_id, name, city, total_spend
        FROM customers
        ORDER BY total_spend DESC
        LIMIT 5
      """
    )

    topCustomers.show()

    // --------------------------------------------------
    // 5. Customer Analytics Report
    // --------------------------------------------------

    println("\n=== Customer Analytics Report ===")

    val citySummary = spark.sql(
      """
        SELECT
          city,
          COUNT(*) AS customer_count,
          ROUND(AVG(total_spend), 2) AS average_spend,
          ROUND(SUM(total_spend), 2) AS total_revenue
        FROM customers
        GROUP BY city
        ORDER BY total_revenue DESC
      """
    )

    citySummary.show()

    println("\n=== Spend Category Summary ===")

    val spendSummary = spark.sql(
      """
        SELECT
          spend_category,
          COUNT(*) AS customer_count,
          ROUND(AVG(total_spend), 2) AS average_spend
        FROM customers
        GROUP BY spend_category
        ORDER BY average_spend DESC
      """
    )

    spendSummary.show()

    println("\n=== Customer Analytics Using SQL ===")

    val analyticsReport = spark.sql(
      """
        SELECT
          city,
          spend_category,
          COUNT(*) AS customers,
          ROUND(AVG(total_spend), 2) AS average_spend,
          ROUND(SUM(total_spend), 2) AS total_spend
        FROM customers
        GROUP BY city, spend_category
        ORDER BY city, spend_category
      """
    )

    analyticsReport.show()

    // --------------------------------------------------
    // Concepts Summary
    // --------------------------------------------------

    println("\n=== Day 13 Concepts ===")

    println("DataFrame -> Distributed table-like data structure.")
    println("select() -> Selects required columns.")
    println("filter() -> Filters rows based on conditions.")
    println("withColumn() -> Creates or replaces a column.")
    println("Temporary View -> Allows SQL queries on a DataFrame.")
    println("spark.sql() -> Executes SQL queries using Spark SQL.")
    println("Customer Analytics -> Groups and summarizes customer data.")

    println("\n=== Application Completed Successfully ===")

    spark.stop()
  }
}