import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 15 - UDF Practice")
      .master("local[2]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("=== Day 15 - UDF Practice ===")

    // --------------------------------------------------
    // 1. Read Customer Transaction Data
    // --------------------------------------------------

    val customerDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/customers.csv")

    println("\n=== Customer Data ===")
    customerDF.show()

    println("\n=== Customer Schema ===")
    customerDF.printSchema()

    // --------------------------------------------------
    // 2. Salary Band UDF
    // --------------------------------------------------

    val salaryBandUDF = udf { salary: Double =>
      if (salary >= 80000) {
        "High"
      } else if (salary >= 50000) {
        "Medium"
      } else {
        "Low"
      }
    }

    val salaryDF = customerDF.withColumn(
      "salary_band",
      salaryBandUDF(col("monthly_salary"))
    )

    println("\n=== Salary Band UDF ===")
    salaryDF.show()

    // --------------------------------------------------
    // 3. Customer Risk Category UDF
    // --------------------------------------------------

    val riskCategoryUDF = udf { transactionAmount: Double =>
      if (transactionAmount >= 80000) {
        "High Risk"
      } else if (transactionAmount >= 30000) {
        "Medium Risk"
      } else {
        "Low Risk"
      }
    }

    val riskDF = salaryDF.withColumn(
      "risk_category",
      riskCategoryUDF(col("transaction_amount"))
    )

    println("\n=== Customer Risk Category ===")
    riskDF.show()

    // --------------------------------------------------
    // 4. Multiple Calculated Columns
    // --------------------------------------------------

    val calculatedDF = riskDF
      .withColumn(
        "annual_salary",
        col("monthly_salary") * 12
      )
      .withColumn(
        "transaction_to_salary_ratio",
        round(
          col("transaction_amount") / col("monthly_salary"),
          2
        )
      )

    println("\n=== Calculated Columns ===")
    calculatedDF.show()

    // --------------------------------------------------
    // 5. UDF vs Built-in Spark Function
    // --------------------------------------------------

    println("\n=== UDF vs Built-in Function ===")

    val upperNameUDF = udf { name: String =>
    name.toUpperCase
    }

    val udfUpperDF = calculatedDF.withColumn(
    "name_upper_udf",
    upperNameUDF(col("customer_name"))
    )

    println("\nUsing UDF for uppercase:")
    udfUpperDF
      .select("customer_name", "name_upper_udf")
      .show()

    val builtInUpperDF = calculatedDF.withColumn(
      "name_upper_builtin",
      upper(col("customer_name"))
    )

    println("Using Spark built-in upper():")
    builtInUpperDF
      .select("customer_name", "name_upper_builtin")
      .show()

    println("Built-in Spark functions are generally preferred")
    println("when an equivalent built-in function is available.")

    // --------------------------------------------------
    // 6. Register UDF with Spark SQL
    // --------------------------------------------------

    spark.udf.register(
      "salary_band",
      (salary: Double) => {
        if (salary >= 80000) {
          "High"
        } else if (salary >= 50000) {
          "Medium"
        } else {
          "Low"
        }
      }
    )

    calculatedDF.createOrReplaceTempView("customers")

    println("\n=== Registered UDF with Spark SQL ===")

    spark.sql(
      """
        |SELECT
        |  customer_id,
        |  customer_name,
        |  monthly_salary,
        |  salary_band(monthly_salary) AS salary_band
        |FROM customers
        |ORDER BY customer_id
        |""".stripMargin
    ).show()

    // --------------------------------------------------
    // 7. SQL Risk Analysis
    // --------------------------------------------------

    println("\n=== SQL Customer Risk Analysis ===")

    spark.udf.register(
      "risk_category",
      (amount: Double) => {
        if (amount >= 80000) {
          "High Risk"
        } else if (amount >= 30000) {
          "Medium Risk"
        } else {
          "Low Risk"
        }
      }
    )

    spark.sql(
      """
        |SELECT
        |  customer_id,
        |  customer_name,
        |  transaction_amount,
        |  risk_category(transaction_amount) AS risk_category
        |FROM customers
        |ORDER BY transaction_amount DESC
        |""".stripMargin
    ).show()

    // --------------------------------------------------
    // 8. Customer Risk Summary
    // --------------------------------------------------

    println("\n=== Customer Risk Summary ===")

    riskDF
      .groupBy("risk_category")
      .count()
      .orderBy("risk_category")
      .show()

    // --------------------------------------------------
    // 9. Salary Band Summary
    // --------------------------------------------------

    println("\n=== Salary Band Summary ===")

    salaryDF
      .groupBy("salary_band")
      .count()
      .orderBy("salary_band")
      .show()

    // --------------------------------------------------
    // 10. UDF Explanation
    // --------------------------------------------------

    println("\n=== UDF Concepts ===")

    println(
      "UDF -> User Defined Function used to apply custom logic to Spark data."
    )

    println(
      "withColumn() -> Adds a calculated or transformed column."
    )

    println(
      "spark.udf.register() -> Registers a UDF for use in Spark SQL."
    )

    println(
      "Built-in functions -> Preferred when Spark already provides the required operation."
    )

    println(
      "UDFs are useful when custom business logic cannot be expressed easily using built-in functions."
    )

    println("\n=== Application Completed Successfully ===")

    spark.stop()
  }
}