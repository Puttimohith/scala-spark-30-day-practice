import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 16 - Aggregations")
      .master("local[2]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("=== Day 16 - Aggregations ===")

    // --------------------------------------------------
    // 1. Read Hospital Revenue Data
    // --------------------------------------------------

    val hospitalDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/hospital_revenue.csv")

    println("\n=== Hospital Revenue Data ===")
    hospitalDF.show()

    println("\n=== Schema ===")
    hospitalDF.printSchema()

    // --------------------------------------------------
    // 2. Basic Aggregations
    // --------------------------------------------------

    println("\n=== Basic Aggregations ===")

    hospitalDF.agg(
      count("*").alias("total_records"),
      sum("patients").alias("total_patients"),
      avg("average_bill").alias("average_bill"),
      min("average_bill").alias("minimum_bill"),
      max("average_bill").alias("maximum_bill")
    ).show()

    // --------------------------------------------------
    // 3. Individual Aggregation Functions
    // --------------------------------------------------

    println("\n=== count() ===")
    println(s"Total records: ${hospitalDF.count()}")

    println("\n=== sum() ===")
    hospitalDF
      .select(sum("patients").alias("total_patients"))
      .show()

    println("\n=== avg() ===")
    hospitalDF
      .select(avg("average_bill").alias("average_bill"))
      .show()

    println("\n=== min() ===")
    hospitalDF
      .select(min("average_bill").alias("minimum_bill"))
      .show()

    println("\n=== max() ===")
    hospitalDF
      .select(max("average_bill").alias("maximum_bill"))
      .show()

    // --------------------------------------------------
    // 4. GroupBy Department
    // --------------------------------------------------

    println("\n=== Department-wise Salary/Revenue Statistics ===")

    val departmentStats = hospitalDF
      .groupBy("department")
      .agg(
        count("*").alias("records"),
        sum("patients").alias("total_patients"),
        avg("average_bill").alias("avg_bill"),
        min("average_bill").alias("min_bill"),
        max("average_bill").alias("max_bill")
      )
      .orderBy("department")

    departmentStats.show()

    // --------------------------------------------------
    // 5. GroupBy Multiple Columns
    // --------------------------------------------------

    println("\n=== GroupBy Department and Patient Type ===")

    val departmentPatientTypeStats = hospitalDF
      .groupBy("department", "patient_type")
      .agg(
        count("*").alias("records"),
        sum("patients").alias("total_patients"),
        avg("average_bill").alias("avg_bill")
      )
      .orderBy("department", "patient_type")

    departmentPatientTypeStats.show()

    // --------------------------------------------------
    // 6. Revenue Calculation
    // --------------------------------------------------

    val revenueDF = hospitalDF.withColumn(
      "revenue",
      col("patients") * col("average_bill")
    )

    println("\n=== Revenue Calculation ===")

    revenueDF
      .select(
        "department",
        "doctor",
        "patient_type",
        "patients",
        "average_bill",
        "revenue"
      )
      .show()

    // --------------------------------------------------
    // 7. Department Revenue Metrics
    // --------------------------------------------------

    println("\n=== Hospital Department Revenue Metrics ===")

    val departmentRevenue = revenueDF
      .groupBy("department")
      .agg(
        sum("patients").alias("total_patients"),
        sum("revenue").alias("total_revenue"),
        avg("revenue").alias("average_revenue"),
        min("revenue").alias("minimum_revenue"),
        max("revenue").alias("maximum_revenue")
      )
      .orderBy(desc("total_revenue"))

    departmentRevenue.show()

    // --------------------------------------------------
    // 8. HAVING-like Filtering
    // --------------------------------------------------

    println("\n=== HAVING-like Filtering ===")

    println("Departments with total revenue greater than ₹500000:")

    departmentRevenue
      .filter(col("total_revenue") > 500000)
      .show()

    // --------------------------------------------------
    // 9. Patient Volume Filtering
    // --------------------------------------------------

    println("\n=== Departments with More Than 50 Patients ===")

    departmentStats
      .filter(col("total_patients") > 50)
      .show()

    // --------------------------------------------------
    // 10. Doctor-wise Revenue
    // --------------------------------------------------

    println("\n=== Doctor-wise Revenue ===")

    revenueDF
      .groupBy("doctor", "department")
      .agg(
        sum("patients").alias("total_patients"),
        sum("revenue").alias("total_revenue")
      )
      .orderBy(desc("total_revenue"))
      .show()

    // --------------------------------------------------
    // 11. Multiple Aggregation Columns
    // --------------------------------------------------

    println("\n=== Multiple Aggregations by Patient Type ===")

    hospitalDF
      .groupBy("patient_type")
      .agg(
        count("*").alias("records"),
        sum("patients").alias("total_patients"),
        avg("average_bill").alias("average_bill"),
        min("average_bill").alias("minimum_bill"),
        max("average_bill").alias("maximum_bill")
      )
      .orderBy("patient_type")
      .show()

    // --------------------------------------------------
    // 12. Aggregation Concepts
    // --------------------------------------------------

    println("\n=== Aggregation Concepts ===")

    println("count() -> Counts the number of records.")
    println("sum()   -> Calculates the total of a numeric column.")
    println("avg()   -> Calculates the average value.")
    println("min()   -> Finds the minimum value.")
    println("max()   -> Finds the maximum value.")

    println("\ngroupBy() -> Groups records before aggregation.")

    println(
      "HAVING-like filtering -> Filter aggregated results using filter() after groupBy()."
    )

    println(
      "Department revenue -> patients multiplied by average_bill."
    )

    // --------------------------------------------------
    // 13. Hospital Revenue Pipeline
    // --------------------------------------------------

    println("\n=== Hospital Revenue Pipeline ===")

    println("Hospital CSV")
    println("     ↓")
    println("DataFrame")
    println("     ↓")
    println("Calculate Revenue")
    println("     ↓")
    println("Group by Department")
    println("     ↓")
    println("count / sum / avg / min / max")
    println("     ↓")
    println("HAVING-like Filtering")
    println("     ↓")
    println("Department Revenue Metrics")

    println("\n=== Day 16 Concepts ===")

    println("Basic Aggregations -> count, sum, avg, min, max")
    println("Multiple Grouping -> groupBy(department, patient_type)")
    println("HAVING-like Filter -> filter() after aggregation")
    println("Revenue -> patients * average_bill")
    println("Department Metrics -> Total, average, minimum and maximum revenue")

    println("\n=== Application Completed Successfully ===")

    spark.stop()
  }
}