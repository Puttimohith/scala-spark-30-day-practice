import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

case class Employee(
    id: Int,
    name: String,
    department: String,
    monthly_salary: Double
)

case class EmployeePayroll(
    id: Int,
    name: String,
    department: String,
    monthly_salary: Double,
    annual_salary: Double,
    tax: Double,
    net_annual_salary: Double
)

object Main {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 14 - DataFrame and Dataset")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    println("=== Day 14 - DataFrame and Dataset ===")

    // --------------------------------------------------
    // 1. Create DataFrame from CSV
    // --------------------------------------------------

    val employeeDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/employees.csv")

    println("\n=== Employee DataFrame ===")
    employeeDF.show()

    println("\n=== DataFrame Schema ===")
    employeeDF.printSchema()

    // --------------------------------------------------
    // 2. Convert DataFrame to Dataset
    // --------------------------------------------------

    val employeeDS: Dataset[Employee] =
      employeeDF.as[Employee]

    println("\n=== Dataset[Employee] ===")
    employeeDS.show()

    println("\nDataset Type:")
    println(employeeDS.getClass.getSimpleName)

    // --------------------------------------------------
    // 3. Convert Dataset back to DataFrame
    // --------------------------------------------------

    val employeeDFAgain = employeeDS.toDF()

    println("\n=== Dataset Converted Back to DataFrame ===")
    employeeDFAgain.show()

    // --------------------------------------------------
    // 4. DataFrame select and filter
    // --------------------------------------------------

    println("\n=== Selected Columns ===")

    employeeDF
      .select("id", "name", "department", "monthly_salary")
      .show()

    println("\n=== Engineering Employees ===")

    employeeDF
      .filter(col("department") === "Engineering")
      .select("id", "name", "monthly_salary")
      .show()

    // --------------------------------------------------
    // 5. withColumn and expressions
    // --------------------------------------------------

    val salaryDF = employeeDF
      .withColumn(
        "annual_salary",
        col("monthly_salary") * 12
      )
      .withColumn(
        "annual_tax",
        col("monthly_salary") * 12 * 0.10
      )

    println("\n=== Salary Calculations ===")
    salaryDF.show()

    // --------------------------------------------------
    // 6. Typed Employee Payroll Pipeline
    // --------------------------------------------------

    val payrollDS: Dataset[EmployeePayroll] =
      employeeDS.map { employee =>

        val annualSalary =
          employee.monthly_salary * 12

        val tax =
          annualSalary * 0.10

        val netAnnualSalary =
          annualSalary - tax

        EmployeePayroll(
          employee.id,
          employee.name,
          employee.department,
          employee.monthly_salary,
          annualSalary,
          tax,
          netAnnualSalary
        )
      }

    println("\n=== Typed Employee Payroll Dataset ===")

    payrollDS.show()

    println("\n=== Employee Payroll Report ===")

    payrollDS
      .select(
        "id",
        "name",
        "department",
        "annual_salary",
        "tax",
        "net_annual_salary"
      )
      .show()

    // --------------------------------------------------
    // 7. Temporary View and SQL
    // --------------------------------------------------

    employeeDF.createOrReplaceTempView("employees")

    println("\n=== SQL: Engineering Payroll ===")

    spark.sql(
      """
        |SELECT
        |  id,
        |  name,
        |  department,
        |  monthly_salary,
        |  monthly_salary * 12 AS annual_salary
        |FROM employees
        |WHERE department = 'Engineering'
        |ORDER BY annual_salary DESC
        |""".stripMargin
    ).show()

    // --------------------------------------------------
    // 8. Payroll Analytics
    // --------------------------------------------------

    println("\n=== Department Payroll Analytics ===")

    payrollDS
      .groupBy("department")
      .agg(
        count("*").alias("employee_count"),
        round(avg("annual_salary"), 2).alias("average_annual_salary"),
        round(sum("annual_salary"), 2).alias("total_annual_salary")
      )
      .orderBy("department")
      .show()

    // --------------------------------------------------
    // 9. RDD, DataFrame and Dataset comparison
    // --------------------------------------------------

    println("\n=== RDD vs DataFrame vs Dataset ===")

    println("RDD:")
    println("- Low-level distributed collection.")
    println("- More control over data processing.")
    println("- Less optimized than DataFrame/Dataset APIs.")

    println("\nDataFrame:")
    println("- Distributed data organized into named columns.")
    println("- Schema-based and optimized by Spark SQL.")
    println("- Suitable for structured data processing.")

    println("\nDataset:")
    println("- Provides DataFrame-style optimization with type safety.")
    println("- Uses case classes for strongly typed records.")
    println("- Available in Scala and Java.")

    // --------------------------------------------------
    // 10. Type Safety and Catalyst Optimization
    // --------------------------------------------------

    println("\n=== Type Safety ===")

    println(
      "Dataset provides compile-time type safety through case classes."
    )

    println(
      "The Employee case class defines the expected fields and data types."
    )

    println("\n=== Catalyst Optimization ===")

    println(
      "Spark SQL uses the Catalyst optimizer to optimize DataFrame and Dataset queries."
    )

    println(
      "Catalyst can analyze and optimize the logical execution plan before execution."
    )

    println("\n=== Typed Payroll Pipeline ===")

    println("CSV")
    println("  ↓")
    println("DataFrame")
    println("  ↓ as[Employee]")
    println("Dataset[Employee]")
    println("  ↓ typed map()")
    println("Dataset[EmployeePayroll]")
    println("  ↓")
    println("Payroll Reports")

    println("\n=== Day 14 Concepts ===")

    println("DataFrame -> Distributed data organized into named columns.")
    println("Dataset -> Strongly typed distributed collection.")
    println("case class -> Defines the schema and types for Dataset records.")
    println("as[Employee] -> Converts DataFrame to Dataset[Employee].")
    println("toDF() -> Converts Dataset back to DataFrame.")
    println("Type Safety -> Compile-time checking of Dataset operations.")
    println("Catalyst -> Spark SQL query optimization framework.")

    println("\n=== Application Completed Successfully ===")

    spark.stop()
  }
}