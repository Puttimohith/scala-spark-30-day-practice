import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

case class Student(
  student_id: Int,
  student_name: String,
  course: String,
  marks: Int,
  exam_date: String
)

case class CustomerPolicy(
  customer_id: Int,
  customer_name: String,
  policy_type: String,
  policy_amount: Double,
  policy_date: String
)

object Main {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day-17-Window-Functions")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    // ------------------------------------------------------------
    // 1. Read student data
    // ------------------------------------------------------------

    val studentsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/students.csv")

    println("\n========== STUDENT DATA ==========")
    studentsDF.show(false)

    // ------------------------------------------------------------
    // 2. row_number(), rank(), dense_rank()
    // ------------------------------------------------------------

    val marksWindow = Window
      .partitionBy("course")
      .orderBy(desc("marks"))

    val rankedStudents = studentsDF
      .withColumn("row_number", row_number().over(marksWindow))
      .withColumn("rank", rank().over(marksWindow))
      .withColumn("dense_rank", dense_rank().over(marksWindow))

    println("\n========== ROW_NUMBER, RANK, DENSE_RANK ==========")
    rankedStudents
      .select(
        "student_id",
        "student_name",
        "course",
        "marks",
        "row_number",
        "rank",
        "dense_rank"
      )
      .orderBy("course", "row_number")
      .show(false)

    // ------------------------------------------------------------
    // 3. Top 3 students per course
    // ------------------------------------------------------------

    val top3Students = rankedStudents
      .filter(col("row_number") <= 3)

    println("\n========== TOP 3 STUDENTS PER COURSE ==========")
    top3Students
      .select(
        "course",
        "student_id",
        "student_name",
        "marks",
        "row_number"
      )
      .orderBy("course", "row_number")
      .show(false)

    // ------------------------------------------------------------
    // 4. Demonstrate lag() and lead()
    // ------------------------------------------------------------

    val studentSequenceWindow = Window
      .partitionBy("course")
      .orderBy("marks", "student_id")

    val studentComparison = studentsDF
      .withColumn("previous_marks", lag("marks", 1).over(studentSequenceWindow))
      .withColumn("next_marks", lead("marks", 1).over(studentSequenceWindow))

    println("\n========== LAG AND LEAD ==========")
    studentComparison
      .select(
        "course",
        "student_name",
        "marks",
        "previous_marks",
        "next_marks"
      )
      .orderBy("course", "marks", "student_id")
      .show(false)

    // ------------------------------------------------------------
    // 5. Latest record per customer
    // ------------------------------------------------------------

    val customerPolicyData = Seq(
      CustomerPolicy(201, "Ramesh", "Health", 500000, "2026-08-10"),
      CustomerPolicy(201, "Ramesh", "Life", 1000000, "2026-09-15"),
      CustomerPolicy(201, "Ramesh", "Vehicle", 300000, "2026-07-20"),
      CustomerPolicy(202, "Suresh", "Health", 400000, "2026-08-25"),
      CustomerPolicy(202, "Suresh", "Life", 800000, "2026-09-18"),
      CustomerPolicy(203, "Kavya", "Vehicle", 250000, "2026-07-12"),
      CustomerPolicy(203, "Kavya", "Health", 450000, "2026-09-05"),
      CustomerPolicy(204, "Priya", "Life", 900000, "2026-08-30"),
      CustomerPolicy(204, "Priya", "Health", 550000, "2026-09-20"),
      CustomerPolicy(205, "Arun", "Vehicle", 350000, "2026-08-15")
    ).toDF()

    println("\n========== CUSTOMER POLICY DATA ==========")
    customerPolicyData.show(false)

    val customerWindow = Window
      .partitionBy("customer_id")
      .orderBy(desc("policy_date"))

    val latestPolicy = customerPolicyData
      .withColumn("row_number", row_number().over(customerWindow))
      .filter(col("row_number") === 1)
      .drop("row_number")

    println("\n========== LATEST POLICY PER CUSTOMER ==========")
    latestPolicy
      .orderBy("customer_id")
      .show(false)

    // ------------------------------------------------------------
    // 6. Partition examples: department / customer / route
    // ------------------------------------------------------------

    val departmentData = Seq(
      ("Engineering", "Arun", 60000),
      ("Engineering", "Rahul", 55000),
      ("Engineering", "Kiran", 70000),
      ("HR", "Priya", 45000),
      ("HR", "Anjali", 48000),
      ("Finance", "Manoj", 65000),
      ("Finance", "Ravi", 52000)
    ).toDF("department", "employee", "salary")

    val departmentWindow = Window
      .partitionBy("department")
      .orderBy(desc("salary"))

    val departmentRanking = departmentData
      .withColumn("department_rank", rank().over(departmentWindow))

    println("\n========== PARTITION BY DEPARTMENT ==========")
    departmentRanking.show(false)

    val routeData = Seq(
      ("R1", "Vehicle-A", 120),
      ("R1", "Vehicle-B", 150),
      ("R2", "Vehicle-C", 90),
      ("R2", "Vehicle-D", 110),
      ("R3", "Vehicle-E", 180)
    ).toDF("route", "vehicle", "distance")

    val routeWindow = Window
      .partitionBy("route")
      .orderBy(desc("distance"))

    val routeRanking = routeData
      .withColumn("route_rank", rank().over(routeWindow))

    println("\n========== PARTITION BY ROUTE ==========")
    routeRanking.show(false)

    // ------------------------------------------------------------
    // 7. Concepts summary
    // ------------------------------------------------------------

    println("\n========== WINDOW FUNCTION CONCEPTS ==========")
    println("row_number() : Assigns a unique sequential number within each partition.")
    println("rank()       : Assigns the same rank to tied values and leaves gaps.")
    println("dense_rank() : Assigns the same rank to tied values without gaps.")
    println("partitionBy(): Divides data into independent groups for window processing.")
    println("lag()        : Accesses a previous row within the window.")
    println("lead()       : Accesses a following row within the window.")
    println("Top 3        : Filter row_number <= 3 after partitioning by course.")
    println("Latest       : Order customer records by date descending and select row_number = 1.")

    println("\n========== DAY 17 COMPLETED ==========")

    spark.stop()
  }
}