trait Logger {
  def log(message: String): Unit
}

class ConsoleLogger extends Logger {
  override def log(message: String): Unit = {
    println(s"[INFO] $message")
  }
}

class DetailedLogger extends Logger {
  override def log(message: String): Unit = {
    println(s"[DETAIL] $message")
  }
}
case class Student(name: String, marks: Int)

object Main {


  def calculateGrade(marks: Int): String = {
  if (marks >= 90) "A"
  else if (marks >= 75) "B"
  else if (marks >= 60) "C"
  else if (marks >= 50) "D"
  else "F"
  }
  def main(args: Array[String]): Unit = {

    // 1. val - immutable variable
    val studentName = "Mohit"
    println(s"Student Name: $studentName")

    // 2. var - mutable variable
    var studentAge = 21
    println(s"Initial Age: $studentAge")

    studentAge = 22
    println(s"Updated Age: $studentAge")

    // 3. lazy val
    lazy val studentMessage = {
      println("lazy val is being evaluated...")
      "Welcome to Scala Day 1"
    }

    println("Before accessing lazy val")
    println(studentMessage)
    println(studentMessage)

    // 4. Immutable Collections
    val numbers = List(10, 20, 30, 40, 50)

    println("\n--- Immutable Collections ---")
    println(s"Original List: $numbers")

    val doubledNumbers = numbers.map(_ * 2)

    println(s"Doubled List: $doubledNumbers")
    println(s"Original List After map: $numbers")


        // 5. For-comprehension with yield

    val students = List(
      ("Arun", 85),
      ("Priya", 92),
      ("Rahul", 68),
      ("Sneha", 95)
    )

    println("\n--- For-Comprehension with Yield ---")

    val passedStudents = for {
      (name, marks) <- students
      if marks >= 70
    } yield name

    println(s"Passed Students: $passedStudents")


        // 6. List, Vector, Set and Map

    println("\n--- List, Vector, Set and Map ---")

    // List - ordered collection
    val studentList = List("Arun", "Priya", "Rahul", "Sneha")
    println(s"List: $studentList")
    println(s"First student: ${studentList.head}")

    // Vector - indexed collection
    val studentVector = Vector("Arun", "Priya", "Rahul", "Sneha")
    println(s"Vector: $studentVector")
    println(s"Student at index 2: ${studentVector(2)}")

    // Set - stores unique values
    val studentSet = Set("Arun", "Priya", "Rahul", "Sneha", "Arun")
    println(s"Set: $studentSet")
    println(s"Set size: ${studentSet.size}")

    // Map - stores key-value pairs
    val studentMarks = Map(
      "Arun" -> 85,
      "Priya" -> 92,
      "Rahul" -> 68,
      "Sneha" -> 95
    )

    println(s"Map: $studentMarks")
    println(s"Priya's marks: ${studentMarks("Priya")}")


        // 7. Logger Trait

    println("\n--- Logger Trait ---")

    val consoleLogger = new ConsoleLogger
    val detailedLogger = new DetailedLogger

    consoleLogger.log("Student processing started")

    detailedLogger.log("Student marks are being processed")



        // 8. Student Grade Processor

    println("\n--- Student Grade Processor ---")

    val studentRecords = List(
      Student("Arun", 85),
      Student("Priya", 92),
      Student("Rahul", 68),
      Student("Sneha", 95),
      Student("Kiran", 45)
    )

    val gradedStudents = studentRecords.map { student =>
      val grade = calculateGrade(student.marks)
      (student.name, student.marks, grade)
    }

    gradedStudents.foreach {
      case (name, marks, grade) =>
        println(s"$name -> Marks: $marks, Grade: $grade")
    }

    val totalMarks = studentRecords.map(_.marks).sum

    val averageMarks =
      totalMarks.toDouble / studentRecords.size

    println(s"Total Marks: $totalMarks")
    println(f"Average Marks: $averageMarks%.2f")
  }
}