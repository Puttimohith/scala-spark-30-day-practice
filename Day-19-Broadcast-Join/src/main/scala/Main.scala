import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day-19-Broadcast-Join")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // ------------------------------------------------------------
    // 1. Read the small branch master DataFrame
    // ------------------------------------------------------------

    val branchMasterDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/branch_master.csv")

    println("\n========== BRANCH MASTER ==========")
    branchMasterDF.show(false)

    println(s"Branch master records: ${branchMasterDF.count()}")

    // ------------------------------------------------------------
    // 2. Create a large fact DataFrame
    // ------------------------------------------------------------

    val transactionsDF = spark.range(1, 100001)
      .select(
        col("id").alias("transaction_id"),
        concat(
          lit("B"),
          lpad(((col("id") % 10) + 1).cast("string"), 3, "0")
        ).alias("branch_id"),
        (col("id") * 10).alias("transaction_amount")
      )

    println("\n========== LARGE TRANSACTIONS ==========")
    transactionsDF.show(10, false)

    println(s"Transaction records: ${transactionsDF.count()}")

    // ------------------------------------------------------------
    // 3. Normal join
    // ------------------------------------------------------------

    val normalJoin = transactionsDF
      .join(
        branchMasterDF,
        transactionsDF("branch_id") === branchMasterDF("branch_id"),
        "inner"
      )
      .select(
        transactionsDF("transaction_id"),
        transactionsDF("branch_id"),
        transactionsDF("transaction_amount"),
        branchMasterDF("branch_name"),
        branchMasterDF("city"),
        branchMasterDF("region")
      )

    println("\n========== NORMAL JOIN ==========")
    normalJoin.show(10, false)

    // ------------------------------------------------------------
    // 4. Broadcast Join
    // ------------------------------------------------------------

    val broadcastJoin = transactionsDF
      .join(
        broadcast(branchMasterDF),
        transactionsDF("branch_id") === branchMasterDF("branch_id"),
        "inner"
      )
      .select(
        transactionsDF("transaction_id"),
        transactionsDF("branch_id"),
        transactionsDF("transaction_amount"),
        branchMasterDF("branch_name"),
        branchMasterDF("city"),
        branchMasterDF("region")
      )

    println("\n========== BROADCAST JOIN ==========")
    broadcastJoin.show(10, false)

    // ------------------------------------------------------------
    // 5. Verify broadcast join plan
    // ------------------------------------------------------------

    println("\n========== BROADCAST JOIN EXECUTION PLAN ==========")
    broadcastJoin.explain()

    // ------------------------------------------------------------
    // 6. Branch-wise transaction metrics
    // ------------------------------------------------------------

    val branchMetrics = broadcastJoin
      .groupBy(
        "branch_id",
        "branch_name",
        "city",
        "region"
      )
      .agg(
        count("*").alias("transaction_count"),
        sum("transaction_amount").alias("total_transaction_amount"),
        avg("transaction_amount").alias("average_transaction_amount")
      )
      .orderBy("branch_id")

    println("\n========== BRANCH-WISE TRANSACTION METRICS ==========")
    branchMetrics.show(false)

    // ------------------------------------------------------------
    // 7. Region-wise transaction metrics
    // ------------------------------------------------------------

    val regionMetrics = broadcastJoin
      .groupBy("region")
      .agg(
        count("*").alias("transaction_count"),
        sum("transaction_amount").alias("total_transaction_amount"),
        avg("transaction_amount").alias("average_transaction_amount")
      )
      .orderBy("region")

    println("\n========== REGION-WISE TRANSACTION METRICS ==========")
    regionMetrics.show(false)

    // ------------------------------------------------------------
    // 8. Compare Broadcast Join with Shuffle Sort Merge Join
    // ------------------------------------------------------------

    println("\n========== JOIN COMPARISON ==========")
    println("Broadcast Join:")
    println("- Small reference DataFrame is broadcast to executors.")
    println("- Avoids shuffling the large transaction DataFrame for this join.")
    println("- Useful when one side of the join is small enough to fit in executor memory.")

    println("\nShuffle Sort Merge Join:")
    println("- Join keys are shuffled across partitions.")
    println("- Data is sorted by the join key.")
    println("- Matching sorted records are merged.")
    println("- Suitable for large-to-large joins when broadcasting is not appropriate.")

    // ------------------------------------------------------------
    // 9. When Broadcast Join is appropriate
    // ------------------------------------------------------------

    println("\n========== WHEN TO USE BROADCAST JOIN ==========")
    println("1. One DataFrame is significantly smaller than the other.")
    println("2. The small DataFrame can fit comfortably in executor memory.")
    println("3. The large DataFrame does not need to be shuffled for the join.")
    println("4. Common examples include branch, store, product, country and department master data.")

    println("\n========== DAY 19 COMPLETED ==========")

    spark.stop()
  }
}