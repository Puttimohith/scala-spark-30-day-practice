# Day 27 — Real-Time Banking Project

## Overview

This project demonstrates a real-time banking transaction processing system using Apache Spark Streaming and Scala.

The project processes transaction events, calculates account-wise transaction totals, detects suspicious transaction bursts using window operations, joins transactions with branch risk information, and demonstrates caching, partitioning, and YARN execution concepts.

## Objectives

- Design a transaction event schema.
- Process real-time banking transactions.
- Aggregate transactions by account.
- Detect suspicious transaction bursts using window operations.
- Join transactions with small branch risk reference data.
- Demonstrate cache/persist.
- Demonstrate RDD partitioning.
- Explain how Spark Streaming can run on YARN.

## Technologies Used

- Scala 2.12.18
- Apache Spark 3.5.6
- Spark Core
- Spark Streaming
- sbt
- WSL Ubuntu
- Git and GitHub

## Project Structure

Day-27-Real-Time-Banking-Project/
├── data/
│   ├── branch_risk.txt
│   └── transactions.txt
├── output.txt
├── project/
│   └── build.properties
├── src/
│   └── main/
│       └── scala/
│           └── Main.scala
├── .gitignore
├── build.sbt
└── README.md

## 1. Transaction Event Schema

Each transaction contains:

- Transaction ID
- Account ID
- Branch ID
- Transaction amount
- Transaction time

Example:

TX001,ACC001,BR001,500,10:00

The Scala case class used is:

case class Transaction(
  transactionId: String,
  accountId: String,
  branchId: String,
  amount: Double,
  time: String
)

## 2. Transaction Aggregation

Transactions are grouped by account ID and the transaction amounts are aggregated.

Example output:

ACC001 -> total transaction amount = 1200.00
ACC002 -> total transaction amount = 1200.00
ACC003 -> total transaction amount = 300.00

This demonstrates account-wise aggregation using Spark transformations.

## 3. Window Processing

A 10-second window with a 5-second sliding interval is used.

The window helps identify accounts that generate many transactions within a short period.

Example:

ACC001 -> 4 transactions in 10-second window
SUSPICIOUS BURST ALERT: ACC001 has 4 transactions in the window.

This demonstrates how window operations can be used for real-time fraud or suspicious-activity detection.

## 4. Branch Risk Join

A small branch-risk reference dataset is used:

BR001,LOW
BR002,MEDIUM
BR003,HIGH

Transactions are joined with the branch risk information using the branch ID.

Example:

TX001 -> ACC001, BR001, 500.00, Risk=LOW
TX002 -> ACC002, BR002, 1200.00, Risk=MEDIUM
TX004 -> ACC003, BR003, 300.00, Risk=HIGH

This demonstrates joining streaming transaction data with small reference data.

## 5. Cache and Persist

Transaction RDDs are persisted in memory using Spark's persistence mechanism.

The purpose is to avoid recomputing the same RDD when it is used by multiple operations.

The project verifies the cached RDD partitions:

Cached transaction RDD partitions: 2

## 6. Partitioning

The transaction RDD uses multiple partitions so that Spark can process data in parallel.

Output:

Current transaction RDD partitions: 2

Partitions allow Spark to distribute processing work across executors in a cluster environment.

## 7. YARN Execution

In a production cluster, Spark can run on YARN.

YARN manages cluster resources while Spark executors process the application tasks.

### YARN Flow

Client submits application
        ↓
YARN ResourceManager allocates resources
        ↓
ApplicationMaster starts
        ↓
Spark Executors run tasks
        ↓
Results are returned

## 8. Sample Input

The project processes banking transactions such as:

TX001,ACC001,BR001,500,10:00
TX002,ACC002,BR002,1200,10:01
TX003,ACC001,BR001,700,10:02
TX004,ACC003,BR003,300,10:03
TX005,ACC001,BR001,900,10:04
TX006,ACC002,BR002,1500,10:05
TX007,ACC001,BR001,1100,10:06
TX008,ACC003,BR003,400,10:07
TX009,ACC001,BR001,1300,10:08
TX010,ACC002,BR002,800,10:09
TX011,ACC001,BR001,1400,10:10
TX012,ACC003,BR003,600,10:11

## 9. Execution

Run the project using:

sbt run

For a clean output proof:

sbt run 2>&1 | grep -E '^==========|^TX[0-9]+|^ACC[0-9]+|^SUSPICIOUS|^Cached|^Current transaction|^Aggregation:|^Window Processing:|^Join:|^Cache/Persist:|^Partitioning:|^YARN:|^YARN Flow:|^StreamingContext started|^DAY 27 COMPLETED' > output.txt

View the output:

cat output.txt

## 10. Actual Execution Results

### Transaction Totals

The application successfully calculated account-wise transaction totals for each streaming batch.

First batch:

ACC001 -> total transaction amount = 1200.00
ACC002 -> total transaction amount = 1200.00
ACC003 -> total transaction amount = 300.00

Second batch:

ACC001 -> total transaction amount = 2000.00
ACC002 -> total transaction amount = 1500.00
ACC003 -> total transaction amount = 400.00

Third batch:

ACC001 -> total transaction amount = 2700.00
ACC002 -> total transaction amount = 800.00
ACC003 -> total transaction amount = 600.00

### Suspicious Burst Detection

The application successfully detected suspicious bursts for ACC001:

ACC001 -> 4 transactions in 10-second window
SUSPICIOUS BURST ALERT: ACC001 has 4 transactions in the window.

### Branch Risk Join

The application successfully joined transactions with branch risk information:

TX005 -> ACC001, BR001, 900.00, Risk=LOW
TX006 -> ACC002, BR002, 1500.00, Risk=MEDIUM
TX007 -> ACC001, BR001, 1100.00, Risk=LOW
TX008 -> ACC003, BR003, 400.00, Risk=HIGH

### Partition Verification

Cached transaction RDD partitions: 2
Current transaction RDD partitions: 2

## 11. Key Concepts Learned

### Aggregation

Aggregation combines multiple transaction records based on an account and calculates the total transaction amount.

### Window Processing

Window processing analyzes data over a fixed time interval. A sliding interval determines how frequently the window moves forward.

### Join

A join combines transaction information with related branch risk information using a common key.

### Cache/Persist

Caching or persistence keeps computed data available for reuse and can reduce repeated computation.

### Partitioning

Partitioning divides data into smaller pieces so Spark can process them in parallel.

### YARN

YARN is a cluster resource manager. It allocates resources for Spark applications and manages the execution environment.

## 12. Conclusion

The Day 27 Real-Time Banking Project successfully demonstrates a Spark Streaming based banking analytics workflow.

The project covers transaction schema design, account-wise aggregation, window-based suspicious activity detection, branch-risk joins, caching, partitioning, and YARN execution concepts.

The application completed successfully with:

========== DAY 27 COMPLETED ==========
