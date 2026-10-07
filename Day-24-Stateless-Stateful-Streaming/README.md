# Day 24 — Stateless vs Stateful Streaming

## Assignment

Implement stateless and stateful processing using Apache Spark DStreams.

### Requirements

1. Implement stateless transformations.
2. Explain stateful processing conceptually.
3. Track running counts by key.
4. Compare current-batch results with accumulated state.
5. Scenario: Maintain running transaction counts per bank account.

---

## Objective

The objective of this exercise is to understand the difference between stateless and stateful stream processing using Apache Spark Streaming.

The project processes transaction data for different bank accounts and maintains transaction counts for each account.

---

## Technologies Used

- Scala 2.12.18
- Apache Spark 3.5.6
- Spark Streaming
- sbt
- WSL Ubuntu

---

## Project Structure

    Day-24-Stateless-Stateful-Streaming/
    │
    ├── src/
    │   └── main/
    │       └── scala/
    │           └── Main.scala
    │
    ├── data/
    │   ├── transactions_batch_01.txt
    │   └── transactions_batch_02.txt
    │
    ├── output/
    │   └── output.txt
    │
    ├── screenshots/
    │
    ├── project/
    │   └── build.properties
    │
    ├── build.sbt
    ├── .gitignore
    └── README.md

---

## 1. Spark Configuration

A Spark Streaming Context is created with a batch interval of 5 seconds.

    val conf = new SparkConf()
      .setAppName("Day-24-Stateless-Stateful-Streaming")
      .setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(5))

The 5-second batch interval means Spark checks for new streaming data every 5 seconds.

---

## 2. Checkpointing

Stateful processing requires checkpointing.

    ssc.checkpoint("data/checkpoint")

Checkpointing allows Spark Streaming to maintain state information across batches.

The checkpoint directory is ignored by Git because it contains runtime-generated files.

---

## 3. Streaming Source

The application reads transaction files from:

    data/stream_input

Each transaction follows this format:

    account_id,amount

Example:

    ACC001,100
    ACC002,200
    ACC001,50
    ACC003,300
    ACC002,150
    ACC001,75

---

## 4. Transaction Validation

The incoming transaction records are validated before processing.

The program checks:

- The record contains two fields.
- The account ID is not empty.
- The transaction amount is a valid numeric value.

Invalid records are removed using the `filter` transformation.

---

## 5. Creating Key-Value Pairs

Each valid transaction is converted into an account and count pair.

Example:

    ACC001,100 → (ACC001, 1)
    ACC002,200 → (ACC002, 1)
    ACC001,50  → (ACC001, 1)

This allows Spark to count transactions by bank account.

---

# Stateless Processing

## Definition

Stateless processing considers only the data available in the current batch.

It does not remember the results of previous batches.

The project uses:

    reduceByKey(_ + _)

to calculate the number of transactions for each account in the current batch.

---

## Stateless Example

For the first batch:

    ACC001,100
    ACC002,200
    ACC001,50
    ACC003,300
    ACC002,150
    ACC001,75

The current-batch transaction counts are:

    ACC001 -> 3
    ACC002 -> 2
    ACC003 -> 1

These counts represent only the current batch.

---

# Stateful Processing

## Definition

Stateful processing maintains information across multiple batches.

Instead of calculating only the current batch, Spark maintains previously calculated state and updates it when new data arrives.

The project uses:

    updateStateByKey

to maintain running transaction counts for each bank account.

---

## Stateful Logic

The state update logic is:

    val newCount = newValues.sum
    val previousCount = previousState.getOrElse(0)

    Some(previousCount + newCount)

The previous count and the new count are added together.

Therefore, the state represents the accumulated transaction count for each account.

---

# Current Batch vs Accumulated State

### Current Batch

The stateless result represents only the transactions received in the current batch.

Example:

    ACC001 -> 3
    ACC002 -> 2
    ACC003 -> 1

### Accumulated State

The stateful result maintains the running count for each account.

Example:

    ACC001 -> 3
    ACC002 -> 2
    ACC003 -> 1

If additional batches are processed by the same running application, the state is updated using the previous state plus the new transactions.

---

# Bank Account Scenario

This project represents a simple bank transaction monitoring scenario.

For every transaction:

    Account ID → Transaction

The streaming application counts how many transactions each bank account has generated.

For example:

    ACC001 → 3 transactions
    ACC002 → 2 transactions
    ACC003 → 1 transaction

With stateful processing, these counts can continue increasing as new transaction batches arrive.

This can be useful for:

- Transaction monitoring
- Account activity analysis
- Banking analytics
- Real-time transaction statistics

---

# Execution Output

The verified execution produced the following results:

    ========== STATELESS RESULT ==========
    Current-batch transaction counts:
    ACC001 -> 3
    ACC002 -> 2
    ACC003 -> 1

    ========== STATEFUL RESULT ==========
    Accumulated transaction counts:
    ACC001 -> 3
    ACC002 -> 2
    ACC003 -> 1

    ========== CONCEPT ==========
    Stateless processing considers only the current batch.
    Stateful processing maintains information across batches.
    Running counts are maintained separately for each account.

    ========== DAY 24 COMPLETED ==========

The complete saved execution output is available in:

    output/output.txt

---

# Stateless vs Stateful Comparison

| Feature | Stateless | Stateful |
|---|---|---|
| Current batch | Yes | Yes |
| Previous batches | No | Yes |
| Maintains state | No | Yes |
| Running count | No | Yes |
| Spark operation used | reduceByKey | updateStateByKey |
| Example | Current transaction count | Accumulated transaction count |

---

# Key Concepts Learned

## DStream

A DStream is a continuous stream of data represented as a sequence of RDDs.

## Batch Interval

The batch interval defines how frequently Spark creates a new micro-batch.

This project uses:

    5 seconds

## Stateless Processing

Processes only the data in the current batch.

## Stateful Processing

Maintains information across multiple batches.

## updateStateByKey

Used to maintain running state for each key across batches.

## Checkpointing

Required for maintaining state and recovering stateful streaming applications.

---

# How to Run

From the project directory:

    sbt compile

Then run:

    sbt run

The application creates a Spark Streaming Context and waits for transaction files in:

    data/stream_input

---

# Expected Result

The application should display:

    STATELESS RESULT

followed by the current-batch transaction counts.

It should then display:

    STATEFUL RESULT

followed by the accumulated transaction counts.

Finally:

    DAY 24 COMPLETED

---

# Conclusion

Day 24 demonstrated the difference between stateless and stateful stream processing using Apache Spark DStreams.

Stateless processing calculates results only for the current batch, while stateful processing maintains running information across batches.

The bank transaction scenario demonstrated how transaction counts can be maintained separately for each account using `updateStateByKey`.

