# Day 25 — Window Operations

## Objective

The objective of Day 25 is to understand and implement window operations in Apache Spark Streaming using DStreams.

This task covers:

- Batch interval
- Window size
- Sliding interval
- `countByWindow`
- `reduceByKeyAndWindow`
- Rolling sales totals
- Sudden transaction increase detection

---

## Technologies Used

- Scala 2.12.18
- Apache Spark 3.5.6
- Spark Streaming
- SBT
- WSL Ubuntu

---

## Project Structure

```text
Day-25-Window-Operations/
│
├── data/
│   └── transactions.txt
│
├── output/
│
├── screenshots/
│
├── src/
│   └── main/
│       └── scala/
│           └── Main.scala
│
├── project/
│
├── .gitignore
├── build.sbt
├── output.txt
└── README.md
```

---

## Streaming Configuration

The application uses the following configuration:

| Configuration | Value |
|---|---|
| Batch Interval | 5 seconds |
| Window Size | 10 minutes |
| Sliding Interval | 5 seconds |

### Batch Interval

The batch interval is the frequency at which Spark creates micro-batches.

In this project:

```text
Batch interval = 5 seconds
```

This means Spark processes incoming data every 5 seconds.

### Window Size

The window size defines how much historical streaming data is included in each window.

In this project:

```text
Window size = 10 minutes
```

So each window represents transaction data from the latest 10 minutes.

### Sliding Interval

The sliding interval defines how frequently the window moves forward and a new result is calculated.

In this project:

```text
Sliding interval = 5 seconds
```

Therefore, the window is updated every 5 seconds.

---

## Input Data

The project uses transaction data containing account IDs and transaction amounts.

Example:

```text
ACC001,100
ACC002,200
ACC001,150
ACC003,300
ACC002,250
ACC001,200
ACC004,400
ACC002,350
ACC001,500
ACC003,450
ACC002,600
ACC004,550
```

Each record contains:

```text
Account ID, Transaction Amount
```

---

## DStream Creation

A Spark Streaming Context is created with a batch interval of 5 seconds.

The application creates a DStream using a queue of transaction batches.

The transaction stream is then transformed into key-value pairs:

```text
(Account ID, Transaction Amount)
```

This format allows transactions to be aggregated by account.

---

## `countByWindow`

`countByWindow` counts the number of records present in the specified window.

The project uses:

```text
Window size = 10 minutes
Sliding interval = 5 seconds
```

The application checks the number of transactions in the current window.

The observed transaction counts were:

```text
4
8
12
```

When the transaction count reaches 10 or more, the application generates an alert.

Example:

```text
Transactions in 10-minute window: 12
ALERT: Sudden increase in transactions detected!
```

---

## `reduceByKeyAndWindow`

`reduceByKeyAndWindow` performs aggregation by key over the streaming window.

In this project, the account ID is the key and the transaction amount is the value.

Therefore, the application calculates rolling sales totals for every account.

Example:

```text
ACC001 -> $950.00
ACC002 -> $1400.00
ACC003 -> $750.00
ACC004 -> $950.00
```

These values represent the rolling transaction totals for each account in the window.

---

## Sudden Transaction Increase Detection

The project demonstrates a simple transaction monitoring scenario.

The application checks the number of transactions in the 10-minute window.

The condition is:

```text
If transaction count >= 10
    Generate alert
Else
    Normal transaction activity
```

Observed result:

```text
Transactions in 10-minute window: 12
ALERT: Sudden increase in transactions detected!
```

This can be used as a basic model for detecting unusual increases in transaction activity.

---

## Window Processing Results

### First Window

```text
Transactions in 10-minute window: 4
Normal transaction activity.

ACC001 -> $250.00
ACC002 -> $200.00
ACC003 -> $300.00
```

### Second Window

```text
Transactions in 10-minute window: 8
Normal transaction activity.

ACC001 -> $450.00
ACC002 -> $800.00
ACC003 -> $300.00
ACC004 -> $400.00
```

### Third Window

```text
Transactions in 10-minute window: 12
ALERT: Sudden increase in transactions detected!

ACC001 -> $950.00
ACC002 -> $1400.00
ACC003 -> $750.00
ACC004 -> $950.00
```

---

## Important Spark Streaming Concepts

### Batch Interval

Controls how frequently Spark creates micro-batches.

```text
5 seconds
```

### Window Size

Defines the amount of historical data considered for each window.

```text
10 minutes
```

### Sliding Interval

Defines how frequently the window moves and produces a new result.

```text
5 seconds
```

### `countByWindow`

Counts records within a streaming window.

### `reduceByKeyAndWindow`

Performs aggregation by key over a streaming window.

---

## Checkpointing

Window operations require checkpointing for maintaining the necessary streaming state.

The project uses:

```text
data/checkpoint/
```

The checkpoint directory is excluded from Git using `.gitignore`.

---

## How to Run

From the project directory:

```bash
sbt run
```

For clean output verification:

```bash
sbt run 2>&1 | grep -E "DAY 25|CONFIGURATION|DSTREAM CREATED|COUNT BY WINDOW|Transactions in|Rolling sales|ALERT|Normal|Batch interval|Window size|Sliding interval|countByWindow:|reduceByKeyAndWindow:|STREAMING|COMPLETED|ACC00" > output.txt
```

View the saved output:

```bash
cat output.txt
```

---

## Output Verification

The application successfully demonstrated:

- Batch interval configuration
- Window size configuration
- Sliding interval configuration
- DStream creation
- `countByWindow`
- `reduceByKeyAndWindow`
- Rolling sales totals
- Normal transaction activity
- Sudden transaction increase detection
- Streaming start and stop
- Successful completion

Final output:

```text
========== DAY 25 COMPLETED ==========
```

---

## Conclusion

Day 25 demonstrates how Spark Streaming window operations can process recent historical data from a stream.

The project uses `countByWindow` to monitor transaction volume and `reduceByKeyAndWindow` to calculate rolling sales totals for each account.

It also demonstrates a simple alert mechanism for detecting a sudden increase in transactions within a 10-minute window.