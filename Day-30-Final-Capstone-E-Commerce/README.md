# Day 30 - Final Capstone: E-Commerce Data Engineering

## Objective

Build an end-to-end E-Commerce Data Engineering capstone using Apache Spark and Scala.

The project demonstrates batch processing, Structured Streaming, joins, aggregations, window functions, UDFs, broadcast variables, accumulators, persistence, partition tuning, Spark SQL, physical plan analysis, DAG, lineage, stages, executors, YARN concepts, and Spark optimization.

## Technology Stack

- Scala 2.12.18
- Apache Spark 3.5.6
- Spark Core
- Spark SQL
- Structured Streaming
- SBT 1.10.11
- Java 17
- WSL Ubuntu

## Project Structure

Day-30-Final-Capstone-E-Commerce/
├── data/
│   ├── customers.csv
│   ├── products.csv
│   ├── orders.csv
│   └── stream_events/
│       └── events.csv
├── output/
│   └── output.txt
├── project/
│   └── build.properties
├── src/
│   └── main/
│       └── scala/
│           └── Main.scala
├── .gitignore
├── build.sbt
└── README.md

## Dataset

The project uses customer, product, order, and streaming event data.

### Customers

Columns:

- customer_id
- name
- city
- segment

### Products

Columns:

- product_id
- product_name
- category
- price

### Orders

Columns:

- order_id
- customer_id
- product_id
- quantity
- order_date
- status

### Streaming Events

Columns:

- order_id
- customer_id
- product_id
- quantity
- event_date
- status

## Project Workflow

The project follows this overall workflow:

E-Commerce Data
→ Batch Data + Streaming Data
→ Data Cleaning
→ Data Enrichment
→ UDF
→ Joins
→ Cache/Persist
→ Aggregations
→ Window Functions
→ Spark SQL
→ Partition Tuning
→ Broadcast Join
→ Physical Plan Analysis
→ Structured Streaming Output

## 1. Batch Data Processing

Spark reads the customer, product, and order CSV files using Spark DataFrame APIs.

Raw data counts:

- Customers: 5
- Products: 5
- Orders: 10

## 2. Data Cleaning

The following cleaning operations are performed:

- Remove duplicate customer records.
- Remove duplicate product records.
- Remove duplicate order records.
- Remove orders where quantity is less than or equal to zero.
- Remove null records.

After cleaning:

- Customers: 5
- Products: 5
- Orders: 10

## 3. Accumulator

A Spark LongAccumulator is used to count cancelled orders.

Accumulator name:

Cancelled Orders

Result:

Cancelled orders counted by accumulator: 1

This demonstrates how Spark accumulators can be used for counters during distributed processing.

## 4. Broadcast Variable

A broadcast variable is created for product prices.

Product price mapping:

- P001 → 60000
- P002 → 25000
- P003 → 3000
- P004 → 2000
- P005 → 15000

Broadcast variables allow small read-only data to be efficiently shared with executors.

## 5. Data Enrichment

Orders are joined with customer and product information.

The enriched dataset contains:

- order_id
- customer_id
- name
- city
- segment
- product_id
- product_name
- category
- price
- quantity
- order_date
- status
- order_value

The order value is calculated as:

order_value = quantity × price

## 6. User Defined Function

A Spark UDF is used to create a customer tier.

Business logic:

- Premium → VIP
- Regular → STANDARD

The UDF demonstrates how custom business logic can be implemented in Spark when built-in functions are not sufficient.

## 7. Cache / Persist

Completed orders are persisted using MEMORY_AND_DISK.

The persisted dataset is reused for multiple analytical operations.

This reduces repeated computation of the same dataset.

After processing, the persisted dataset is released using unpersist().

## 8. Window Functions

A window is partitioned by customer_id and ordered by order_date and order_id.

Two window calculations are performed:

- Running sales per customer
- Customer order rank

This demonstrates analytical processing using Spark window functions.

## 9. Customer Aggregation

Customer-level sales statistics are calculated using:

- Total sales
- Total orders
- Total quantity

The results are sorted by total sales in descending order.

## 10. Category Aggregation

Category-level statistics are calculated using:

- Category sales
- Order count
- Quantity sold

The results are sorted by category sales in descending order.

## 11. Partition Tuning

The completed orders are repartitioned into two partitions using customer_id.

The project checks:

- Number of partitions
- Record distribution across partitions

This demonstrates how partitioning can be tuned to improve parallel processing and data distribution.

## 12. Spark SQL

The completed orders DataFrame is registered as a temporary SQL view named completed_orders.

Spark SQL is then used to calculate:

- Category
- Total sales
- Total orders
- Total quantity

This demonstrates SQL-based analytical processing using Spark.

## 13. Broadcast Join

A broadcast join is performed between completed orders and the small product reference dataset.

The product dataset is explicitly broadcast.

This demonstrates how broadcast joins can reduce shuffle when joining a large dataset with a small reference dataset.

## 14. Physical Plan

The project uses explain("formatted") to inspect the Spark physical execution plan.

The physical plan demonstrates Spark operations including:

- Broadcast joins
- Hash aggregation
- Exchange
- Sort
- Adaptive Spark execution

Exchange operations demonstrate shuffle boundaries in the execution plan.

## 15. Spark Execution Concepts

### DAG

DAG stands for Directed Acyclic Graph.

Spark builds a DAG representing the sequence of transformations that need to be executed.

### Lineage

Lineage represents the history of transformations used to create a dataset.

Spark can use lineage to recompute lost partitions.

### Stage

A stage is a group of tasks that can execute without crossing a shuffle boundary.

Shuffle operations create boundaries between stages.

### Shuffle

Shuffle redistributes data between partitions.

Operations such as aggregation, sorting, repartitioning, and some joins can cause shuffle.

### Executor

An executor runs Spark tasks and can store cached or persisted data.

### YARN

YARN is a Hadoop cluster resource manager that allocates resources for applications.

### Optimization

The project demonstrates the following optimization techniques:

- Filter data early.
- Persist reused data.
- Tune partitions.
- Use broadcast joins for small datasets.
- Inspect the physical execution plan.

## 16. Structured Streaming

The project also contains a Structured Streaming component.

Streaming data is read from:

data/stream_events

The file source is configured with maxFilesPerTrigger = 1.

Only COMPLETED streaming orders are processed.

The streaming output contains:

- order_id
- customer_id
- product_id
- quantity
- event_date
- event_value

The streaming query uses Trigger.AvailableNow() to process the available streaming files.

The successful streaming output processed:

- S001
- S002
- S003
- S005

The cancelled event S004 was filtered out.

## 17. Batch and Streaming Architecture

Batch Layer:

Customers + Products + Orders
→ Data Cleaning
→ Data Enrichment
→ UDF
→ Joins
→ Cache/Persist
→ Aggregations
→ Window Functions
→ Spark SQL
→ Partition Tuning
→ Broadcast Join
→ Physical Plan

Streaming Layer:

Streaming Events
→ Structured Streaming
→ Filter COMPLETED Events
→ Streaming Output

## 18. Execution

Run the project from the Day 30 project directory using:

sbt run

To save the complete execution output:

sbt run 2>&1 | tee output/output.txt

## 19. Successful Execution

The final project execution completed successfully.

The output contains:

DAY 30 CAPSTONE COMPLETED SUCCESSFULLY

The Structured Streaming component also completed successfully and processed the available completed events.

The complete execution proof is saved in:

output/output.txt

## 20. Interview Questions and Answers

### 1. What is Apache Spark?

Apache Spark is a distributed data processing engine used for large-scale batch and streaming data processing.

### 2. What is a DAG in Spark?

DAG stands for Directed Acyclic Graph. Spark uses it to represent the sequence of transformations and operations that need to be executed.

### 3. What is lineage?

Lineage is the history of transformations used to create a dataset. Spark uses lineage for fault recovery.

### 4. What is a shuffle?

Shuffle is the redistribution of data between partitions. It can occur during operations such as groupBy, aggregation, sorting, repartitioning, and some joins.

### 5. What is an accumulator?

An accumulator is a Spark shared variable mainly used for aggregating values such as counters.

### 6. What is a broadcast variable?

A broadcast variable allows a small read-only dataset to be efficiently shared with executors.

### 7. Why are broadcast joins used?

Broadcast joins can avoid a large shuffle when one side of the join is small enough to broadcast.

### 8. What is persistence in Spark?

Persistence stores a computed dataset so that it can be reused without recomputing the complete lineage.

### 9. What is repartition?

Repartition changes the number of partitions and redistributes data between them.

### 10. What is a window function?

A window function performs calculations across related rows while preserving individual rows.

### 11. Why was a UDF used in this project?

A UDF was used to implement custom customer-tier logic that converts Premium customers to VIP and Regular customers to STANDARD.

### 12. What is Spark SQL?

Spark SQL allows structured data to be processed using SQL queries and DataFrame APIs.

### 13. What is Structured Streaming?

Structured Streaming is Spark's streaming processing engine for processing streaming data using DataFrame and Dataset APIs.

### 14. What is an executor?

An executor is a process that runs Spark tasks and can store cached or persisted data.

### 15. What is a stage?

A stage is a group of tasks that can execute together without crossing a shuffle boundary.

### 16. What causes a shuffle?

Operations such as groupBy, aggregation, sorting, repartitioning, and certain joins can cause shuffle.

### 17. Why is partition tuning important?

Proper partitioning can improve parallelism, distribute workload, and reduce unnecessary data movement.

### 18. Why is cache or persist used?

Cache or persist is used when a dataset is reused multiple times and recomputing it would be expensive.

### 19. What is YARN?

YARN is a Hadoop resource management and cluster scheduling system.

### 20. How can this project be optimized for production?

The project can be optimized using appropriate partition sizing, broadcast joins for small datasets, persistence for reused data, early filtering, shuffle monitoring, and execution-plan analysis.

## Conclusion

This Day 30 final capstone combines the major Spark concepts practiced throughout the 30-day training.

The project demonstrates:

- Batch processing
- Structured Streaming
- Data cleaning
- Data enrichment
- Joins
- Aggregations
- Window functions
- UDF
- Broadcast variables
- Broadcast joins
- Accumulators
- Cache and persistence
- Partition tuning
- Spark SQL
- Physical plan analysis
- DAG and lineage
- Stages and shuffle
- Executors
- YARN concepts
- Spark optimization

The final application successfully completed the batch processing workflow and the Structured Streaming workflow.