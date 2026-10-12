# Day 29 — End-to-End E-Commerce Project

## Objective

Build an end-to-end e-commerce data processing pipeline using Apache Spark.

The project covers:

- Raw → Clean → Enrich → Aggregate pipeline
- RDD / Pair RDD processing
- DataFrame processing
- UDF decision
- Joins
- Window functions
- Partitioning
- Persistence
- Spark SQL
- Shuffle and stage explanation
- Optimization choices
- Production-style documentation

## Technologies Used

- Scala 2.12.18
- Apache Spark 3.5.6
- Spark Core
- Spark SQL
- sbt 1.10.11
- Java 17
- WSL2 Ubuntu

## Project Structure

Day-29-End-to-End-E-Commerce-Project/
├── src/
│   └── main/
│       └── scala/
│           └── Main.scala
├── data/
│   ├── customers.csv
│   ├── products.csv
│   └── orders.csv
├── output/
│   └── output.txt
├── project/
│   └── build.properties
├── build.sbt
├── .gitignore
└── README.md

## Input Data

The project uses three CSV files:

### customers.csv

Contains:
- customer_id
- name
- city
- segment

### products.csv

Contains:
- product_id
- product_name
- category
- price

### orders.csv

Contains:
- order_id
- customer_id
- product_id
- quantity
- order_date
- status

## Pipeline

Raw Data
→ Data Cleaning
→ Data Enrichment
→ Completed Orders
→ Window Functions
→ Aggregations
→ Partitioning
→ Spark SQL Reporting

## Data Cleaning

The input CSV files are loaded into Spark DataFrames.

Cleaning operations include:

- Trimming string columns
- Converting values to the required data types
- Converting status and segment values to uppercase
- Removing invalid or missing records
- Converting order_date to DateType
- Validating positive quantity and price values

Input counts:

- Customers: 5
- Products: 5
- Orders: 10

## Pair RDD Processing

A Pair RDD is used for category-wise product price aggregation.

The product DataFrame is converted into a Pair RDD containing:

- Category as the key
- Price as the value

`reduceByKey` is then used to calculate the total price by category.

Result:

- Accessories → 5000.0
- Electronics → 100000.0

This demonstrates RDD transformation and key-based aggregation.

## DataFrame Enrichment

Orders are joined with customers using `customer_id`.

The resulting DataFrame is then joined with products using `product_id`.

The enriched dataset contains:

- Order details
- Customer details
- Product details
- Category
- Price
- Quantity
- Order date
- Status

An `order_value` column is calculated using:

`quantity × price`

## Completed Orders

Only completed orders are selected for business analytics.

Total completed orders:

- 9

The completed orders DataFrame is persisted using `MEMORY_ONLY` because it is reused by multiple downstream operations.

## Window Functions

A window is created using:

- `customer_id` as the partition
- `order_date` and `order_id` as the ordering columns

The project calculates:

- Running customer sales
- Customer order number using `row_number`

This demonstrates customer-level analytical processing using Spark window functions.

## Customer Aggregation

Customer-level analytics are calculated using `groupBy`.

The project calculates:

- Total orders
- Total quantity
- Total sales

Results:

- C001 → 81000.0
- C003 → 60000.0
- C002 → 59000.0
- C004 → 34000.0
- C005 → 25000.0

## Category Aggregation

Completed orders are aggregated by product category.

The project calculates:

- Total orders
- Total quantity
- Total sales

Results:

- Electronics → 6 orders, 8 quantity, 240000.0 sales
- Accessories → 3 orders, 7 quantity, 19000.0 sales

## Joins

Two main joins are performed:

1. Orders + Customers using `customer_id`
2. Enriched Orders + Products using `product_id`

The physical execution plan shows Spark using `BroadcastHashJoin` for the small customer and product datasets.

## Partitioning

The completed orders DataFrame is repartitioned using `customer_id`:

`repartition(2, customer_id)`

Result:

- Total partitions: 2
- Partition 0: 5 records
- Partition 1: 4 records

Partitioning helps organize data for customer-based processing.

## Persistence

`completedOrders` is persisted using:

`StorageLevel.MEMORY_ONLY`

It is reused for:

- Window operations
- Customer aggregation
- Category aggregation
- Partitioning
- Spark SQL reporting

Persistence helps avoid unnecessary recomputation of reused data.

## Spark SQL

The completed orders DataFrame is registered as a temporary view:

`completed_orders`

Spark SQL is then used to generate category-level sales reporting.

Result:

- Electronics → 6 orders, 8 quantity, 240000.0 sales
- Accessories → 3 orders, 7 quantity, 19000.0 sales

## UDF Decision

No custom UDF is used.

Spark built-in functions are sufficient for the required transformations, including:

- trim
- upper
- cast
- to_date
- filter
- sum
- count
- round
- row_number

Using built-in functions allows Spark to optimize the operations more effectively than a custom UDF.

## Shuffle and Stages

The project demonstrates several operations that can involve shuffle:

- `reduceByKey`
- `groupBy`
- Window operations
- `repartition`
- Sorting and ordering

Shuffle moves data between partitions and creates stage boundaries in Spark execution.

The physical execution plan is inspected using:

`explain("formatted")`

The plan shows operations such as:

- BroadcastHashJoin
- HashAggregate
- Exchange
- Sort
- AdaptiveSparkPlan

The actual execution plan uses broadcast joins for the small customer and product datasets.

## Optimization Choices

The following optimization decisions are used:

- Clean data before downstream processing
- Select only required columns
- Filter completed orders before analytics
- Persist reused DataFrames
- Use broadcast joins for small reference datasets
- Use built-in Spark functions instead of UDFs
- Repartition data using customer_id where required
- Inspect the physical execution plan

## Architecture

Raw CSV Files
│
├── customers.csv
├── products.csv
└── orders.csv
        │
        ▼
Data Ingestion
        │
        ▼
Data Cleaning
        │
        ▼
Data Enrichment
   ┌────┴────┐
   ▼         ▼
Pair RDD   DataFrame
   │         │
   │      Joins
   │         │
   │      Windows
   │         │
   │    Aggregations
   │         │
   └────┬────┘
        ▼
Partitioning
        │
        ▼
Spark SQL Reporting
        │
        ▼
Final Analytics

## Output Verification

The project completed successfully.

Execution output is saved in:

`output/output.txt`

Final verification includes:

- Raw counts
- Clean counts
- Pair RDD aggregation
- Enriched orders
- Completed order count
- Window function results
- Customer sales
- Category sales
- Partition distribution
- Spark SQL report
- Physical execution plan
- Shuffle and optimization explanation

## How to Run

From the project directory:

`sbt run`

To save the execution output:

`sbt run 2>&1 | tee output/output.txt`

## Conclusion

Day 29 demonstrates an end-to-end Spark e-commerce analytics pipeline using both RDD and DataFrame APIs.

The project covers data ingestion, cleaning, enrichment, joins, Pair RDD processing, window functions, aggregations, partitioning, persistence, Spark SQL, shuffle analysis, execution plans, and optimization techniques.

The complete project was executed successfully and the execution proof is stored in `output/output.txt`.
