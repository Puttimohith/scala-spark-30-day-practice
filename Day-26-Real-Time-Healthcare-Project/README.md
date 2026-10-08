# Day 26 — Real-Time Healthcare Project

## Objective

Build a real-time healthcare monitoring application using Apache Spark Streaming.

The project processes patient vital signs, detects abnormal readings, generates alerts, maintains patient-wise abnormal reading counts, and demonstrates broadcast variables, accumulators, stateful processing, window processing, partitions, DAG, and fault tolerance.

## Technologies Used

- Scala 2.12.18
- Apache Spark 3.5.6
- Spark Streaming
- SBT
- WSL Ubuntu
- Java 17

## Project Structure

Day-26-Real-Time-Healthcare-Project/
├── build.sbt
├── README.md
├── output.txt
├── data/
│   └── patient_vitals.txt
├── src/
│   └── main/
│       └── scala/
│           └── Main.scala
└── project/
    └── build.properties

## 1. Patient Vital Event Schema

Each patient vital event contains:

- Patient ID
- Temperature
- Heart Rate
- Systolic Blood Pressure
- Diastolic Blood Pressure
- SpO2

The Scala case class used in the project is:

PatientVital(patientId, temperature, heartRate, systolicBP, diastolicBP, spo2)

Example:

P001,98.6,78,120,80,98

## 2. Broadcast Thresholds

Broadcast variables are used to distribute common abnormal-vital thresholds efficiently to Spark worker nodes.

The thresholds used are:

| Vital | Threshold |
|---|---:|
| Maximum Temperature | 100.4 F |
| Maximum Heart Rate | 100 bpm |
| Maximum Systolic BP | 140 |
| Maximum Diastolic BP | 90 |
| Minimum SpO2 | 92% |

A vital is considered abnormal when it crosses any of these thresholds.

## 3. Abnormal Vital Detection

The streaming data is converted into PatientVital objects.

The application checks:

- Temperature > 100.4 F
- Heart Rate > 100 bpm
- Systolic BP > 140
- Diastolic BP > 90
- SpO2 < 92%

If any condition is true, the vital reading is treated as abnormal.

## 4. Abnormal Vital Alerts

For every streaming batch, abnormal patient readings are displayed as alerts.

Example:

P001 -> Temp=103.1 F, HR=130 bpm, BP=155/102, SpO2=87%
P003 -> Temp=103.0 F, HR=128 bpm, BP=152/99, SpO2=88%

This helps identify patients whose vital signs require attention.

## 5. Accumulator

An accumulator is used to count the total number of abnormal vital events detected during the streaming application.

The accumulator is updated once for each batch based on the number of abnormal records in that batch.

Final result:

Total abnormal vital events detected: 5

The accumulator demonstrates how Spark can maintain a shared counter across distributed processing.

## 6. Stateful Processing

Stateful processing maintains information across multiple streaming batches.

The project maintains the number of abnormal readings for each patient.

Example:

P001 -> 2 abnormal readings
P003 -> 3 abnormal readings

When a patient reaches two or more abnormal readings, the application generates a repeated abnormal alert.

Example:

REPEATED ABNORMAL ALERT: P001 has repeated abnormal readings.
REPEATED ABNORMAL ALERT: P003 has repeated abnormal readings.

The project uses updateStateByKey for maintaining the running state.

Checkpointing is enabled because stateful Spark Streaming requires checkpoint information for recovery.

## 7. Window Processing

Window processing analyzes streaming data over a fixed period of time.

This project uses:

- Window duration: 10 seconds
- Sliding interval: 5 seconds

The project uses reduceByKeyAndWindow to calculate abnormal readings within the window.

Example:

P001 -> 2 abnormal readings in 10-second window
P003 -> 2 abnormal readings in 10-second window

Window processing helps identify recent patterns in patient abnormalities.

## 8. Partitions

The input RDDs are created with two partitions.

The application displays:

Current RDD partitions: 2

Partitions divide data into smaller parts so Spark can process it in parallel.

## 9. DAG

DAG stands for Directed Acyclic Graph.

Spark creates a DAG from the transformations used in the application.

The DAG represents the sequence of operations Spark needs to perform before executing the computation.

Example processing flow:

Input Stream
    ↓
Parse Data
    ↓
Filter Abnormal Vitals
    ↓
Stateful Processing
    ↓
Window Processing
    ↓
Alerts

## 10. Fault Tolerance

Spark provides fault tolerance through RDD lineage.

If a partition is lost, Spark can recompute the required data using the transformations that created the RDD.

Checkpointing is also used in this project for stateful streaming processing.

## 11. Streaming Architecture

The basic processing flow is:

Patient Vital Events
        ↓
Spark Streaming
        ↓
Parse Vital Data
        ↓
Broadcast Thresholds
        ↓
Abnormal Vital Detection
        ↓
 ┌───────────────┬─────────────────┐
 ↓               ↓                 ↓
Alerts      Stateful Counts    Window Counts
 ↓               ↓                 ↓
Current      Repeated          Recent
Alerts       Alerts             Patterns
        \       |       /
         \      |      /
          \     |     /
      Healthcare Monitoring

## 12. Sample Input

Sample patient vital events include:

P001,98.6,78,120,80,98
P002,99.1,82,125,82,97
P003,101.8,118,145,95,91
P004,98.4,72,118,76,99
P001,102.2,125,150,100,89
P002,98.9,80,122,80,98
P003,102.5,122,148,96,90
P004,98.7,75,119,78,98
P001,103.1,130,155,102,87
P002,99.0,81,124,81,97
P003,103.0,128,152,99,88
P004,98.5,73,117,77,99

## 13. Actual Output

The project successfully detected abnormal patient readings.

### Batch 1

P003 -> Temp=101.8 F, HR=118 bpm, BP=145/95, SpO2=91%

State:

P003 -> 1 abnormal readings

Window:

P003 -> 1 abnormal readings in 10-second window

### Batch 2

P001 -> Temp=102.2 F, HR=125 bpm, BP=150/100, SpO2=89%
P003 -> Temp=102.5 F, HR=122 bpm, BP=148/96, SpO2=90%

State:

P001 -> 1 abnormal readings
P003 -> 2 abnormal readings

Repeated alert:

REPEATED ABNORMAL ALERT: P003 has repeated abnormal readings.

### Batch 3

P001 -> Temp=103.1 F, HR=130 bpm, BP=155/102, SpO2=87%
P003 -> Temp=103.0 F, HR=128 bpm, BP=152/99, SpO2=88%

Final state:

P001 -> 2 abnormal readings
P003 -> 3 abnormal readings

Repeated alerts:

REPEATED ABNORMAL ALERT: P001 has repeated abnormal readings.
REPEATED ABNORMAL ALERT: P003 has repeated abnormal readings.

## 14. Final Result

========== FINAL ACCUMULATOR RESULT ==========

Total abnormal vital events detected: 5

========== DAY 26 COMPLETED ==========

The application successfully completed the real-time healthcare monitoring task.

## 15. How to Run

Navigate to the project directory:

cd ~/scala-spark-30-day-practice/Day-26-Real-Time-Healthcare-Project

Compile the project:

sbt compile

Run the project:

sbt run

To save the important execution output:

sbt run 2>&1 | grep -E '^==========|^Maximum|^Minimum|^P00[1-4] ->|^REPEATED|^Current RDD partitions:|^Total abnormal vital events detected:|^Broadcast:|^Accumulator:|^Stateful Processing:|^Window Processing:|^Partitions:|^DAG:|^Fault Tolerance:|^Broadcast sends|^Accumulator is|^Stateful processing|^Window processing|^Partitions divide|^Spark creates|^Spark can recover|^StreamingContext started' > output.txt

View the output:

cat output.txt

## 16. Key Concepts

### Broadcast Variable

A broadcast variable efficiently distributes read-only common data to worker nodes.

In this project, abnormal vital thresholds are broadcast.

### Accumulator

An accumulator is used for aggregation such as counters.

In this project, it counts abnormal vital events.

### Stateful Processing

Stateful processing maintains information from previous batches.

In this project, abnormal readings are maintained for each patient.

### Window Processing

Window processing analyzes recent streaming data over a defined time period.

This project uses a 10-second window with a 5-second sliding interval.

### Partition

A partition is a logical division of an RDD.

Partitions allow Spark to process data in parallel.

### DAG

DAG means Directed Acyclic Graph.

Spark uses a DAG to represent and optimize the sequence of transformations.

### Fault Tolerance

Spark can recover lost RDD partitions using lineage and recomputation.

Checkpointing is also used for stateful streaming recovery.

## Conclusion

This project demonstrates a real-time healthcare monitoring system using Scala and Apache Spark Streaming.

It processes patient vital events, detects abnormal readings, generates alerts, tracks repeated abnormalities using stateful processing, analyzes recent abnormalities using window operations, and demonstrates important Spark concepts such as broadcast variables, accumulators, partitions, DAG execution, and fault tolerance.