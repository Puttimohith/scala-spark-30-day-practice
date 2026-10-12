# Day 28 — Real-Time Booking Project

## Objective

Build a real-time booking processing application using Apache Spark Streaming.

The project processes hotel booking and cancellation events and demonstrates state management, occupancy calculation, Pair RDDs, window operations, broadcast variables, and Spark SQL reporting.

## Requirements Covered

1. Process hotel booking events.
2. Maintain booking and cancellation state.
3. Calculate hotel occupancy and room availability.
4. Use Pair RDD operations.
5. Use window operations to detect booking activity.
6. Use broadcast reference data.
7. Generate reporting output using Spark SQL.
8. Demonstrate partitioning and micro-batch processing.

## Technologies

- Scala 2.12.18
- Apache Spark 3.5.6
- Spark Streaming
- Spark SQL
- SBT 1.10.11
- Java 17

## Project Structure

Day-28-Real-Time-Booking-Project/
├── src/
│   └── main/
│       └── scala/
│           └── Main.scala
├── data/
│   ├── booking_events.txt
│   └── hotel_reference.txt
├── output/
├── project/
│   └── build.properties
├── build.sbt
├── .gitignore
├── output.txt
└── README.md

## Input Data

### Booking Events

Each booking event contains:

- Booking ID
- Customer ID
- Hotel ID
- Room ID
- Event Type
- Number of Rooms
- Amount
- Time

Supported event types:

- BOOK
- CANCEL

Example event:

B001,C001,H001,ROOM101,BOOK,2,2000,10:00

### Hotel Reference Data

The hotel reference data contains:

- Hotel ID
- Hotel Name
- Total Rooms

Example:

H001,Grand Plaza,5

## Main Components

### 1. Booking Event Schema

The BookingEvent case class represents incoming booking and cancellation events.

### 2. Broadcast Variable

Small hotel reference data is broadcast to Spark executors so the same reference information does not need to be repeatedly transferred.

### 3. Pair RDD

Booking events are converted into key-value pairs using the hotel ID as the key.

Pair RDD operations are used for booking and cancellation changes, revenue aggregation, and window calculations.

### 4. Booking and Cancellation State

The application calculates the room change for every event.

- BOOK increases occupied rooms.
- CANCEL decreases occupied rooms.

A running occupancy state is maintained for each hotel.

### 5. Occupancy and Availability

For every hotel:

Current Occupancy = Previous Occupancy + Booking/Cancellation Change

Availability is calculated from the hotel's total room capacity.

Occupancy is limited between 0% and 100%.

### 6. Window Operations

A 10-second window with a 5-second sliding interval is used to identify booking activity.

If a hotel has 4 or more events in the window, a high booking activity message is generated.

### 7. Cache / Persist

The current booking RDD is persisted using MEMORY_ONLY storage level.

This avoids recomputing the same RDD for multiple operations.

### 8. Partitioning

The booking RDD uses 2 partitions.

Partitions allow Spark to process data in parallel.

### 9. Spark SQL Reporting

The booking event data is loaded into a Spark DataFrame.

A temporary SQL view is created and queried to generate a hotel-level report containing:

- Number of events
- Booked rooms
- Cancelled rooms
- Booking revenue

## Sample Output

The application successfully calculated hotel occupancy and availability across streaming batches.

H001 (Grand Plaza) -> Occupied=5, Available=0, Occupancy=100.00%

H002 (City Heights) -> Occupied=3, Available=1, Occupancy=75.00%

H001 (Grand Plaza) -> Occupied=3, Available=2, Occupancy=60.00%

H002 (City Heights) -> Occupied=4, Available=0, Occupancy=100.00%

H003 (Lake View) -> Occupied=2, Available=1, Occupancy=66.67%

H001 (Grand Plaza) -> Occupied=5, Available=0, Occupancy=100.00%

H002 (City Heights) -> Occupied=1, Available=3, Occupancy=25.00%

H003 (Lake View) -> Occupied=3, Available=0, Occupancy=100.00%

High booking activity was detected for H001:

HIGH BOOKING ACTIVITY: H001 has 4 events in the window.

## Spark SQL Report

H001 -> events=6, bookedRooms=8, cancelledRooms=2, revenue=9100

H002 -> events=4, bookedRooms=6, cancelledRooms=3, revenue=9200

H003 -> events=2, bookedRooms=3, cancelledRooms=0, revenue=4600

## Concepts Demonstrated

### Pair RDD

Used to represent booking data as key-value pairs for aggregation.

### Window Operations

Used to analyze booking activity over a moving 10-second window with a 5-second sliding interval.

### Broadcast

Used for distributing small hotel reference data efficiently to Spark executors.

### State

Used to maintain running hotel occupancy across streaming batches.

### Occupancy

Used to calculate occupied rooms, available rooms, and occupancy percentage for each hotel.

### Spark SQL

Used for structured reporting and aggregation of booking events.

### Partitioning

Used to process booking data in parallel using multiple partitions.

### Spark Streaming

Used to process booking events as micro-batches.

## Execution

Compile the project using:

sbt compile

Run the project using:

sbt run

The verified execution completed successfully with:

========== DAY 28 COMPLETED ==========

## Output Proof

The verified execution output is stored in output.txt.

The output contains proof of:

- Booking event processing
- Broadcast reference data
- Booking and cancellation state
- Occupancy and availability
- Revenue aggregation
- Pair RDD processing
- Window operations
- Spark SQL reporting
- Partition information
- Streaming execution
- Completion status

## Conclusion

The Day 28 Real-Time Booking Project successfully demonstrates how Apache Spark Streaming can process booking and cancellation events, maintain hotel occupancy state, calculate room availability, detect booking activity using window operations, use broadcast reference data, and generate reports using Spark SQL.