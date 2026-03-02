# Lab — Advanced Spark Structured Streaming  
## Time, State, and Data Quality

---

## Context
In Lab 1 for Spark Streaming, you learned how to connect Kafka to Spark Structured Streaming and apply basic transformations.

In this lab, you will work with **structured but dirty streaming data** and explore the **core challenges of real-world stream processing**:
state, event time, windowing, watermarks, and data quality.

---

## Prerequisites
You must have:
- Docker installed and running
- Kafka running via Docker
- Apache Spark available (local or Docker)
- Python 3.9+

---

## Architecture
Kafka (JSON events) → Spark Structured Streaming → Validation / Aggregation → Output


Kafka handles **ingestion and durability**.  
Spark handles **structure, time, and state**.

---

## Data

The dataset may include invalid JSON, missing fields, wrong types, and inconsistent values.

⚠️ You must not modify the input data.

---

## Handle Dirty Data
Separate the stream into:
- **Valid events** (correct schema and values)
- **Invalid events** (malformed, missing, or incorrect data)

Requirements:
- The application must not crash
- Invalid events must be isolated (e.g., separate sink or topic)

---

## Event Time and Watermarks
- Use event time instead of processing time
- Define a watermark (e.g. 10 minutes)
- Observe how late data is handled

Key idea: state must be bounded in infinite streams.

---

## Windowed Aggregations
Perform stateful aggregations, such as:
- Average temperature per device per time window
- Event count per country per window

Use event-time windows.

---

## Write Streaming Results
Write results to one or more sinks:
- Console (for debugging)
- File sink (Parquet or JSON)
- Kafka topic

Experiment with output modes:
- `append`
- `update`
- `complete`

---

## Expected Outcome
At the end of this lab, you should have:
- A robust Spark Structured Streaming application
- Correct handling of dirty streaming data
- Stateful, windowed aggregations
- A clear understanding of time and state in streams

If your application crashes on bad data, the lab is **not complete**.

---

## How to Run This Lab

### Step 1: Install Python Dependencies

```bash
pip install -r requirements.txt
```

**Note:** Make sure you have Java 8 or 11 installed (required for Spark).

### Step 2: Start Kafka with Docker

Navigate to the `kafka/` directory and start the Docker containers:

```bash
cd kafka
docker-compose up -d
```

Wait for Kafka to be fully ready (about 10-20 seconds).

### Step 3: Create Kafka Topics

On **Windows with Git Bash** or **WSL**:

```bash
bash create_topics.sh
```

On **Windows PowerShell** (alternative):

```powershell
# Wait for Kafka to be ready
Start-Sleep -Seconds 10

# Create topics manually
docker exec kafka11 kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic raw-events --if-not-exists
docker exec kafka11 kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic invalid-events --if-not-exists
docker exec kafka11 kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic processed-events --if-not-exists

# List topics
docker exec kafka11 kafka-topics --list --bootstrap-server localhost:9092
```

### Step 4: Start the Spark Streaming Application

Open a **new terminal**, navigate to the `spark/` directory, and run:

```bash
cd spark
python streaming_app.py
```

The application will start and wait for incoming events from Kafka.

### Step 5: Run the Kafka Producer

Open **another terminal**, navigate to the `kafka/` directory, and run:

```bash
cd kafka
python producer.py
```

The producer will start sending events from `data/events_dirty.json` to Kafka.

### Step 6: Observe the Results

You should see output in the Spark application terminal showing:

1. **Valid events** - correctly parsed and validated events
2. **Invalid events** - with reasons like:
   - `malformed_json` - not valid JSON
   - `missing_device_id` - device_id is null or empty
   - `invalid_sentinel_temperature` - temperature is -999
   - `temperature_out_of_range` - temperature outside realistic range
   - `invalid_country_format` - country contains numbers or special characters
   - And more...

3. **Device temperature aggregations** - average temperature per device per 5-minute window
4. **Country aggregations** - event count and average temperature per country per 5-minute window

---

## Key Implementation Details

### 1. Handling Dirty Data

The application uses `from_json()` which returns `null` for malformed JSON instead of throwing exceptions. This prevents the application from crashing.

```python
df = df.withColumn("parsed", from_json(col("json_string"), EVENT_SCHEMA))
```

### 2. Data Validation

Events are validated using business rules:
- Device ID must not be null or empty
- Event time must be valid
- Temperature must be in realistic range (-50°C to 60°C)
- Temperature must not be the sentinel value (-999)
- Country must not contain invalid characters

### 3. Event Time and Watermarks

The application uses event time (from the data) rather than processing time:

```python
df = df.withWatermark("event_timestamp", "10 minutes")
```

This watermark allows events up to 10 minutes late, after which they are dropped. This is essential for managing state in infinite streaming applications.

### 4. Windowed Aggregations

The application performs stateful aggregations over 5-minute tumbling windows:

```python
df.groupBy(
    window(col("event_timestamp"), "5 minutes"),
    col("device_id")
).agg(
    avg("temperature").alias("avg_temperature"),
    count("*").alias("event_count")
)
```

### 5. Multiple Output Sinks

The application writes to multiple sinks:
- **Console** - for debugging (4 different queries)
- **Parquet files** - commented out but available
- **Kafka topics** - commented out but available

### 6. Output Modes

Different output modes are used:
- `append` - for raw events (only new records)
- `update` - for aggregations (only changed records)
- `complete` - available for aggregations (all records, requires more memory)

---

## Troubleshooting

### Kafka Not Starting

```bash
# Check if containers are running
docker ps

# View logs
docker logs kafka11
docker logs zookeeper11

# Restart if needed
docker-compose down
docker-compose up -d
```

### Spark Application Crashes

Make sure you have:
- Java 8 or 11 installed
- PySpark installed (`pip install pyspark`)
- Kafka running and accessible

### Producer Cannot Connect

Make sure:
- Kafka is running (`docker ps`)
- Topics are created (`bash create_topics.sh`)
- Bootstrap servers address is correct (localhost:9092)

---

## Cleanup

To stop everything:

```bash
# Stop Spark application: Ctrl+C in terminal
# Stop producer: Ctrl+C in terminal

# Stop and remove Kafka containers
cd kafka
docker-compose down

# Remove output directories (optional)
rm -rf spark/checkpoints
rm -rf spark/output
```

---

## What You Learned

✓ **Time in Streaming**: Difference between event time and processing time  
✓ **Watermarks**: How to handle late data and bound state  
✓ **State Management**: Windowed aggregations with stateful operations  
✓ **Data Quality**: Separating valid and invalid data without crashing  
✓ **Fault Tolerance**: Using checkpoints for recovery  
✓ **Multiple Sinks**: Writing to console, files, and Kafka  
✓ **Output Modes**: append, update, and complete modes

---

## Next Steps

Experiment with:
- Different window sizes (1 minute, 10 minutes, etc.)
- Different watermark delays
- Sliding windows instead of tumbling windows
- More complex aggregations (min, max, stddev)
- Writing results to databases or cloud storage
- Implementing custom stateful operations with `mapGroupsWithState`
