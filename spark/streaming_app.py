"""
Advanced Spark Structured Streaming Application

This application demonstrates:
- Reading from Kafka
- Handling dirty/malformed data (NO Python UDFs - pure Spark SQL only)
- Event-time processing with watermarks
- Stateful windowed aggregations
- Multiple output sinks
- Robust error handling

The application will NOT crash on bad data.
"""

import sys
import os

# ── Must be set BEFORE importing pyspark ──────────────────────────────────────
os.environ["PYSPARK_PYTHON"]        = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

if sys.platform == "win32":
    os.environ["HADOOP_HOME"] = "C:\\hadoop"
    os.environ["PATH"]        = "C:\\hadoop\\bin;" + os.environ.get("PATH", "")
    # Set JAVA_HOME so PySpark finds the JDK reliably
    if not os.environ.get("JAVA_HOME"):
        os.environ["JAVA_HOME"] = "C:\\Program Files\\Java\\jdk-20"
    # Pre-create every directory Spark needs — prevents winutils path errors
    for d in ["C:/tmp", "C:/tmp/hive", "C:/tmp/spark-warehouse", "C:/tmp/spark-local"]:
        os.makedirs(d, exist_ok=True)
    # Grant Hive scratch dir permissions
    import subprocess
    subprocess.run(
        ["C:\\hadoop\\bin\\winutils.exe", "chmod", "777", "C:\\tmp\\hive"],
        capture_output=True
    )

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, window, avg, count,
    when, lit, to_json, struct, regexp_replace, trim,
    isnull, length
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

# ── Schema ──────────────────────────────────────────────────────────────────
EVENT_SCHEMA = StructType([
    StructField("device_id",   StringType(), True),
    StructField("event_time",  StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("country",     StringType(), True),
])

# ── Kafka config ─────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_INPUT_TOPIC        = "raw-events"
KAFKA_INVALID_TOPIC      = "invalid-events"
CHECKPOINT_DIR           = "./checkpoints"


# ── Spark session ─────────────────────────────────────────────────────────────
def create_spark_session():
    print("[DEBUG] Building Spark session...")
    py_exec = sys.executable
    spark = (
        SparkSession.builder
        .appName("Advanced-Spark-Structured-Streaming")
        .master("local[*]")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .config("spark.sql.warehouse.dir",  "C:/tmp/spark-warehouse")
        .config("spark.driver.host",         "127.0.0.1")
        .config("spark.pyspark.python",      py_exec)
        .config("spark.pyspark.driver.python", py_exec)
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    print(f"✓ Spark {spark.version} ready")
    return spark


# ── Read from Kafka ───────────────────────────────────────────────────────────
def read_from_kafka(spark):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_INPUT_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


# ── Parse JSON safely ─────────────────────────────────────────────────────────
def parse_json(raw_df):
    """
    from_json returns NULL for malformed JSON — no crash, no Python UDF.
    """
    return (
        raw_df
        .selectExpr("CAST(value AS STRING) AS raw_message")
        .withColumn("p", from_json(col("raw_message"), EVENT_SCHEMA))
        .select(
            col("raw_message"),
            col("p.device_id")   .alias("device_id"),
            col("p.event_time")  .alias("event_time"),
            col("p.temperature") .alias("temperature"),
            col("p.country")     .alias("country"),
        )
    )


# ── Validate with pure Spark SQL expressions (NO Python UDF) ─────────────────
def add_validation(df):
    """
    All validation is expressed as Spark Column expressions.
    No Python UDF → no Python worker subprocess → no crash.
    """

    # A country is invalid if it contains digits or special chars like @
    country_has_bad_chars = col("country").rlike("[0-9@#\\$%\\^&\\*\\(\\)]")

    is_valid = (
        col("device_id").isNotNull()   & (trim(col("device_id"))  != "") &
        col("event_time").isNotNull()  & (trim(col("event_time")) != "") &
        col("temperature").isNotNull() &
        (col("temperature") != lit(-999.0)) &
        (col("temperature") >= lit(-50.0)) &
        (col("temperature") <= lit(60.0))  &
        col("country").isNotNull()     & (trim(col("country")) != "") &
        ~country_has_bad_chars
    )

    # Reason column (also pure SQL — uses nested when/otherwise)
    reason = (
        when(col("device_id").isNull() | (trim(col("device_id")) == ""),
             lit("missing_device_id"))
        .when(col("event_time").isNull() | (trim(col("event_time")) == ""),
              lit("missing_event_time"))
        .when(col("temperature").isNull(),
              lit("missing_temperature"))
        .when(col("temperature") == lit(-999.0),
              lit("invalid_sentinel_temperature"))
        .when((col("temperature") < lit(-50.0)) | (col("temperature") > lit(60.0)),
              lit("temperature_out_of_range"))
        .when(col("country").isNull() | (trim(col("country")) == ""),
              lit("missing_country"))
        .when(country_has_bad_chars,
              lit("invalid_country_format"))
        # all fields null → JSON was malformed
        .when(
            col("device_id").isNull() & col("event_time").isNull() &
            col("temperature").isNull() & col("country").isNull(),
            lit("malformed_json")
        )
        .otherwise(lit("valid"))
    )

    return (
        df
        .withColumn("is_valid",          is_valid)
        .withColumn("validation_reason", reason)
    )


# ── Process valid events ──────────────────────────────────────────────────────
def process_valid(df):
    valid = df.filter(col("is_valid"))
    return (
        valid
        .withColumn(
            "event_timestamp",
            to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
        )
        .filter(col("event_timestamp").isNotNull())
        .withWatermark("event_timestamp", "10 minutes")
    )


# ── Aggregations ──────────────────────────────────────────────────────────────
def device_temp_agg(df):
    return (
        df.groupBy(window("event_timestamp", "5 minutes"), "device_id")
        .agg(avg("temperature").alias("avg_temperature"),
             count("*").alias("event_count"))
        .select(
            col("window.start").alias("window_start"),
            col("window.end")  .alias("window_end"),
            "device_id", "avg_temperature", "event_count"
        )
    )


def country_agg(df):
    return (
        df.groupBy(window("event_timestamp", "5 minutes"), "country")
        .agg(count("*").alias("event_count"),
             avg("temperature").alias("avg_temperature"))
        .select(
            col("window.start").alias("window_start"),
            col("window.end")  .alias("window_end"),
            "country", "event_count", "avg_temperature"
        )
    )


# ── Console sink helper ───────────────────────────────────────────────────────
def to_console(df, name, mode="append"):
    return (
        df.writeStream
        .outputMode(mode)
        .format("console")
        .queryName(name)
        .option("truncate", "false")
        .option("numRows", 50)
        .trigger(processingTime="15 seconds")
        .start()
    )


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 70)
    print("Advanced Spark Structured Streaming Application")
    print("=" * 70)

    spark = create_spark_session()

    print("\n[1] Reading from Kafka...")
    raw = read_from_kafka(spark)

    print("[2] Parsing JSON...")
    parsed = parse_json(raw)

    print("[3] Validating (pure Spark SQL, no Python UDFs)...")
    validated = add_validation(parsed)

    invalid_df = validated.filter(~col("is_valid"))
    valid_df   = process_valid(validated)

    print("[4] Computing aggregations...")
    dev_agg  = device_temp_agg(valid_df)
    coun_agg = country_agg(valid_df)

    print("[5] Starting output queries...")
    queries = []

    # Valid events
    queries.append(to_console(
        valid_df.select("device_id", "event_timestamp", "temperature", "country"),
        "valid_events", "append"
    ))

    # Invalid events
    queries.append(to_console(
        invalid_df.select("raw_message", "validation_reason"),
        "invalid_events", "append"
    ))

    # Device temperature aggregation
    queries.append(to_console(dev_agg,  "device_temp_agg",  "update"))

    # Country aggregation
    queries.append(to_console(coun_agg, "country_agg", "update"))

    print(f"\n✓ {len(queries)} streaming queries started.")
    print("Application is running. Press Ctrl+C to stop.\n")

    try:
        for q in queries:
            q.awaitTermination()
    except KeyboardInterrupt:
        print("\n⚠ Interrupted — stopping queries...")
        for q in queries:
            q.stop()
    finally:
        spark.stop()
        print("✓ Spark session stopped.")


if __name__ == "__main__":
    main()
