from pyspark.sql import SparkSession
from producer import EVENTS_TOPIC
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql.functions import from_json, to_json, struct, sum, col, window
from prometheus_client import Counter, start_http_server
import logging

ANALYSIS_TOPIC = "traffic_analysis"

start_http_server(8001)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder.appName("TrafficAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("sensor_id", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("vehicle_count", IntegerType(), False),
    StructField("average_speed", DoubleType(), False),
    StructField("congestion_level", StringType(), True),
])

kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", EVENTS_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

parsed_stream = kafka_stream \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")

vehicle_count = Counter("vehicle_count_total", "Total number of vehicles", ["sensor_id"])

def validate_schema(batch_df, batch_id):
    """
    Validates schema by enforcing correct types and logging invalid rows.
    """
    logger.info(f"Validating Schema for Batch ID: {batch_id}")

    invalid_rows = batch_df.filter(
        (col("sensor_id").isNull()) | 
        (col("timestamp").isNull()) | 
        (col("vehicle_count").isNull() | (col("vehicle_count").cast("int").isNull())) | 
        (col("average_speed").isNull() | (col("average_speed").cast("double").isNull()))
    )

    if invalid_rows.count() > 0:
        logger.warning(f"Batch {batch_id} contains {invalid_rows.count()} invalid rows")
        invalid_rows.show(truncate=False)

    # Keep only valid rows
    return batch_df.na.drop(subset=["sensor_id", "timestamp", "vehicle_count", "average_speed"])

def clean_and_aggregate(batch_df, batch_id):
    """
    Cleans, validates, and aggregates batch data.
    """
    logger.info(f"Cleaning & Aggregating Batch ID: {batch_id}")

    batch_df = validate_schema(batch_df, batch_id) \
        .dropDuplicates(["sensor_id", "timestamp"]) \
        .filter(
            (col("vehicle_count") >= 0) & 
            (col("average_speed") > 0)
        )

    aggregated_df = batch_df \
        .groupBy(window(col("timestamp"), "5 minutes"), col("sensor_id")) \
        .agg(sum("vehicle_count").alias("total_count"))

    return aggregated_df

def process_event(batch_df, batch_id):
    """
    Processes and logs events from the data stream.
    """
    logger.info(f"Processing Batch ID: {batch_id} - Streaming Data")
    volume_stream = clean_and_aggregate(batch_df, batch_id)
    volume_stream.show(truncate=False)

def write_to_kafka(batch_df, batch_id):
    """
    Writes processed batch data to Kafka and updates Prometheus metrics.
    """
    logger.info(f"Processing Batch ID: {batch_id} - Writing to Kafka")
    volume_stream = clean_and_aggregate(batch_df, batch_id)

    volume_stream \
        .select(to_json(struct("*")).alias("value")) \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", ANALYSIS_TOPIC) \
        .save()

    for row in volume_stream.collect():
        vehicle_count.labels(sensor_id=row.sensor_id)._value.set(row.total_count)
        logger.info(f"Sent event: {row.sensor_id} with count {row.total_count}")

volume_query = parsed_stream.writeStream.outputMode("update").foreachBatch(process_event).start()
kafka_query = parsed_stream.writeStream.outputMode("update").foreachBatch(write_to_kafka).start()

volume_query.awaitTermination()
kafka_query.awaitTermination()
