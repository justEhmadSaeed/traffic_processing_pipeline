from pyspark.sql import SparkSession
from producer import EVENTS_TOPIC
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import from_json, to_json, struct, sum, col, window
from prometheus_client import Counter, start_http_server
import logging

start_http_server(8001)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder.appName("TrafficAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("vehicle_count", IntegerType(), True),
    StructField("average_speed", DoubleType(), True),
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
    .select('data.*') \
    .withColumn("timestamp", col("timestamp").cast("timestamp"))

vehicle_count = Counter("vehicle_count_total", "Total number of vehicles", ["sensor_id"])

ANALYSIS_TOPIC = "traffic_analysis"

def clean_and_aggregate(batch_df, batch_id):
    logger.info(f"Cleaning & Aggregating Batch ID: {batch_id}")

    batch_df = batch_df.na.drop(subset=["sensor_id", "timestamp"]) \
        .dropDuplicates(["sensor_id", "timestamp"]) \
        .filter(
            col("vehicle_count").isNotNull() & col("average_speed").isNotNull() &
            (col("vehicle_count") >= 0) & (col("average_speed") > 0)
        )

    volume_stream = batch_df \
        .groupBy(window(col("timestamp"), "5 minutes"), col("sensor_id")) \
        .agg(sum("vehicle_count").alias("total_count"))

    return volume_stream

def process_event(batch_df, batch_id):
    logger.info(f"Processing Batch ID: {batch_id} - Streaming Data")
    volume_stream = clean_and_aggregate(batch_df, batch_id)
    volume_stream.show(truncate=False)

def write_to_kafka(batch_df, batch_id):
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
