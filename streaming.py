from pyspark.sql import SparkSession
from producer import EVENTS_TOPIC
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import from_json, to_json, struct, sum, col, window, count, when, desc, current_timestamp, row_number, first, lag
from pyspark.sql.window import Window
from prometheus_client import Counter, start_http_server


start_http_server(8000)

spark = SparkSession.builder.appName("TrafficAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", EVENTS_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

schema = StructType(
    [
        StructField("sensor_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("vehicle_count", IntegerType(), True),
        StructField("average_speed", DoubleType(), True),
        StructField("congestion_level", StringType(), True),
    ]
)


parsed_stream = kafka_stream \
    .select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select('data.*') \
    .withColumn("timestamp", col("timestamp").cast("timestamp")) \

# 1. Traffic Volume per Sensor
volume_stream = parsed_stream \
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("sensor_id")
    ).agg(sum("vehicle_count").alias("total_count"))

vehicle_count = Counter(
    "vehicle_count_total", "Total number of vehicles", ["sensor_id"]
)


def process_event(batch_df, batch_id):
    print(f"Batch ID: {batch_id}")
    batch_df.show(truncate=False)


ANALYSIS_TOPIC = 'traffic_analysis'


def write_to_kafka(batch_df, batch_id):
    batch_df \
        .select(to_json(struct("*")).alias("value")) \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", ANALYSIS_TOPIC) \
        .save()
    # Increment the Prometheus counter
    for row in batch_df.collect():
        # Reset counter before incrementing to avoid double counting
        vehicle_count.labels(sensor_id=row.sensor_id)._value.set(0)
        vehicle_count.labels(sensor_id=row.sensor_id)._value.set(row.total_count)
        print(f"Sent event: {row.sensor_id} with count {row.total_count}")


volume_query = volume_stream.writeStream \
    .outputMode("update") \
    .foreachBatch(process_event) \
    .start()
volume_query = volume_stream.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_kafka) \
    .start()

volume_query.awaitTermination()
