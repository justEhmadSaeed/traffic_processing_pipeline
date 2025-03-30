from pyspark.sql import SparkSession
from producer import EVENTS_TOPIC
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql.functions import from_json, to_json, struct, sum, col, window, avg, lag, desc
from pyspark.sql.window import Window
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

def clean_and_aggregate(batch_df, batch_id):
    """
    Cleans, validates, and aggregates batch data.
    """
    logger.info(f"Cleaning & Aggregating Batch ID: {batch_id}")

    # Remove invalid rows and duplicates
    batch_df = batch_df.na.drop(subset=["sensor_id", "timestamp", "vehicle_count", "average_speed"]) \
        .dropDuplicates(["sensor_id", "timestamp"]) \
        .filter(
            (col("vehicle_count") >= 0) & 
            (col("average_speed") > 0)
        )

    traffic_volume = batch_df \
        .groupBy(window(col("timestamp"), "5 minutes"), col("sensor_id")) \
        .agg(sum("vehicle_count").alias("total_count"))

    window_spec = Window.partitionBy("sensor_id").orderBy(col("timestamp"))
    batch_df = batch_df.withColumn("prev_congestion", lag("congestion_level", 1).over(window_spec)) \
                       .withColumn("prev2_congestion", lag("congestion_level", 2).over(window_spec))
    
    congestion_hotspots = batch_df.filter(
        (col("congestion_level") == "HIGH") & 
        (col("prev_congestion") == "HIGH") & 
        (col("prev2_congestion") == "HIGH")
    ).select("sensor_id", "timestamp", "congestion_level")

    avg_speed = batch_df \
        .groupBy(window(col("timestamp"), "10 minutes", "5 minutes"), col("sensor_id")) \
        .agg(avg("average_speed").alias("avg_speed"))

    batch_df = batch_df.withColumn("prev_speed", lag("average_speed", 1).over(window_spec))
    speed_drops = batch_df.filter(
        (col("prev_speed").isNotNull()) & 
        ((col("prev_speed") - col("average_speed")) / col("prev_speed") >= 0.5)
    ).select("sensor_id", "timestamp", "average_speed", "prev_speed")

    busiest_sensors = batch_df \
        .groupBy(window(col("timestamp"), "30 minutes"), col("sensor_id")) \
        .agg(sum("vehicle_count").alias("total_count")) \
        .orderBy(desc("total_count")) \
        .limit(3)

    return traffic_volume, congestion_hotspots, avg_speed, speed_drops, busiest_sensors

def process_event(batch_df, batch_id):
    """
    Processes and logs events from the data stream.
    """
    logger.info(f"Processing Batch ID: {batch_id} - Streaming Data")
    traffic_volume, congestion_hotspots, avg_speed, speed_drops, busiest_sensors = clean_and_aggregate(batch_df, batch_id)

    logger.info("Traffic Volume per Sensor:")
    traffic_volume.show(truncate=False)

    logger.info("Congestion Hotspots:")
    congestion_hotspots.show(truncate=False)

    logger.info("Average Speed per Sensor:")
    avg_speed.show(truncate=False)

    logger.info("Sudden Speed Drops:")
    speed_drops.show(truncate=False)

    logger.info("Busiest Sensors:")
    busiest_sensors.show(truncate=False)

def write_to_kafka(batch_df, batch_id):
    """
    Writes processed batch data to Kafka and updates Prometheus metrics.
    """
    logger.info(f"Processing Batch ID: {batch_id} - Writing to Kafka")
    traffic_volume, congestion_hotspots, avg_speed, speed_drops, busiest_sensors = clean_and_aggregate(batch_df, batch_id)

    def send_to_kafka(df, topic=ANALYSIS_TOPIC):
        df.select(to_json(struct("*")).alias("value")) \
          .write \
          .format("kafka") \
          .option("kafka.bootstrap.servers", "localhost:9092") \
          .option("topic", topic) \
          .save()

    send_to_kafka(traffic_volume)
    send_to_kafka(congestion_hotspots)
    send_to_kafka(avg_speed)
    send_to_kafka(speed_drops)
    send_to_kafka(busiest_sensors)

    for row in traffic_volume.collect():
        vehicle_count.labels(sensor_id=row.sensor_id)._value.set(row.total_count)
        logger.info(f"Sent event: {row.sensor_id} with count {row.total_count}")

volume_query = parsed_stream.writeStream.outputMode("update").foreachBatch(process_event).start()
kafka_query = parsed_stream.writeStream.outputMode("update").foreachBatch(write_to_kafka).start()

volume_query.awaitTermination()
kafka_query.awaitTermination()
