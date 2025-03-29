from pyspark.sql import SparkSession
from producer import EVENTS_TOPIC
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import from_json, to_json, struct, sum, col, window, count, when, desc, current_timestamp, row_number, first, lag
from pyspark.sql.window import Window

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
    ).agg(sum("vehicle_count").alias("total_vehicle_count"))

# 2. Detect Congestion Hotspots
congestion_stream = parsed_stream \
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("sensor_id")
    ) \
    .agg(
        # first("congestion_level").alias("congestion_level"),
        count(when(col('congestion_level') == 'HIGH', True)).alias("window_count")
    ) \

sensor_window = Window.partitionBy("sensor_id").orderBy("window")

hotspot_stream = congestion_stream \
    .withColumn("prev_1", lag('window_count', 1).over(sensor_window)) \
    .withColumn("prev_2", lag('window_count', 2).over(sensor_window)) \



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


volume_query = volume_stream.writeStream \
    .outputMode("update") \
    .foreachBatch(process_event) \
    .start()
volume_query = volume_stream.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_kafka) \
    .start()

volume_query.awaitTermination()
