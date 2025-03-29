from pyspark.sql import SparkSession
from producer import TOPIC
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import from_json, col

spark = SparkSession.builder.appName("TrafficAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", TOPIC) \
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


events_stream = kafka_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select('data.*') \
    .withColumn("timestamp", col("timestamp").cast("timestamp")) \
    .withColumn("vehicle_count", col("vehicle_count").cast("integer")) \
    .withColumn("average_speed", col("average_speed").cast("double")) \



def process_event(batch_df, batch_id):
    print(f"Batch ID: {batch_id}")
    batch_df.show(truncate=False)


query = events_stream.writeStream \
    .outputMode("update") \
    .foreachBatch(process_event) \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()
