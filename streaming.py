from pyspark.sql import SparkSession
from producer import TOPIC

spark = SparkSession.builder.appName("TrafficAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

