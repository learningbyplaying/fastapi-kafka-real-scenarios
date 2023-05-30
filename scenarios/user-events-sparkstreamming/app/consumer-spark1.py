from pyspark.sql import SparkSession

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "ecommerce_events"

SCHEMA = StructType([
    StructField("event_type", StringType()),
    StructField("user_id", StringType()),
    StructField("url", StringType()),
    StructField("product_id", StringType()),
    StructField("text", StringType()),
    StructField("order_id", StringType()),
    StructField("search", StringType()),
    StructField("epoch", LongType())
])

spark = SparkSession.builder.appName("read_traffic_sensor_topic").getOrCreate()

# Reduce logging verbosity
spark.sparkContext.setLogLevel("WARN")

df_traffic_stream = spark\
    .readStream.format("kafka")\
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)\
    .option("subscribe", KAFKA_TOPIC)\
    .option("startingOffsets", "earliest")\
    .load()

df_traffic_stream.select(
    # Convert the value to a string
    F.from_json(
        F.decode(F.col("value"), "iso-8859-1"),
        SCHEMA
    ).alias("value")
)\
.select("value.*")\
.writeStream\
.outputMode("append")\
.format("console")\
.start()\
.awaitTermination()
