from pyspark.sql import SparkSession
from pyspark.sql.functions import window

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
jsonFormatSchema = open("/app/sources/ecommerce/schema.avsc", "r").read()

# Reduce logging verbosity
spark.sparkContext.setLogLevel("WARN")

# Define a function to handle the writing process
def write_to_s3(df, epoch_id):
    batch_time = epoch_id
    s3_path = f"s3a://your_bucket/path/{batch_time}/"

    # Write the DataFrame to S3
    df.write \
        .format("parquet") \
        .mode("append") \
        .save(s3_path)


df_connect = spark\
    .readStream.format("kafka")\
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)\
    .option("subscribe", KAFKA_TOPIC)\
    .option("startingOffsets", "latest")\
    .load()

windowed_df = df_connect \
    .withColumn("window",window("timestamp","10 minutes")) #,"1 minutes"))

query = windowed_df \
    .writeStream \
    .outputMode("update") \
    .format("console")\
    .foreachBatch(write_to_s3) \
    .start()

# Start the streaming query
query.awaitTermination()
