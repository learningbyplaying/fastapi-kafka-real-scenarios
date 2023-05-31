from dotenv import load_dotenv
import os
load_dotenv()
load_dotenv('/app/.credentials')

from pyspark.sql import SparkSession
from pyspark.sql.functions import window
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

spark = SparkSession.builder.appName("read_traffic_sensor_topic").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "ecommerce_events"

df_connect = spark\
    .readStream.format("kafka")\
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)\
    .option("subscribe", KAFKA_TOPIC)\
    .option("startingOffsets", "latest")\
    .load()

# Define a function to handle the writing process
def write_to_s3(df, epoch_id):
    batch_time = epoch_id
    s3_path = f"s3a://etl-on-yaml/repositories/kafka/{batch_time}/"
    df.write \
        .format("parquet") \
        .mode("append") \
        .save(s3_path)

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
