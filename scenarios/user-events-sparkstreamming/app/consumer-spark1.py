from pyspark.sql import SparkSession
from pyspark.sql.functions import window
#from pyspark.sql.avro.functions import from_avro

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

df_connect = spark\
    .readStream.format("kafka")\
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)\
    .option("subscribe", KAFKA_TOPIC)\
    .option("startingOffsets", "latest")\
    .load()

#output = df_connect\
#  .select(from_avro("value", jsonFormatSchema).alias("event"))


step1 = df_connect.select(
    # Convert the value to a string
    F.from_json(
        F.decode(F.col("value"), "iso-8859-1"),
        SCHEMA
    ).alias("value")
).select("value.*")

#windowed_df = df_connect
# Apply a window to the streaming DataFrame
windowed_df = df_connect \
    .withColumn("window",window("timestamp","10 minutes")) #,"1 minutes"))
#    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp") \
#    .withWatermark("timestamp", "10 minutes")
#\
#    .groupBy(window("timestamp", "5 minutes"), "key") \
#    .count()


# Perform transformations and actions on the windowed DataFrame
query = windowed_df \
    .writeStream \
    .outputMode("update") \
    .trigger(processingTime="2 minutes")\
    .format("console")\
    .start()

#    .option("truncate", "false") \

# Start the streaming query
query.awaitTermination()

"""
result = df_connect\
    .writeStream\
    .outputMode("append")\
    .format("console")\
    .start()\
    .awaitTermination()
"""
