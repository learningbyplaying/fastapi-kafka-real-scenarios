from pyspark.sql import SparkSession
import pyspark.sql.functions as psf
from pyspark.sql.types import *
import io
import fastavro

def deserialize_avro(serialized_msg):
    bytes_io = io.BytesIO(serialized_msg)
    bytes_io.seek(0)
    avro_schema = {
                    "type": "record",
                    "name": "struct",
                    "fields": [
                      {"name": "col1", "type": "long"},
                      {"name": "col2", "type": "string"}
                    ]
                  }

    deserialized_msg = fastavro.schemaless_reader(bytes_io, avro_schema)

    return (    deserialized_msg["col1"],
                deserialized_msg["col2"]
            )

if __name__=="__main__":
  spark = SparkSession \
        .builder \
        .appName("consume kafka message") \
        .getOrCreate()

  kafka_df = spark \
              .readStream \
              .format("kafka") \
              .option("kafka.bootstrap.servers", "kafka:9092") \
              .option("subscribe", "ecommerce_events") \
              .option("stopGracefullyOnShutdown", "true") \
              .load()

  df_schema = StructType([
              StructField("col1", LongType(), True),
              StructField("col2", StringType(), True)
          ])

  avro_deserialize_udf = psf.udf(deserialize_avro, returnType=df_schema)
  parsed_df = kafka_df.withColumn("avro", avro_deserialize_udf(psf.col("value"))).select("avro.*")

  query = parsed_df.writeStream.format("console").option("truncate", "true").start()
  query.awaitTermination()
