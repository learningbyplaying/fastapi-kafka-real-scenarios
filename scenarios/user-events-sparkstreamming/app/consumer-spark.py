from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

# Create a SparkSession
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .getOrCreate()

# Create a StreamingContext with a batch interval of 1 second
ssc = StreamingContext(spark.sparkContext, 1)

# Set the Kafka broker(s) and topic(s) to consume from
kafka_params = {
    "bootstrap.servers": "kafka:9092", #localhost:9092",
    "group.id": "my-consumer-group"
}
topics = ["ecommerce_events"]

# Create a Kafka direct stream
stream = KafkaUtils.createDirectStream(ssc, topics, kafka_params)

# Process each message in the stream
stream.foreachRDD(lambda rdd: rdd.foreach(print))

# Start the streaming context
ssc.start()

# Wait for the termination signal
ssc.awaitTermination()
