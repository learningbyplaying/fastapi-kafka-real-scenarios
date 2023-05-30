from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HelloWorld") \
    .getOrCreate()

data = [("Hello, World!",)]
df = spark.createDataFrame(data, ["message"])

df.show()

spark.stop()
