from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("HelloWorld") \
    .getOrCreate()

# Create a DataFrame with a single row containing the message
data = [("Hello, World!",)]
df = spark.createDataFrame(data, ["message"])

# Show the DataFrame
df.show()

# Stop the SparkSession
spark.stop()
