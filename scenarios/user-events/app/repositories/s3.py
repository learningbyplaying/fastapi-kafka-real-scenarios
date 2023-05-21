import time
from datetime import datetime
import boto3
import json


# S3 configuration
s3_bucket_name = 'your_s3_bucket_name'
s3_prefix = 'your_s3_prefix/'

# Initialize S3 client
s3_client = boto3.client('s3')

# Define a partitioner function to determine the S3 partition path based on the message key
def partitioner(key):
    partition_id = hash(key) % num_partitions
    current_time = datetime.utcnow()
    partition_path = f"{current_time.year}/{current_time.month}/{current_time.day}/{current_time.hour}/{partition_id}/"
    return partition_path

# Consume Kafka messages and store them in S3
for message in consumer:
    # Get the current S3 partition path based on the message key
    partition_path = partitioner(message.key)

    # Convert Avro record to JSON
    json_message = json.dumps(message.value)

    # Generate a unique file name for each message based on the current timestamp
    file_name = f"{int(time.time() * 1000)}.json"

    # Prepare the S3 object key
    s3_key = f"{s3_prefix}{partition_path}{file_name}"

    # Write the message to S3
    s3_client.put_object(
        Bucket=s3_bucket_name,
        Key=s3_key,
        Body=json_message
    )
