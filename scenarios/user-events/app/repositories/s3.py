from datetime import datetime
import boto3, json, time, os


class S3DataStore:

    def __init__(self,**kwargs):

        self.bucket = kwargs.get('bucket')
        self.prefix = kwargs.get('prefix')

        #self.client = boto3.client('s3')

        self.client = boto3.resource('s3',
            aws_access_key_id= os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key= os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name= os.getenv("AWS_DEFAULT_REGION")
        )

    # Define a partitioner function to determine the S3 partition path based on the message key
    def partitioner_old(self,key,num_partitions):
        partition_id = hash(key) % num_partitions
        current_time = datetime.utcnow()
        partition_path = f"/year={current_time.year}/month={current_time.month}/day={current_time.day}/hour={current_time.hour}/partition={partition_id}/"
        return partition_path


    def partitioner(self, key, num_partitions):

        partition_id = hash(key) % num_partitions
        current_time = datetime.utcnow()
        partition_path = f"year={current_time.year:04}/month={current_time.month:02}/day={current_time.day:02}/hour={current_time.hour:02}/partition={partition_id}/"

        return partition_path


    def store(self, key, value, num_partitions):

        # Get the current S3 partition path based on the message key
        partition_path = self.partitioner(key, num_partitions)
        # Convert Avro record to JSON
        json_message = json.dumps(value)
        data_bytes = json.dumps(value).encode('utf-8')
        # Generate a unique file name for each message based on the current timestamp
        file_name = f"{int(time.time() * 1000)}.json"

        # Prepare the S3 object key
        prefix = self.prefix
        s3_key = f"{prefix}{partition_path}{file_name}"

        print(self.bucket,s3_key,data_bytes)


        some_binary_data = b'Here we have some data'
        more_binary_data = b'Here we have some more data'
        object = self.client.Object(self.bucket, 'my/key/including/filename.txt')
        object.put(Body=some_binary_data)

        object = self.client.Object(self.bucket, s3_key)
        object.put(Body=data_bytes)

        #result = self.client.put_object(Body=data_bytes, Bucket=self.bucket, Key=s3_key)

        print(object)
