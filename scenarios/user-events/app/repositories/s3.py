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


    def topic_store(self, key, value, num_partitions):

        data_bytes = json.dumps(value).encode('utf-8')

        prefix = self.prefix
        partition_path = self.partitioner(key, num_partitions)
        file_name = f"{int(time.time() * 1000)}.json"
        s3_key = f"{prefix}{partition_path}{file_name}"

        print(self.bucket,s3_key,data_bytes)


        object = self.client.Object(self.bucket, s3_key)
        object.put(Body=data_bytes)
        print(object)
