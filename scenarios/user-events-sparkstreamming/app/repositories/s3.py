from datetime import datetime
import boto3, json, time, os


class S3DataStore:

    def __init__(self,**kwargs):

        self.bucket = kwargs.get('bucket')
        self.prefix = kwargs.get('prefix')

        self.client = boto3.resource('s3',
            aws_access_key_id= os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key= os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name= os.getenv("AWS_DEFAULT_REGION")
        )

    def store(self, partition_path, data):

        data_bytes = json.dumps(data).encode('utf-8')

        prefix = self.prefix
        file_name = f"{int(time.time() * 1000)}.json"
        s3_key = f"{prefix}{partition_path}{file_name}"

        print(self.bucket,s3_key,data_bytes)
        object = self.client.Object(self.bucket, s3_key)
        object.put(Body=data_bytes)
        print(object)
