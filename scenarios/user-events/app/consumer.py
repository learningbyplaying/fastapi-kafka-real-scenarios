import time, json, argparse, os
from dotenv import load_dotenv
load_dotenv()
load_dotenv('/app/.credentials')

from repositories.kafka import KafkaConsumer
from repositories.s3 import S3DataStore

def run(source):

    bucket_datalake = os.getenv("bucket_datalake")
    schema_registry_url = os.getenv("schema_registry_url")
    bootstrap_servers = os.getenv("bootstrap.servers")
    base_path = os.getenv("base_path")

    #print( os.getenv("AWS_ACCESS_KEY_ID"))
    #print( os.getenv("AWS_SECRET_ACCESS_KEY"))


    source_path = f"{base_path}/{source}"
    schema_str = open(f"{source_path}/schema.avsc").read()
    topic_data = json.load(open(f"{source_path}/topic.json"))
    topic_name = topic_data['topic']

    kc = KafkaConsumer(
        topic=topic_data,
        schema=schema_str,
        schema_registry_url=schema_registry_url,
        bootstrap_servers=bootstrap_servers,
    )

    prefix = f"repositories/kafka/topic={topic_name}"
    ds = S3DataStore(bucket=bucket_datalake, prefix = prefix)

    kc.run(ds)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description = 'Example with non-optional arguments')
    parser.add_argument('--source', action = "store")
    source = parser.parse_args().source

    while True:
        #try:
        print(">>Run batch consumer...")
        run(source)
        time.sleep(5)
