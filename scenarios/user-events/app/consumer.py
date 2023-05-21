from confluent_kafka import avro
import io
import time, json, argparse

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.deserializing_consumer import DeserializingConsumer

## Settings
from dotenv import load_dotenv
import os
load_dotenv()

def consume_events(source):

    base_path = os.getenv("base_path")
    source_path = f"{base_path}/{source}"
    schema_registry_client = SchemaRegistryClient( {"url": os.getenv("schema_registry_url") } )

    # The AVRO Schema
    schema_str = open(f"{source_path}/schema.avsc").read()
    json_file = f"{source_path}/topic.json"
    json_data = json.load(open(json_file))
    topic = json_data['topic']
    num_partitions = json_data['num_partitions']

    serializer = AvroDeserializer(schema_str=schema_str, schema_registry_client=schema_registry_client)

    conf = {
        'bootstrap.servers': os.getenv("bootstrap.servers"), #'kafka:9092',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
        'group.id': 'my-group',
        'api.version.request': True,
        'api.version.fallback.ms': 0,
        "value.deserializer": serializer,  # Serializer used for message values.
    }

    consumer = DeserializingConsumer(conf)
    consumer.subscribe([topic])

    while True:
        message = consumer.poll(timeout=5.0)
        # id created to track logic through logs
        if message is None:
            continue
        else:
            print(message.value(), message.key)
            #Sink into datalake

        consumer.commit(asynchronous=True)

    consumer.close()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description = 'Example with non-optional arguments')
    parser.add_argument('--source', action = "store")
    source = parser.parse_args().source

    while True:
        #try:
        print(">>Run batch consumer...")
        consume_events(source)
        time.sleep(2)
