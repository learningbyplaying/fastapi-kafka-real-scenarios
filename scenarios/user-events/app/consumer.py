from confluent_kafka import avro
import io
import time, json, argparse

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.deserializing_consumer import DeserializingConsumer


def consume_messages(channel):

    base_path = '/app/channels/{}'.format(channel)
    schema_registry_configuration = {
        "url": "http://schema-registry:8081",
    }
    schema_registry_client = SchemaRegistryClient(schema_registry_configuration)
    
    schema_str = open(f"{base_path}/schema.avsc").read()
    json_file = "{}/topic.json".format(base_path)
    json_data = json.load(open(json_file))
    topic = json_data['topic']

    # The AVRO Schema


    serializer = AvroDeserializer(schema_str=schema_str, schema_registry_client=schema_registry_client)

    conf = {
        'bootstrap.servers': 'kafka:9092',
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
            print(message.value())
        consumer.commit(asynchronous=True)

    consumer.close()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description = 'Example with non-optional arguments')
    parser.add_argument('--channel', action = "store")
    channel = parser.parse_args().channel

    while True:
        #try:
        print(">>Run batch consumer...")
        consume_messages(channel)
        time.sleep(2)
