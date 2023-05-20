from confluent_kafka import Consumer, KafkaError, KafkaException, avro
from avro.io import DatumReader, BinaryDecoder
import io
import time, json, argparse

conf = {
    'bootstrap.servers': 'kafka:9092',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
    'group.id': 'my-group',
    'api.version.request': True,
    'api.version.fallback.ms': 0
}

def deserialize_avro_message(message_value, avro_schema):
    bytes_reader = io.BytesIO(message_value)
    decoder = BinaryDecoder(bytes_reader)
    reader = DatumReader(avro_schema)
    message = reader.read(decoder)
    return message

def consume_messages(channel):

    base_path = '/app/channels/{}'.format(channel)
    avro_schema = avro.load('{}/schema.avsc'.format(base_path))
    json_file = "{}/topic.json".format(base_path)

    json_data = json.load(open(json_file))
    topic = json_data['topic']
    print(topic, avro_schema)

    consumer = Consumer(conf)
    consumer.subscribe([topic])

    #try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f'Reached end of partition: {msg.topic()}[{msg.partition()}]')
            else:
                print(f'Error while consuming messages: {msg.error()}')
        else:
            #print(f"Received message: {msg.value().decode('utf-8')}")

            message_value = msg.value()
            print(message_value)

            bytes_reader = io.BytesIO(message_value)
            print(bytes_reader)

            decoder = BinaryDecoder(bytes_reader)
            print(decoder)

            reader = DatumReader(avro_schema)
            print(reader)

            message = reader.read(decoder)
            print(message)
            #avro_message = deserialize_avro_message(message_value,avro_schema)

            # Process the Avro message
            #print(avro_message)

    #except Exception as e:
    #    print(f"Exception occurred while consuming messages: {e}")
    #finally:
    consumer.close()

def startup(channel):
    consume_messages(channel)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description = 'Example with non-optional arguments')
    parser.add_argument('--channel', action = "store")
    channel = parser.parse_args().channel

    while True:
        #try:
        print(">>Run batch consumer...")
        startup(channel)
        #except Exception as e:
        #    print(f"Exception occurred: {e}")

        time.sleep(2)  # Sleep for 1 second
