from confluent_kafka import Consumer, KafkaError, avro
import time, json, argparse

conf = {
    'bootstrap.servers': 'kafka:9092',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
    'group.id': 'my-group',
    'api.version.request': True,
    'api.version.fallback.ms': 0
}

def consume_messages(channel):

    base_path = '/app/channels/{}'.format(channel)
    avro_schema = avro.load('{}/ecommerce_event.avsc'.format(base_path))
    json_file = "{}/topic.json".format(base_path)
    
    json_data = json.load(open(json_file))
    topic = json_data['topic']
    print(topic)

    consumer = Consumer(conf)
    consumer.subscribe([topic])

    try:
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
                print(f"Received message: {msg.value().decode('utf-8')}")

    except Exception as e:
        print(f"Exception occurred while consuming messages: {e}")
    finally:
        consumer.close()

def startup(channel):
    consume_messages(channel)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description = 'Example with non-optional arguments')
    parser.add_argument('--channel', action = "store")
    channel = parser.parse_args().channel

    while True:
        try:
            print("Starting consumer...")
            startup(channel)
        except Exception as e:
            print(f"Exception occurred: {e}")

        time.sleep(5)  # Sleep for 1 second
