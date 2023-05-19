from confluent_kafka import Consumer, KafkaError

conf = {
    'bootstrap.servers': 'kafka:9092',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
    'group.id': 'my-group',
    'api.version.request': True,
    'api.version.fallback.ms': 0
}

def consume_messages():
    consumer = Consumer(conf)

    consumer.subscribe(['my_topic'])

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

def startup():
    consume_messages()

if __name__ == "__main__":
    try:
        print("Starting consumer...")
        startup()
    except Exception as e:
        print(f"Exception occurred: {e}")
