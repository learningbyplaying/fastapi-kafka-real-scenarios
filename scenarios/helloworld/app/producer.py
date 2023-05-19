from fastapi import FastAPI
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

app = FastAPI()

async def kafka_startup():
    # Kafka AdminClient Configuration
    admin_config = {
        'bootstrap.servers': 'kafka:9092'  # Update with your Kafka broker's address
    }
    # Create AdminClient
    admin_client = AdminClient(admin_config)
    # Define the topic to be created
    topic_name = 'my_topic'  # Update with your desired topic name
    num_partitions = 3  # Specify the number of partitions for the topic
    replication_factor = 1  # Specify the replication factor for the topic

    # Create a NewTopic object with the topic configuration
    new_topic = NewTopic(topic_name, num_partitions, replication_factor)
    # Create the topic using the AdminClient
    admin_client.create_topics([new_topic])
    print(admin_client.list_topics().topics)


@app.get("/")
async def read_root():

    await kafka_startup()
    producer_config = {
        'bootstrap.servers': 'kafka:9092',  # Update with your Kafka broker's address
        'client.id': 'fastapi-producer'
    }

        # Create Kafka Producer
    producer = Producer(producer_config)

    # Produce a message
    topic = 'my_topic'  # Update with your desired Kafka topic
    message = 'Hello Kafka!'
    producer.produce(topic, value=message.encode('utf-8'))

    # Flush and wait for delivery reports
    producer.flush()

    return {"Kafka": "Producer"}
