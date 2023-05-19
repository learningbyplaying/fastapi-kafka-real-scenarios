from fastapi import FastAPI
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from pydantic import BaseModel

app = FastAPI()

producer_config = {
    'bootstrap.servers': 'kafka:9092',  # Update with your Kafka broker's address
    'client.id': 'fastapi-producer'
}

topic = 'my_topic'

async def kafka_startup():
    # Kafka AdminClient Configuration
    admin_config = {
        'bootstrap.servers': 'kafka:9092'  # Update with your Kafka broker's address
    }
    # Create AdminClient
    admin_client = AdminClient(admin_config)
    # Define the topic to be created
    topic_name = topic
    num_partitions = 3  # Specify the number of partitions for the topic
    replication_factor = 1  # Specify the replication factor for the topic

    # Create a NewTopic object with the topic configuration
    new_topic = NewTopic(topic_name, num_partitions, replication_factor)
    # Create the topic using the AdminClient
    admin_client.create_topics([new_topic])
    print(admin_client.list_topics().topics)


class Message(BaseModel):
    text: str

@app.post("/")
async def read_root(message: Message):

    await kafka_startup()

    producer = Producer(producer_config)
    producer.produce(topic, value=message.text.encode('utf-8'))
    producer.flush()

    return {"Kafka": "Producer"}
