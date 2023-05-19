from fastapi import FastAPI
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from models import EcommerceMessage, PublisherMessage

from dotenv import load_dotenv
import os
load_dotenv()

app = FastAPI()

KAFKA_SERVICE=os.getenv("KAFKA_SERVICE")
EVENTS_TOPIC=os.getenv("EVENTS_TOPIC")
print(os.getenv("APP_NAME"))

admin_config = {
    'bootstrap.servers': 'kafka:9092' # Update with your Kafka broker's address
}
producer_config = {
    'bootstrap.servers': 'kafka:9092',  # Update with your Kafka broker's address
    'client.id': 'fastapi-producer'
}
topic = EVENTS_TOPIC
num_partitions = 3  # Specify the number of partitions for the topic
replication_factor = 1  # Specify the replication factor for the topic

@app.post("/setup")
async def setup():

    admin_client = AdminClient(admin_config)
    new_topic = NewTopic(topic, num_partitions, replication_factor)
    admin_client.create_topics([new_topic])
    print(admin_client.list_topics().topics)

    return {"Kafka": "Setup"}


@app.post("/producer/ecommerce")
async def producer(message: EcommerceMessage):

    producer = Producer(producer_config)
    producer.produce(topic, value=message.text.encode('utf-8'))
    producer.flush()

    return {"Kafka": "EcommerceMessage"}

@app.post("/producer/publisher")
async def producer(message: PublisherMessage):

    producer = Producer(producer_config)
    producer.produce(topic, value=message.text.encode('utf-8'))
    producer.flush()

    return {"Kafka": "PublisherMessage"}
