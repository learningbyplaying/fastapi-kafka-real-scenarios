from fastapi import FastAPI
from pydantic import BaseModel
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic


from dotenv import load_dotenv
import os
load_dotenv()
KAFKA_SERVICE=os.getenv("KAFKA_SERVICE")
admin_config = {'bootstrap.servers': '{}:9092'.format(KAFKA_SERVICE)}
producer_config = {'bootstrap.servers': '{}:9092'.format(KAFKA_SERVICE),'client.id': 'fastapi-producer'}

topic = "ecommerce"
num_partitions = 2  # Specify the number of partitions for the topic
replication_factor = 1  # Specify the replication factor for the topic

class EcommerceMessage(BaseModel):
    text: str

app = FastAPI()

@app.post("/ecommerce/setup")
async def setup():
    admin_client = AdminClient(admin_config)
    new_topic = NewTopic(topic, num_partitions, replication_factor)
    admin_client.create_topics([new_topic])
    print(admin_client.list_topics().topics)
    return {"Kafka": "Setup"}


@app.post("/ecommerce/producer")
async def producer(message: EcommerceMessage):

    producer = Producer(producer_config)
    producer.produce(topic, value=message.text.encode('utf-8'))
    producer.flush()
    return {"Kafka": "EcommerceMessage"}
