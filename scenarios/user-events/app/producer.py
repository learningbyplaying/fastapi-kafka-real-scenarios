from fastapi import FastAPI
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from models import KafkaTopic

## Settings
from dotenv import load_dotenv
import os
load_dotenv()
admin_config = {'bootstrap.servers': 'kafka:9092'}

app = FastAPI()

@app.post("/kafka-topic-create")
async def setup(topic: KafkaTopic):
    admin_client = AdminClient(admin_config)
    new_topic = NewTopic(topic.topic, topic.num_partitions, topic.replication_factor)
    admin_client.create_topics([new_topic])
    topics  = admin_client.list_topics(topic=topic.topic).topics
    return {"Message": "Kafka Topic created", "Items": topics}


from channels.ecommerce.api_gateway import app as ecommerce
app.include_router(ecommerce.router)

#from channels.publishers.publishers import app as publishers
#app.include_router(publishers.router)
