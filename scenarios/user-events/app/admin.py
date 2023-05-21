from fastapi import FastAPI
from pydantic import BaseModel

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

## Settings
from dotenv import load_dotenv
import os
load_dotenv()
admin_config = {'bootstrap.servers': os.getenv("bootstrap.servers")}

#endpoint
app = FastAPI()

class KafkaTopic(BaseModel):
    topic: str
    num_partitions: int = 1
    replication_factor: int = 1

@app.post("/kafka-topic-create")
async def setup(topic: KafkaTopic):
    admin_client = AdminClient(admin_config)
    new_topic = NewTopic(topic.topic, topic.num_partitions, topic.replication_factor)
    admin_client.create_topics([new_topic])
    topics  = admin_client.list_topics(topic=topic.topic).topics
    return {"Message": "Kafka Topic created", "Items": topics}
