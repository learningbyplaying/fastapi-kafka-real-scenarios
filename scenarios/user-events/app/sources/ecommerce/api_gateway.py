from fastapi import FastAPI
from pydantic import BaseModel

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

import os, json

## Settings
from dotenv import load_dotenv
import os
load_dotenv()

#Infraestructue
base_path = os.getenv("base_path")
source_path = f"{base_path}/ecommerce"
producer_config = {'bootstrap.servers': os.getenv("bootstrap.servers"),'client.id': 'fastapi-producer', 'schema.registry.url':os.getenv("schema_registry_url")}

#Schema
avro_schema = avro.load(f"{source_path}/schema.avsc")
json_file = f"{source_path}/topic.json"
json_data = json.load(open(json_file))
topic = json_data['topic']

#endpoint
app = FastAPI()

class EcommerceEvent(BaseModel):
    event_type: str
    time: str
    user_id: str
    url: str
    text: str

@app.post("/events/gateway",  tags=['Ecommerce'])
async def events(message: EcommerceEvent):
    #print(message.dict())
    producer = AvroProducer(producer_config, default_value_schema=avro_schema)
    producer.produce(topic=topic, value=message.dict())
    producer.flush()
    return {"Kafka": "EcommerceMessage"}
