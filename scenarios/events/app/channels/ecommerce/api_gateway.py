from fastapi import FastAPI
from pydantic import BaseModel

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

import os, json

#Infraestructue
base_path = '/app/channels/ecommerce'
schema_registry_url = 'http://schema-registry:8081'
producer_config = {'bootstrap.servers': 'kafka:9092','client.id': 'fastapi-producer', 'schema.registry.url': schema_registry_url}

#Schema
avro_schema = avro.load('{}/ecommerce_event.avsc'.format(base_path))
json_file = "{}/topic.json".format(base_path)
json_data = json.load(open(json_file))
topic = json_data['topic']

#endpoint
app = FastAPI()

class EcommerceMessage(BaseModel):
    event_type: str
    time: str
    user_id: str
    url: str
    text: str

@app.post("/events/gateway",  tags=['Ecommerce'])
async def events(message: EcommerceMessage):
    #print(message.dict())
    producer = AvroProducer(producer_config, default_value_schema=avro_schema)
    producer.produce(topic=topic, value=message.dict())
    producer.flush()
    return {"Kafka": "EcommerceMessage"}
