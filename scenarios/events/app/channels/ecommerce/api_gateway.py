from fastapi import FastAPI
from pydantic import BaseModel

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

from .models import EcommerceMessage

from dotenv import load_dotenv
import os, json
load_dotenv()

#Infraestructue
schema_registry_url = 'http://schema-registry:8081'
producer_config = {'bootstrap.servers': 'kafka:9092','client.id': 'fastapi-producer', 'schema.registry.url': schema_registry_url}

#Schema
avro_schema = avro.load('/app/channels/ecommerce/ecommerce_event.avsc')
json_file = "/app/channels/ecommerce/topic.json"
json_data = json.load(open(json_file))
topic = json_data['topic']


app = FastAPI()

@app.post("/events/gateway",  tags=['Ecommerce'])
async def events(message: EcommerceMessage):
    #print(message.dict())
    producer = AvroProducer(producer_config, default_value_schema=avro_schema)
    producer.produce(topic=topic, value=message.dict())
    producer.flush()
    return {"Kafka": "EcommerceMessage"}
