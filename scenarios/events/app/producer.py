from fastapi import FastAPI
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

## Settings
from dotenv import load_dotenv
import os
load_dotenv()
KAFKA_SERVICE=os.getenv("KAFKA_SERVICE")
EVENTS_TOPIC=os.getenv("EVENTS_TOPIC")
admin_config = {'bootstrap.servers': '{}:9092'.format(KAFKA_SERVICE)}
producer_config = {'bootstrap.servers': '{}:9092'.format(KAFKA_SERVICE),'client.id': 'fastapi-producer'}


app = FastAPI()

from channels.ecommerce.ecommerce import app as ecommerce
app.include_router(ecommerce.router)

#from channels.publishers.publishers import app as publishers
#app.include_router(publishers.router)
