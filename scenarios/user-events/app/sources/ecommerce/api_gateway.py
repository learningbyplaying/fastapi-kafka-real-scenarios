from fastapi import FastAPI
from pydantic import BaseModel, Field
from datetime import datetime, timezone, timedelta


from repositories.kafka import KafkaProducer

import os, json
from confluent_kafka import avro
## Settings
from dotenv import load_dotenv
load_dotenv()

#Infraestructue
base_path = os.getenv("base_path")
source_path = f"{base_path}/ecommerce"
producer_config = {'bootstrap.servers': os.getenv("bootstrap.servers"),'client.id': 'fastapi-producer', 'schema.registry.url':os.getenv("schema_registry_url")}

#Schema
avro_schema = avro.load(f"{source_path}/schema.avsc")
topic_data = json.load(open( f"{source_path}/topic.json"))

#endpoint
app = FastAPI()

class EcommerceEvent(BaseModel):
    event_type: str
    user_id: str

    url: str = None
    product_id: str = None
    text: str = None
    order_id: str = None
    search: str = None

    created_date: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

@app.post("/ecommerce/events",  tags=['Ecommerce'])
async def events(event: EcommerceEvent):

    event_dict = event.dict()
    del event_dict['created_date']
    event_dict['epoch'] = int(event.created_date.timestamp())
    #print(event_dict)

    kp = KafkaProducer(topic=topic_data,schema=avro_schema,config=producer_config)
    kp.run(event_dict)

    return {"Kafka": "EcommerceMessage"}
