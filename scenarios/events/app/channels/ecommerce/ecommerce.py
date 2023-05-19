from fastapi import FastAPI
from pydantic import BaseModel

class EcommerceMessage(BaseModel):
    text: str
    
app = FastAPI()

@app.post("/producer/ecommerce")
async def producer(message: EcommerceMessage):

    producer = Producer(producer_config)
    producer.produce(topic, value=message.text.encode('utf-8'))
    producer.flush()
    return {"Kafka": "EcommerceMessage"}
