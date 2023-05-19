from fastapi import FastAPI
from pydantic import BaseModel

class PublisherMessage(BaseModel):
    text: str

app = FastAPI()

@app.post("/producer/publisher")
async def producer(message: PublisherMessage):

    producer = Producer(producer_config)
    producer.produce(topic, value=message.text.encode('utf-8'))
    producer.flush()
    return {"Kafka": "PublisherMessage"}
