from fastapi import FastAPI
from confluent_kafka import Producer

app = FastAPI()

@app.get("/")
def read_root():

    producer_config = {
        'bootstrap.servers': 'kafka:9092',  # Update with your Kafka broker's address
        'client.id': 'fastapi-producer'
    }

        # Create Kafka Producer
    producer = Producer(producer_config)

    # Produce a message
    topic = 'my_topic'  # Update with your desired Kafka topic
    message = 'Hello Kafka!'
    producer.produce(topic, value=message.encode('utf-8'))

    # Flush and wait for delivery reports
    producer.flush()

    return {"Kafka": "Consumer"}
