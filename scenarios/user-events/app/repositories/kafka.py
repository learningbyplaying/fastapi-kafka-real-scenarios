from confluent_kafka import avro
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.deserializing_consumer import DeserializingConsumer
from confluent_kafka.avro import AvroProducer

import os, time, json

class KafkaProducer:

    def __init__(self,**kwargs):

        self.topic = kwargs.get('topic')
        self.schema = kwargs.get('schema')
        self.config = kwargs.get('config')

        self.producer = AvroProducer(self.config, default_value_schema=self.schema)

    def run(self,message):

        self.producer.produce(topic=self.topic['topic'], value=message)
        self.producer.flush()

class KafkaConsumer:

    def __init__(self,**kwargs):

        self.topic = kwargs.get('topic')
        self.schema = kwargs.get('schema')
        self.schema_registry_url = kwargs.get('schema_registry_url')
        self.bootstrap_servers = kwargs.get('bootstrap_servers')

        schema_registry_client = SchemaRegistryClient( {"url": self.schema_registry_url  } )

        self.num_partitions = self.topic['num_partitions']

        serializer = AvroDeserializer(schema_str=self.schema, schema_registry_client=schema_registry_client)

        conf = {
            'bootstrap.servers': self.bootstrap_servers,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'group.id': 'my-group',
            'api.version.request': True,
            'api.version.fallback.ms': 0,
            "value.deserializer": serializer,
        }

        self.consumer = DeserializingConsumer(conf)
        self.consumer.subscribe([ self.topic['topic'] ])

    def run(self, datastore):

        while True:
            message = self.consumer.poll(timeout=5.0)
            # id created to track logic through logs
            if message is None:
                continue
            else:
                data = message.value()
                print(data, message.key)
                datastore.store( message.key, data, self.num_partitions)
                #Sink into datalake

            self.consumer.commit(asynchronous=True)

        self.consumer.close()
