from confluent_kafka import avro, KafkaException
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


class Batch:

    def __init__(self,consumer):

        self.consumer = consumer
        self.batch_size_max = 100
        self.batch_timeout = 60 * 30   # Timeout in seconds
        self.batch_start = time.time()
        self.batch = []

        print('>> Init Batch ',self.batch_start, len(self.batch))

    def gather(self):

        while len(self.batch) < self.batch_size_max:

            current_at = time.time()
            diff = (current_at - self.batch_start)
            if diff > self.batch_timeout:
                print(">> Batch timeout", len(self.batch))
                break

            message = self.consumer.poll(timeout=1.0)  # Poll for a single message
            if message is None:
                print(">> No Message", len(self.batch))
                continue
            elif not message.error():
                self.batch.append(message)

        print('>> Gather Batch ', len(self.batch))

    def release(self):

        if len(self.batch) > 0:

            partition = self.batch[0].partition()
            partition_id = hash(partition) % self.num_partitions
            release = {"partition_id": partition_id, "batch": self.batch}

        print('>> Release Batch ', len(self.batch))


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
            #'max.poll.records': 100
        }

        self.consumer = DeserializingConsumer(conf)
        self.consumer.subscribe([ self.topic['topic'] ])


    def run(self, datastore):

        try:
            while True:

                b = Batch( self.consumer  ).gather( ).release()

                #batch_parsed = self.parse_batch(batch)
                #datastore.topic_batch_store( message.key, batch_parsed, self.num_partitions)
        except KeyboardInterrupt:
            # Stop the Kafka consumer on keyboard interrupt
            self.consumer.close()

            #message = self.consumer.poll(timeout=5.0)
            # id created to track logic through logs
            #if message is None:
            #    continue
            #else:
            #    datastore.topic_store( message.key, message.value(), self.num_partitions)
            #self.consumer.commit(asynchronous=True)
