from confluent_kafka import avro, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.deserializing_consumer import DeserializingConsumer
from confluent_kafka.avro import AvroProducer

from datetime import datetime
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

    def __init__(self,**kwargs):

        self.consumer = kwargs.get('consumer')
        self.num_partitions = kwargs.get('num_partitions')

        self.batch_size_max = 1000
        self.minutes_timeout = 30
        self.batch_timeout = 60 * self.minutes_timeout   # Timeout in seconds
        self.batch_start = time.time()
        self.partition = None
        self.batch = []

        print('>> Init Batch ',self.batch_start, len(self.batch))

    def gather(self):

        while len(self.batch) < self.batch_size_max:

            current_at = time.time()
            diff = (current_at - self.batch_start)
            if diff > self.batch_timeout:
                print(">> Batch timeout", diff, len(self.batch))
                break

            message = self.consumer.poll(timeout=1.0)  # Poll for a single message
            if message is None:
                print(">> No Message",diff, len(self.batch))
                continue
            elif not message.error():

                self.partition = message.partition()
                self.batch.append(message.value())

        print('>> Gather Batch ', len(self.batch))
        return self

    def release(self):

        print('>> Release Batch ', len(self.batch))

        if len(self.batch) > 0:
            partition = self.partition #self.batch[0].partition()
            partition_id = hash(partition) % self.num_partitions
            partition_path = self.partitioner(partition_id)
            return {"partition_id": partition_id, "partition_path": partition_path, "batch": self.batch}
        return None

    def partitioner(self, partition_id):
        current_time = datetime.utcnow()
        partition_path = f"year={current_time.year:04}/month={current_time.month:02}/day={current_time.day:02}/hour={current_time.hour:02}/partition={partition_id}/"
        return partition_path


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

                batch = Batch( consumer=self.consumer, num_partitions=self.num_partitions  ).gather().release()
                if batch != None:
                    datastore.store( batch['partition_path'], batch['batch'])

        except KeyboardInterrupt:
            self.consumer.close()
