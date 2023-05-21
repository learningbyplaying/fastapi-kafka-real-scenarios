import time, json, argparse, os
from dotenv import load_dotenv
load_dotenv()

from repositories.kafka import KafkaConsumer

def consume_events(source):

    schema_registry_url = os.getenv("schema_registry_url")
    bootstrap_servers = os.getenv("bootstrap.servers")
    base_path = os.getenv("base_path")

    source_path = f"{base_path}/{source}"
    schema_str = open(f"{source_path}/schema.avsc").read()
    topic_data = json.load(open(f"{source_path}/topic.json"))

    kc = KafkaConsumer(
        topic=topic_data,
        schema=schema_str,
        schema_registry_url=schema_registry_url,
        bootstrap_servers=bootstrap_servers
    )

    kc.run()

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description = 'Example with non-optional arguments')
    parser.add_argument('--source', action = "store")
    source = parser.parse_args().source

    while True:
        #try:
        print(">>Run batch consumer...")
        consume_events(source)
        time.sleep(2)
