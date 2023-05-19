from confluent_kafka.admin import AdminClient, NewTopic

# Kafka AdminClient Configuration
admin_config = {
    'bootstrap.servers': 'kafka:9092'  # Update with your Kafka broker's address
}

# Create AdminClient
admin_client = AdminClient(admin_config)

# Define the topic to be created
topic_name = 'my_topic'  # Update with your desired topic name
num_partitions = 3  # Specify the number of partitions for the topic
replication_factor = 1  # Specify the replication factor for the topic

# Create a NewTopic object with the topic configuration
new_topic = NewTopic(topic_name, num_partitions, replication_factor)


# Create the topic using the AdminClient
r = admin_client.create_topics([new_topic])

print(new_topic, admin_client, r)

# Close the AdminClient
#admin_client.close()
