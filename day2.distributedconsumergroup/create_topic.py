from kafka.admin import NewTopic
from kafka.admin import KafkaAdminClient


kafkaclient=KafkaAdminClient(
    bootstrap_servers=['localhost:9092'],
    client_id='admin_setup'
)


topic_list=[NewTopic(name="multi-part-orders",num_partitions=3,replication_factor=1)]


try:
    kafkaclient.create_topics(new_topics=topic_list,validate_only=False)
    print(f"Mutliorder topic is created successfully with 3 partitions")
except Exception as e:
    print(f"Topic might already exists or error occured:{e}")

finally:
    kafkaclient.close()