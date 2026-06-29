from typing import final
import json
from kafka import KafkaConsumer
import time

consumer=KafkaConsumer(
    'user_id', 
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"Consumer is listening for messages... Press Ctrl+C to exit")

try:
    for message in consumer:
        print(f"Received: Key={message.key}, Value={message.value}, Partition={message.partition}, Offset={message.offset}")
        time.sleep(1)

except KeyboardInterrupt:
   print(" Consumer stopped.")

finally:
    consumer.close()