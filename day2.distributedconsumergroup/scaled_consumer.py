import json
import os
from kafka import KafkaConsumer

# Get the process ID (PID) so we can tell our consumers apart in the terminal
pid = os.getpid()

consumer = KafkaConsumer(
    'multi-part-orders',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',  
    group_id='billing-service-group', 
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f" Consumer [PID: {pid}] is online and waiting for its assigned partitions...")

try:
    for message in consumer:
        print(f"[Consumer {pid}] processed order from Partition: {message.partition} | Offset: {message.offset} | Data: {message.value}")
except KeyboardInterrupt:
    print(f"\n Consumer {pid} shutting down.")
finally:
    consumer.close()