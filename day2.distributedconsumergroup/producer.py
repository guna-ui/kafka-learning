import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(" Blasting 15 orders into Kafka...")

for i in range(1, 100):
    order_data = {"order_id": i, "amount": i * 10.5, "status": "PENDING"}
    
    # Send to our multi-partition topic
    producer.send('multi-part-orders', value=order_data)
    time.sleep(1)
    print(f" Dispatched order {i}")

producer.flush()
producer.close()
print("All orders sent successfully.")