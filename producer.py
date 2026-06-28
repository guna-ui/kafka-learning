import json
import time
from kafka import KafkaProducer



#1.create a producer
producer=KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name='user_id'

print(f"Press Control+c to stop the producer")

try:
    for i in range(6):
    
        event={
            'user_id':f"user_id:{i}",
            'topic_name': topic_name,
            'action':'login',
            "timestamp":time.time()
        }
        #send data to topic
        producer.send(topic=topic_name,value=event)
        print(f"sent message{i}:{event}")

        time.sleep(1)
    
    producer.flush()

except Exception as e:
        print(f"error occurred:{e}")