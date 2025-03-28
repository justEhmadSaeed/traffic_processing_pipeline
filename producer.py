import time
import random
import json
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

TOPIC = "traffic_data"
SECONDS_TO_WAIT = 1
congestion_levels = ["LOW", "MEDIUM", "HIGH"]
while True:
    data = {
        "sensor_id": random.randint(1, 5),
        "timestamp": time.time(),
        "vehicle_count": random.randint(1, 100),
        "average_speed": round(random.uniform(10, 130), 1),
        "congestion_level": random.choice(congestion_levels),
    }
    producer.send(TOPIC, value=data)
    print(f"Sent: {data}")
    time.sleep(SECONDS_TO_WAIT)
