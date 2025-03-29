import random
import json
from kafka import KafkaProducer
from datetime import datetime
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

EVENTS_TOPIC = "traffic_data"
CONGESTION_LEVELS = ["LOW", "MEDIUM", "HIGH"]
SENSORS = ['S101', 'S202', 'S303', 'S404', 'S505']
SECONDS_TO_WAIT = 1


def generate_traffic_event(sensor_id):
    return {
        "sensor_id": sensor_id,
        "timestamp": datetime.now().isoformat(),
        "vehicle_count": random.randint(1, 100),
        "average_speed": round(random.uniform(10, 130), 1),
        "congestion_level": random.choice(CONGESTION_LEVELS),
    }


def run_producer():
    while True:
        sensor_id = random.choice(SENSORS)
        data = generate_traffic_event(sensor_id)
        try:
            producer.send(EVENTS_TOPIC, value=data)
            print(f"Sent: {data}")
        except Exception as e:
            print(f"Failed to send: {data}")
            print(e)

        time.sleep(SECONDS_TO_WAIT)

if __name__ == "__main__":
    run_producer()
