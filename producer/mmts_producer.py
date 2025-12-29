import pandas as pd
import json
import time
from kafka import KafkaProducer

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Load GTFS data
BASE_PATH = "../data/mmts"
stops = pd.read_csv(f"{BASE_PATH}/stops.txt")

print("Streaming stops data to Kafka...")

# Send each stop as a message
for _, row in stops.iterrows():
    message = {
        "stop_id": row["stop_id"],
        "stop_name": row["stop_name"],
        "stop_lat": row["stop_lat"],
        "stop_lon": row["stop_lon"]
    }

    producer.send("mmts_gtfs", value=message)
    print("Sent:", message)

    time.sleep(1)  # simulate real-time delay

producer.flush()
producer.close()

print("Streaming completed.")
