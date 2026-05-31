import json
import time

import pandas as pd
from kafka import KafkaProducer

INPUT_FILE = "data/taxi_stream_sample.csv"
KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "taxi-trips"

STREAM_DELAY_SECONDS = 0.001

print("Loading taxi trip dataset...")

df = pd.read_csv(INPUT_FILE)

df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"])

df = df.sort_values("pickup_datetime").reset_index(drop=True)

print(f"Loaded {len(df)} rows")
print(df["service_type"].value_counts())

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

print("Kafka producer started...")
print("Streaming taxi trips...\n")

for index, row in df.iterrows():

    message = {
        "pickup_datetime": row["pickup_datetime"].strftime("%Y-%m-%d %H:%M:%S"),
        "dropoff_datetime": row["dropoff_datetime"].strftime("%Y-%m-%d %H:%M:%S"),
        "pickup_location_id": int(row["PULocationID"]),
        "dropoff_location_id": int(row["DOLocationID"]),
        "trip_distance": float(row["trip_distance"]),
        "trip_duration_minutes": float(row["trip_duration_minutes"]),
        "average_speed_kmh": float(row["average_speed_kmh"]),
        "fare_amount": float(row["fare_amount"]),
        "total_amount": float(row["total_amount"]),
        "passenger_count": int(row["passenger_count"]),
        "service_type": row["service_type"]
    }

    producer.send(TOPIC_NAME, value=message)

    if index % 1000 == 0:
        print(
            f"Sent {index} trips | "
            f"time={message['pickup_datetime']} | "
            f"service={message['service_type']} | "
            f"pickup_zone={message['pickup_location_id']} | "
            f"speed={message['average_speed_kmh']:.2f} km/h"
        )

    time.sleep(STREAM_DELAY_SECONDS)

producer.flush()

print("\nFinished streaming taxi trips.")
