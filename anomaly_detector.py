import json
from kafka import KafkaConsumer, KafkaProducer

INPUT_TOPIC = "taxi-trips"
OUTPUT_TOPIC = "taxi-anomalies"
KAFKA_SERVER = "localhost:9092"

consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    value_deserializer=lambda message: json.loads(message.decode("utf-8")),
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="taxi-anomaly-detector"
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value, default=str).encode("utf-8")
)

def detect_anomalies(trip):
    anomalies = []

    if trip["average_speed_kmh"] < 5 and trip["trip_duration_minutes"] > 10:
        anomalies.append("very_slow_trip")

    if trip["trip_duration_minutes"] > 90:
        anomalies.append("unusually_long_trip")

    if trip["trip_distance"] < 1 and trip["total_amount"] > 40:
        anomalies.append("suspicious_short_distance_high_fare")

    if trip["average_speed_kmh"] > 120:
        anomalies.append("impossible_speed")

    return anomalies

def compute_severity(anomalies):
    if "impossible_speed" in anomalies or "unusually_long_trip" in anomalies:
        return "high"
    if "very_slow_trip" in anomalies:
        return "medium"
    return "low"

print("Anomaly detector started...")

for message in consumer:
    trip = message.value
    anomalies = detect_anomalies(trip)

    if anomalies:
        anomaly_event = {
            "event_time": trip["pickup_datetime"],
            "location_id": trip["pickup_location_id"],
            "dropoff_location_id": trip["dropoff_location_id"],
            "trip_distance": trip["trip_distance"],
            "trip_duration_minutes": trip["trip_duration_minutes"],
            "average_speed_kmh": trip["average_speed_kmh"],
            "total_amount": trip["total_amount"],
            "anomaly_types": anomalies,
            "severity": compute_severity(anomalies)
        }

        producer.send(OUTPUT_TOPIC, value=anomaly_event)
        print("ANOMALY:", anomaly_event)
