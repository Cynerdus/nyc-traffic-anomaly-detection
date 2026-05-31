import json
from collections import defaultdict
from datetime import datetime, timedelta
from statistics import stdev

import pandas as pd
from kafka import KafkaConsumer, KafkaProducer

INPUT_TOPIC = "taxi-trips"
OUTPUT_TOPIC = "taxi-anomalies"
KAFKA_SERVER = "localhost:9092"
WEATHER_FILE = "data/nyc_weather_hourly.csv"

WINDOW_SIZE_MINUTES = 1
MIN_TRIPS_IN_WINDOW = 1

LOW_SPEED_THRESHOLD = 20
VERY_LOW_SPEED_THRESHOLD = 12
SLOW_TRIP_SPEED_THRESHOLD = 25
SLOW_TRIP_RATIO_THRESHOLD = 0.25

HIGH_TRIP_VOLUME_THRESHOLD = 12
LONG_DURATION_THRESHOLD = 25
HIGH_VARIABILITY_THRESHOLD = 18


def load_weather_data():
    weather_df = pd.read_csv(WEATHER_FILE)
    weather_df["weather_hour"] = pd.to_datetime(weather_df["weather_hour"])

    weather_by_hour = {}

    for _, row in weather_df.iterrows():
        hour_key = row["weather_hour"].to_pydatetime().replace(tzinfo=None)

        weather_by_hour[hour_key] = {
            "weather_hour": row["weather_hour"].strftime("%Y-%m-%d %H:%M:%S"),
            "temperature_c": round(float(row["temperature_c"]), 2)
            if pd.notna(row["temperature_c"]) else None,
            "wind_speed_kmh": round(float(row["wind_speed_kmh"]), 2)
            if pd.notna(row["wind_speed_kmh"]) else None,
            "visibility_km": round(float(row["visibility_km"]), 2)
            if pd.notna(row["visibility_km"]) else None,
            "precipitation_mm": round(float(row["precipitation_mm"]), 2)
            if pd.notna(row["precipitation_mm"]) else 0,
            "is_precipitation": bool(row["is_precipitation"]),
            "is_low_visibility": bool(row["is_low_visibility"]),
            "is_windy": bool(row["is_windy"]),
            "is_cold": bool(row["is_cold"])
        }

    return weather_by_hour


def get_weather_for_window(window_start, weather_by_hour):
    weather_hour = window_start.replace(minute=0, second=0, microsecond=0)

    default_weather = {
        "weather_hour": weather_hour.strftime("%Y-%m-%d %H:%M:%S"),
        "temperature_c": None,
        "wind_speed_kmh": None,
        "visibility_km": None,
        "precipitation_mm": 0,
        "is_precipitation": False,
        "is_low_visibility": False,
        "is_windy": False,
        "is_cold": False
    }

    return weather_by_hour.get(weather_hour, default_weather)


consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    value_deserializer=lambda message: json.loads(message.decode("utf-8")),
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="windowed-taxi-anomaly-detector-weather-v1"
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value, default=str).encode("utf-8")
)

weather_by_hour = load_weather_data()
windows = defaultdict(list)


def parse_datetime(value):
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


def get_window_start(event_time):
    minute = (event_time.minute // WINDOW_SIZE_MINUTES) * WINDOW_SIZE_MINUTES
    return event_time.replace(minute=minute, second=0, microsecond=0)


def detect_window_anomalies(trips):
    trip_count = len(trips)

    speeds = [t["average_speed_kmh"] for t in trips]
    durations = [t["trip_duration_minutes"] for t in trips]

    avg_speed = sum(speeds) / trip_count
    avg_duration = sum(durations) / trip_count

    slow_trips = [
        t for t in trips
        if t["average_speed_kmh"] < SLOW_TRIP_SPEED_THRESHOLD
    ]

    slow_trip_ratio = len(slow_trips) / trip_count
    speed_stddev = stdev(speeds) if len(speeds) > 1 else 0

    anomaly_types = []

    if avg_speed < LOW_SPEED_THRESHOLD and slow_trip_ratio >= SLOW_TRIP_RATIO_THRESHOLD:
        anomaly_types.append("possible_congestion")

    if avg_speed < VERY_LOW_SPEED_THRESHOLD:
        anomaly_types.append("unusually_slow_area")

    if trip_count >= HIGH_TRIP_VOLUME_THRESHOLD:
        anomaly_types.append("high_trip_volume")

    if avg_duration >= LONG_DURATION_THRESHOLD:
        anomaly_types.append("long_duration_cluster")

    if speed_stddev >= HIGH_VARIABILITY_THRESHOLD:
        anomaly_types.append("high_variability")

    return {
        "anomaly_types": anomaly_types,
        "avg_speed": avg_speed,
        "avg_duration": avg_duration,
        "slow_trip_count": len(slow_trips),
        "slow_trip_ratio": slow_trip_ratio,
        "speed_stddev": speed_stddev
    }


def compute_severity(anomaly_types, avg_speed, slow_trip_ratio, trip_count, avg_duration, speed_stddev):
    if (
        "unusually_slow_area" in anomaly_types
        or avg_speed < 8
        or avg_duration > 40
        or len(anomaly_types) >= 3
    ):
        return "high"

    if (
        "possible_congestion" in anomaly_types
        or "high_trip_volume" in anomaly_types
        or "long_duration_cluster" in anomaly_types
        or speed_stddev >= HIGH_VARIABILITY_THRESHOLD
    ):
        return "medium"

    return "low"


def process_window(window_key, trips):
    window_start, location_id, service_type = window_key
    trip_count = len(trips)

    if trip_count < MIN_TRIPS_IN_WINDOW:
        return None

    result = detect_window_anomalies(trips)

    anomaly_types = result["anomaly_types"]
    avg_speed = result["avg_speed"]
    avg_duration = result["avg_duration"]
    slow_trip_count = result["slow_trip_count"]
    slow_trip_ratio = result["slow_trip_ratio"]
    speed_stddev = result["speed_stddev"]

    weather_data = get_weather_for_window(window_start, weather_by_hour)

    is_anomalous = len(anomaly_types) > 0

    severity = (
        compute_severity(
            anomaly_types,
            avg_speed,
            slow_trip_ratio,
            trip_count,
            avg_duration,
            speed_stddev
        )
        if is_anomalous
        else "normal"
    )

    weather_context_flags = []

    if weather_data["is_precipitation"]:
        weather_context_flags.append("precipitation")

    if weather_data["is_low_visibility"]:
        weather_context_flags.append("low_visibility")

    if weather_data["is_windy"]:
        weather_context_flags.append("windy")

    if weather_data["is_cold"]:
        weather_context_flags.append("cold")

    return {
        "event_time": window_start.strftime("%Y-%m-%d %H:%M:%S"),
        "location_id": location_id,
        "service_type": service_type,
        "window_status": "anomalous" if is_anomalous else "normal",
        "anomaly_types": anomaly_types,
        "severity": severity,
        "trip_count": trip_count,
        "avg_speed_kmh": round(avg_speed, 2),
        "avg_duration_minutes": round(avg_duration, 2),
        "slow_trip_count": slow_trip_count,
        "slow_trip_ratio": round(slow_trip_ratio, 2),
        "speed_stddev": round(speed_stddev, 2),
        "window_size_minutes": WINDOW_SIZE_MINUTES,
        "weather_hour": weather_data["weather_hour"],
        "temperature_c": weather_data["temperature_c"],
        "wind_speed_kmh": weather_data["wind_speed_kmh"],
        "visibility_km": weather_data["visibility_km"],
        "precipitation_mm": weather_data["precipitation_mm"],
        "is_precipitation": weather_data["is_precipitation"],
        "is_low_visibility": weather_data["is_low_visibility"],
        "is_windy": weather_data["is_windy"],
        "is_cold": weather_data["is_cold"],
        "weather_context_flags": weather_context_flags
    }


print("Windowed anomaly detector with service-type and weather enrichment started...")
print(f"Loaded weather hours: {len(weather_by_hour)}")

latest_event_time = None
processed_windows = set()

for message in consumer:
    trip = message.value

    event_time = parse_datetime(trip["pickup_datetime"])
    latest_event_time = event_time if latest_event_time is None else max(latest_event_time, event_time)

    window_start = get_window_start(event_time)
    location_id = trip["pickup_location_id"]
    service_type = trip.get("service_type", "unknown")

    window_key = (window_start, location_id, service_type)
    windows[window_key].append(trip)

    watermark = latest_event_time - timedelta(minutes=WINDOW_SIZE_MINUTES)

    for key in list(windows.keys()):
        key_window_start, _, _ = key
        window_end = key_window_start + timedelta(minutes=WINDOW_SIZE_MINUTES)

        if window_end <= watermark and key not in processed_windows:
            window_event = process_window(key, windows[key])

            if window_event:
                producer.send(OUTPUT_TOPIC, value=window_event)
                print("WINDOW EVENT:", window_event)

            processed_windows.add(key)
            del windows[key]
