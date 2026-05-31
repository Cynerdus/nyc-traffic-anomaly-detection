"""
Prepares NYC Yellow and Green Taxi trip data for the streaming pipeline

The script:
- loads raw Parquet taxi datasets;
- normalizes Yellow and Green Taxi schemas;
- filters relevant time intervals;
- computes derived traffic metrics;
- removes invalid trips;
- limits the number of rows per service type;
- exports a clean CSV file used by the Kafka producer.
"""

import pandas as pd


# input files downloaded from the NYC TLC Trip Record Data source
YELLOW_INPUT_FILE = "data/yellow_tripdata_2023-01.parquet"
GREEN_INPUT_FILE = "data/green_tripdata_2023-01.parquet"

# output file consumed later by taxi_trip_producer.py
OUTPUT_FILE = "data/taxi_stream_sample.csv"

# yellow Taxi has a much higher trip density than green, so a shorter interval should enough
YELLOW_START = "2023-01-01 00:00:00"
YELLOW_END = "2023-01-02 00:00:00"

# green Taxi has fewer records, so we select a longer interval
GREEN_START = "2023-01-01 00:00:00"
GREEN_END = "2023-01-15 00:00:00"

# maximum number of rows retained per service type
MAX_ROWS_PER_SERVICE = 100000


def prepare_yellow_taxi():
    """
    Loads and normalizes the Yellow Taxi dataset

    Yellow Taxi uses tpep_* timestamp columns, which are renamed to a
    common schema shared with Green Taxi data
    """
    df = pd.read_parquet(YELLOW_INPUT_FILE)

    df = df[
        [
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "PULocationID",
            "DOLocationID",
            "trip_distance",
            "fare_amount",
            "total_amount",
            "passenger_count"
        ]
    ].copy()

    df = df.rename(
        columns={
            "tpep_pickup_datetime": "pickup_datetime",
            "tpep_dropoff_datetime": "dropoff_datetime"
        }
    )

    df["service_type"] = "yellow"

    return df


def prepare_green_taxi():
    """
    Loads and normalizes the Green Taxi dataset

    Green Taxi uses lpep_* timestamp columns, which are renamed to the
    same generic column names used for Yellow Taxi data
    """
    df = pd.read_parquet(GREEN_INPUT_FILE)

    df = df[
        [
            "lpep_pickup_datetime",
            "lpep_dropoff_datetime",
            "PULocationID",
            "DOLocationID",
            "trip_distance",
            "fare_amount",
            "total_amount",
            "passenger_count"
        ]
    ].copy()

    df = df.rename(
        columns={
            "lpep_pickup_datetime": "pickup_datetime",
            "lpep_dropoff_datetime": "dropoff_datetime"
        }
    )

    df["service_type"] = "green"

    return df


# load and standardize both datasets
yellow_df = prepare_yellow_taxi()
green_df = prepare_green_taxi()

# combine both service types
df = pd.concat([yellow_df, green_df], ignore_index=True)

print("Initial combined rows:", len(df))
print("Columns:", df.columns.tolist())

df = df.dropna()

# convert timestamp columns to datetime objects for filtering and metric calculation
df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"])

# convert interval boundaries to datetime values
yellow_start = pd.to_datetime(YELLOW_START)
yellow_end = pd.to_datetime(YELLOW_END)

green_start = pd.to_datetime(GREEN_START)
green_end = pd.to_datetime(GREEN_END)

# filter Yellow Taxi trips to the selected interval
yellow_filtered = df[
    (df["service_type"] == "yellow") &
    (df["pickup_datetime"] >= yellow_start) &
    (df["pickup_datetime"] < yellow_end)
]

# filter Green Taxi trips to the selected interval
green_filtered = df[
    (df["service_type"] == "green") &
    (df["pickup_datetime"] >= green_start) &
    (df["pickup_datetime"] < green_end)
]

# recombine filtered data
df = pd.concat([yellow_filtered, green_filtered], ignore_index=True)

# compute trip duration in minutes
df["trip_duration_minutes"] = (
    df["dropoff_datetime"] - df["pickup_datetime"]
).dt.total_seconds() / 60

# compute average speed in km/h (cuz the original trip distance is expressed in miles)
df["average_speed_kmh"] = (
    df["trip_distance"] * 1.60934
) / (df["trip_duration_minutes"] / 60)


# remove unrealistic or invalid trips
df = df[
    (df["trip_duration_minutes"] > 1) &
    (df["trip_duration_minutes"] < 180) &
    (df["trip_distance"] > 0) &
    (df["fare_amount"] > 0) &
    (df["average_speed_kmh"] > 0) &
    (df["average_speed_kmh"] < 150)
]

# sort chronologically to preserve realistic event-time order
df = df.sort_values("pickup_datetime")

# limit each service type independently to keep the dataset manageable for Kafka and Streamlit
yellow_final = (
    df[df["service_type"] == "yellow"]
    .head(MAX_ROWS_PER_SERVICE)
)

green_final = (
    df[df["service_type"] == "green"]
    .head(MAX_ROWS_PER_SERVICE)
)

# combined stream sample
df = pd.concat([yellow_final, green_final], ignore_index=True)
df = df.sort_values("pickup_datetime")

# save the dataset
df.to_csv(OUTPUT_FILE, index=False)

print("Selected intervals:")
print(f"Yellow: {YELLOW_START} to {YELLOW_END}")
print(f"Green:  {GREEN_START} to {GREEN_END}")
print(f"Maximum rows per service type: {MAX_ROWS_PER_SERVICE}")
print("Cleaned rows:", len(df))
print(df["service_type"].value_counts())
print(f"Saved file to: {OUTPUT_FILE}")
