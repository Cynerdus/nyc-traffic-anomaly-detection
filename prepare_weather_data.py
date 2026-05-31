"""
Downloads and prepares hourly NOAA weather data for New York City

The script:
- downloads a raw NOAA Global Hourly CSV file for the selected station and year;
- extracts relevant weather fields;
- parses NOAA-specific encoded values;
- aggregates observations at hourly level;
- creates weather context flags used later by the anomaly detector;
- exports a clean CSV file consumed by windowed_anomaly_detector.py.
"""

import os
import requests
import pandas as pd


OUTPUT_FILE = "data/nyc_weather_hourly.csv"

# NOAA Global Hourly station for New York City / Central Park area
# note to self, the format is USAF + WBAN
NOAA_STATION_ID = "72505394728"
YEAR = "2023"

NOAA_URL = (
    f"https://www.ncei.noaa.gov/data/global-hourly/access/"
    f"{YEAR}/{NOAA_STATION_ID}.csv"
)


def parse_numeric_noaa_value(value):
    """
    Parses numeric values stored in NOAA encoded fields

    NOAA fields often look like '+0039,1' or '-0017,1',
    where the part before the comma is the actual numeric value
    Missing values are commonly encoded as 9999, 99999, etc.

    Returns the parsed numeric value, or None if the value is missing or invalid
    """
    if pd.isna(value):
        return None

    value = str(value)

    if value == "":
        return None

    raw_value = value.split(",")[0]

    try:
        numeric_value = float(raw_value)
    except ValueError:
        return None

    if abs(numeric_value) >= 9999:
        return None

    return numeric_value


def parse_temperature(value):
    """
    Parses NOAA temperature values
    """
    parsed = parse_numeric_noaa_value(value)

    if parsed is None:
        return None

    return parsed / 10


def parse_wind_speed(value):
    """
    Parses NOAA wind speed values
    """
    parsed = parse_numeric_noaa_value(value)

    if parsed is None:
        return None

    return parsed / 10 * 3.6


def parse_visibility(value):
    """
    Parses NOAA visibility values
    """
    parsed = parse_numeric_noaa_value(value)

    if parsed is None:
        return None

    return parsed / 1000


def extract_precipitation_mm(row):
    """
    Extracts total precipitation from NOAA AA* precipitation columns

    NOAA precipitation may be stored across columns such as AA1, AA2, etc.
    AA1 = "01,0000,9,1"

    In this format, the second component represents precipitation depth
    in tenths of millimeters

    Returns the total precipitation depth in millimeters for the row
    """
    precipitation_columns = [
        col for col in row.index
        if col.startswith("AA")
    ]

    total_precipitation = 0
    found_precipitation = False

    for col in precipitation_columns:
        value = row[col]

        if pd.isna(value):
            continue

        parts = str(value).split(",")

        if len(parts) < 2:
            continue

        try:
            depth = float(parts[1])
        except ValueError:
            continue

        if depth >= 9999:
            continue

        total_precipitation += depth / 10
        found_precipitation = True

    if not found_precipitation:
        return 0

    return total_precipitation


def main():
    """
    This function performs the complete weather preprocessing step used by
    the traffic anomaly detection pipeline

    The workflow is:
    1. Make sure that the local data directory exists.
    2. Download the raw NOAA Global Hourly CSV file for the configured station.
    3. Save the raw file locally for reproducibility/debugging.
    4. Load the raw file into a Pandas DataFrame.
    5. Extract and normalize only the weather fields needed by the project.
    6. Aggregate observations to hourly level.
    7. Create boolean weather context flags.
    8. Save the processed hourly weather dataset as CSV.
    """

    os.makedirs("data", exist_ok=True)

    print("Downloading NOAA hourly weather data...")
    print(NOAA_URL)

    # download the raw NOAA CSV file for the selected station/year
    response = requests.get(NOAA_URL, timeout=60)

    if response.status_code != 200:
        raise RuntimeError(
            f"Could not download NOAA data. "
            f"Status code: {response.status_code}"
        )

    raw_file = "data/noaa_nyc_raw_2023.csv"

    # save the original NOAA file locally
    with open(raw_file, "wb") as file:
        file.write(response.content)

    print(f"Raw NOAA file saved to: {raw_file}")

    df = pd.read_csv(raw_file, low_memory=False)

    print("Available columns:")
    print(df.columns.tolist())

    if "DATE" not in df.columns:
        raise ValueError("Expected DATE column was not found in NOAA file.")

    weather_df = pd.DataFrame()

    # parse the NOAA timestamp and convert invalid timestamps to NaT
    weather_df["weather_time"] = pd.to_datetime(df["DATE"], errors="coerce")

    # extract temperature if the TMP column is available
    if "TMP" in df.columns:
        weather_df["temperature_c"] = df["TMP"].apply(parse_temperature)
    else:
        weather_df["temperature_c"] = None

    # extract wind speed if the WND column is available
    if "WND" in df.columns:
        weather_df["wind_speed_kmh"] = df["WND"].apply(parse_wind_speed)
    else:
        weather_df["wind_speed_kmh"] = None

    # extract visibility if the VIS column is available
    if "VIS" in df.columns:
        weather_df["visibility_km"] = df["VIS"].apply(parse_visibility)
    else:
        weather_df["visibility_km"] = None

    # extract precipitation from all available AA* precipitation columns
    weather_df["precipitation_mm"] = df.apply(extract_precipitation_mm, axis=1)

    # remove rows without a valid timestamp because they can't be joined to taxi windows
    weather_df = weather_df.dropna(subset=["weather_time"])

    weather_df["weather_hour"] = weather_df["weather_time"].dt.floor("h")

    # multiple NOAA observations may exist within the same hour, alas we aggregate them into one hourly record:
    # - average for continuous measurements;
    # - sum for precipitation.
    weather_df = (
        weather_df
        .groupby("weather_hour")
        .agg(
            temperature_c=("temperature_c", "mean"),
            wind_speed_kmh=("wind_speed_kmh", "mean"),
            visibility_km=("visibility_km", "mean"),
            precipitation_mm=("precipitation_mm", "sum")
        )
        .reset_index()
    )

    # round numeric fields
    weather_df["temperature_c"] = weather_df["temperature_c"].round(2)
    weather_df["wind_speed_kmh"] = weather_df["wind_speed_kmh"].round(2)
    weather_df["visibility_km"] = weather_df["visibility_km"].round(2)
    weather_df["precipitation_mm"] = weather_df["precipitation_mm"].round(2)

    # simple context flags
    weather_df["is_precipitation"] = weather_df["precipitation_mm"] > 0
    weather_df["is_low_visibility"] = weather_df["visibility_km"] < 5
    weather_df["is_windy"] = weather_df["wind_speed_kmh"] > 25
    weather_df["is_cold"] = weather_df["temperature_c"] < 0

    weather_df.to_csv(OUTPUT_FILE, index=False)

    print(f"Processed weather data saved to: {OUTPUT_FILE}")
    print("Rows:", len(weather_df))
    print(weather_df.head())


if __name__ == "__main__":
    main()
