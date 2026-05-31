# Architecture

## Project Overview

This project implements a real-time traffic anomaly detection pipeline using NYC Taxi Trip data and NOAA weather data.

The system simulates a streaming environment where taxi trips are continuously published, processed into time-based windows, enriched with weather information, analyzed for anomalies, and visualized through an interactive dashboard.

The objective is to identify traffic irregularities such as congestion, unusually slow traffic, high trip volume, long trip duration clusters, and high speed variability.

---

# High-Level Architecture

```text
NYC Taxi Datasets
        |
        v
prepare_taxi_data.py
        |
        v
taxi_stream_sample.csv
        |
        v
taxi_trip_producer.py
        |
        v
Kafka Topic: taxi-trips
        |
        v
windowed_anomaly_detector.py
        |
        v
Kafka Topic: taxi-anomalies
        |
        v
dashboard.py
        |
        v
Streamlit Dashboard
```

Weather data follows a separate preparation pipeline:

```text
NOAA Weather Data
        |
        v
prepare_weather_data.py
        |
        v
nyc_weather_hourly.csv
        |
        v
windowed_anomaly_detector.py
```

---

# Main Components

## 1. Taxi Data Preparation

### File

```text
prepare_taxi_data.py
```

### Responsibilities

* Load Yellow Taxi and Green Taxi datasets
* Normalize schema differences
* Filter selected time intervals
* Compute trip duration
* Compute average speed
* Remove invalid records
* Balance service types
* Export a clean streaming dataset

### Output

```text
data/taxi_stream_sample.csv
```

---

## 2. Weather Data Preparation

### File

```text
prepare_weather_data.py
```

### Responsibilities

* Download NOAA hourly weather observations
* Parse NOAA encoded values
* Extract:

  * temperature
  * wind speed
  * visibility
  * precipitation
* Aggregate observations by hour
* Create weather condition flags
* Export processed weather dataset

### Output

```text
data/nyc_weather_hourly.csv
```

---

## 3. Kafka Producer

### File

```text
taxi_trip_producer.py
```

### Responsibilities

* Read taxi_stream_sample.csv
* Convert each trip into a Kafka event
* Simulate real-time streaming
* Publish events to Kafka

### Output Topic

```text
taxi-trips
```

### Event Structure

Each event contains:

* pickup timestamp
* dropoff timestamp
* pickup location
* dropoff location
* trip distance
* trip duration
* average speed
* fare amount
* passenger count
* service type

---

## 4. Apache Kafka

Kafka acts as the communication layer between all processing components.

### Topics

#### taxi-trips

Contains raw taxi trip events.

#### taxi-anomalies

Contains processed traffic windows and anomaly detection results.

### Purpose

Kafka decouples producers and consumers and enables real-time event streaming.

---

## 5. Apache ZooKeeper

ZooKeeper is used internally by Kafka.

### Responsibilities

* Broker coordination
* Topic metadata management
* Consumer group coordination
* Cluster state management

The application code never communicates directly with ZooKeeper.

---

## 6. Windowed Anomaly Detector

### File

```text
windowed_anomaly_detector.py
```

### Responsibilities

* Consume taxi trips from Kafka
* Create event-time windows
* Compute aggregated traffic metrics
* Enrich windows with weather data
* Detect anomalies
* Assign severity levels
* Publish processed windows

### Output Topic

```text
taxi-anomalies
```

---

# Windowing Strategy

Trips are not analyzed individually.

Instead, trips are grouped using:

```text
window_start
pickup_location_id
service_type
```

Example:

```text
08:15
Location ID = 161
Service Type = yellow
```

All Yellow Taxi trips originating from location 161 between:

```text
08:15:00
08:15:59
```

belong to the same processing window.

---

# Window Metrics

For each valid window, the detector computes:

* trip_count
* avg_speed_kmh
* avg_duration_minutes
* slow_trip_count
* slow_trip_ratio
* speed_stddev

These metrics describe traffic behavior at zone level rather than individual trip level.

---

# Anomaly Types

The system currently detects five anomaly categories.

## Possible Congestion

Low average speed combined with many slow trips.

## Unusually Slow Area

Extremely low average speed within a window.

## High Trip Volume

Abnormally large number of trips in a short period.

## Long Duration Cluster

High average trip duration.

## High Variability

Large speed differences between trips within the same window.

---

# Severity Levels

Each window receives one of the following severity levels:

```text
normal
low
medium
high
```

Severity is determined using:

* anomaly count
* average speed
* average duration
* variability indicators

---

# Weather Enrichment

Each traffic window is matched with the corresponding weather hour.

Example:

```text
Window time: 2023-01-01 08:15

Weather hour:
2023-01-01 08:00
```

The following weather attributes are added:

* temperature_c
* wind_speed_kmh
* visibility_km
* precipitation_mm
* is_precipitation
* is_low_visibility
* is_windy
* is_cold

This allows anomaly analysis under different weather conditions.

---

# Dashboard

### File

```text
dashboard.py
```

### Responsibilities

* Consume processed windows
* Apply filters
* Visualize traffic statistics
* Visualize anomaly distributions
* Compare Yellow vs Green Taxi services
* Display weather context
* Present location-based insights

### Main Dashboard Sections

* Overview
* Service Comparison
* Anomaly Indicators
* Recent Stream Output
* Anomaly Overview
* Location Insights
* Speed Analysis
* Weather Analysis

---

# Data Flow Summary

1. Raw taxi data is cleaned and transformed.
2. Weather observations are downloaded and prepared.
3. The producer streams taxi trips to Kafka.
4. Kafka stores events in the taxi-trips topic.
5. The anomaly detector consumes trip events.
6. Trips are grouped into windows.
7. Traffic metrics are calculated.
8. Weather information is attached.
9. Anomalies and severity levels are assigned.
10. Processed windows are published to taxi-anomalies.
11. The dashboard consumes processed windows.
12. Results are visualized in real time.

---

# Current Limitations

* Dataset limited to selected January 2023 intervals
* Rule-based anomaly detection
* Hourly weather granularity
* In-memory dashboard storage
* No historical persistence
* No map visualization yet

---

# Future Improvements

* Extend dataset coverage to multiple months
* Add historical trend analysis
* Export processed results to CSV or database
* Implement geographic heatmaps
* Add weather correlation visualizations
* Explore machine learning anomaly detection
* Introduce adaptive thresholds
* Support larger streaming workloads

```
```
