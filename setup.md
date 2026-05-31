# Project Setup and Run Guide

## Prerequisites

Before running the project, make sure the following software is installed:

### Required Software

* Python 3.11 or newer
* Docker Desktop
* VS Code (recommended)

---

## Verify Installation

### Python

Open a terminal and run:

```powershell
python --version
```

Expected result:

```text
Python 3.x.x
```

---

### Docker

Open Docker Desktop and make sure it is running.

Then verify:

```powershell
docker --version
```

Expected result:

```text
Docker version ...
```

---

# Initial Project Setup

## 1. Extract the Project Archive

Extract the shared project folder anywhere on your computer.

Example:

```text
E:\Projects\traffic-anomaly-detection
```

---

## 2. Open the Project

Open the folder in VS Code.

---

## 3. Create a Virtual Environment

Open a terminal inside the project folder and run:

```powershell
python -m venv .venv
```

---

## 4. Activate the Virtual Environment

Windows PowerShell:

```powershell
.\.venv\Scripts\activate
```

You should now see:

```text
(.venv)
```

at the beginning of the terminal line.

---

## 5. Install Python Dependencies

Run:

```powershell
pip install -r requirements.txt
```

Wait for all dependencies to finish installing.

---

# Starting Kafka Infrastructure

## 1. Start Docker Desktop

Make sure Docker Desktop is fully started before continuing.

---

## 2. Start Kafka and Zookeeper Containers

Inside the project folder, run:

```powershell
docker compose up -d
```

Wait approximately 20–30 seconds.

---

## 3. Verify Running Containers

Run:

```powershell
docker ps
```

You should see containers for:

* Kafka
* Zookeeper

running successfully.

---

# Preparing the Dataset

## 1. Place the Required Files

Inside the `data/` folder, place (if not already present):

```text
yellow_tripdata_2023-01.parquet
green_tripdata_2023-01.parquet
taxi_zone_lookup.csv
```

---

## 2. Generate the Stream Dataset

Run:

```powershell
python prepare_taxi_data.py
```

This script:

* loads the taxi datasets
* cleans invalid trips
* balances Yellow and Green Taxi data
* generates the stream sample file

Expected output:

```text
Saved file to: data/taxi_stream_sample.csv
```

---

# Running the Project

The project requires 3 separate terminals.

---

# Terminal 1 — Taxi Trip Producer

Activate the virtual environment:

```powershell
.\.venv\Scripts\activate
```

Run:

```powershell
python taxi_trip_producer.py
```

This streams taxi trips into Kafka.

---

# Terminal 2 — Windowed Anomaly Detector

Activate the virtual environment:

```powershell
.\.venv\Scripts\activate
```

Run:

```powershell
python windowed_anomaly_detector.py
```

This component:

* consumes streamed trips
* creates event-time windows
* computes traffic metrics
* detects anomalies
* sends processed windows back to Kafka

---

# Terminal 3 — Dashboard

Activate the virtual environment:

```powershell
.\.venv\Scripts\activate
```

Run:

```powershell
streamlit run dashboard.py
```

The dashboard should open automatically in the browser.

Default address:

```text
http://localhost:8501
```

---

# Stopping the Project

## Stop Python Processes

In each terminal, press:

```text
CTRL + C
```

---

## Stop Kafka Containers

Run:

```powershell
docker compose down
```

---

# Common Issues

## Kafka Connection Error

Example:

```text
NoBrokersAvailable
```

Solution:

1. Verify Docker Desktop is running
2. Run:

```powershell
docker compose up -d
```

3. Wait 20–30 seconds

---

## Streamlit Does Not Refresh

Restart the dashboard:

```powershell
streamlit run dashboard.py
```

---

## Docker Containers Fail to Start

Check container logs:

```powershell
docker logs <container_name>
```

---

## Port Already In Use

Restart Docker Desktop or stop previous processes using:

* port 8501 (Streamlit)
* port 9092 (Kafka)

---

# Notes

* The dashboard updates automatically in real time.
* Yellow and Green Taxi trips are processed separately.
* Anomalies are detected at window level, not per individual trip.
* Filtering in the dashboard affects visual analytics live.
