import json
import time

import pandas as pd
import plotly.express as px
import streamlit as st
from kafka import KafkaConsumer

KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "taxi-anomalies"
ZONE_LOOKUP_FILE = "data/taxi_zone_lookup.csv"

ANOMALY_TYPES = [
    "possible_congestion",
    "unusually_slow_area",
    "high_trip_volume",
    "long_duration_cluster",
    "high_variability"
]

ANOMALY_LABELS = {
    "possible_congestion": "Possible congestion",
    "unusually_slow_area": "Unusually slow area",
    "high_trip_volume": "High trip volume",
    "long_duration_cluster": "Long duration cluster",
    "high_variability": "High variability"
}

SEVERITY_COLORS = {
    "normal": "#22C55E",
    "low": "#38BDF8",
    "medium": "#F59E0B",
    "high": "#EF4444"
}

ANOMALY_COLORS = {
    "possible_congestion": "#F97316",
    "unusually_slow_area": "#EF4444",
    "high_trip_volume": "#8B5CF6",
    "long_duration_cluster": "#06B6D4",
    "high_variability": "#EAB308"
}

SERVICE_COLORS = {
    "yellow": "#FACC15",
    "green": "#22C55E",
    "unknown": "#94A3B8"
}

WEATHER_COLORS = {
    "No precipitation": "#38BDF8",
    "Precipitation": "#F59E0B",
    "Normal visibility": "#22C55E",
    "Low visibility": "#EF4444",
    "Not windy": "#94A3B8",
    "Windy": "#8B5CF6",
    "Not cold": "#38BDF8",
    "Cold": "#06B6D4"
}

st.set_page_config(
    page_title="Real-Time Traffic Anomaly Detection",
    layout="wide"
)

st.markdown(
    """
    <style>
    html, body, [class*="css"] {
        font-family: "Inter", "Segoe UI", sans-serif;
    }

    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }

    .section-title {
        font-size: 1.2rem;
        font-weight: 700;
        margin-top: 1.6rem;
        margin-bottom: 0.8rem;
        color: #F8FAFC;
    }

    .small-muted {
        color: #94A3B8;
        font-size: 0.92rem;
        margin-bottom: 1.2rem;
    }

    .legend-card {
        background: linear-gradient(
            180deg,
            rgba(17,24,39,0.95) 0%,
            rgba(15,23,42,0.95) 100%
        );
        border: 1px solid #1E293B;
        border-radius: 16px;
        padding: 18px 20px;
        min-height: 170px;
        transition: 0.2s ease;
    }

    .legend-card:hover {
        border-color: #334155;
        transform: translateY(-2px);
    }

    .legend-card h4 {
        margin-top: 0;
        margin-bottom: 0.9rem;
        font-size: 1rem;
        font-weight: 700;
        color: #F8FAFC;
    }

    .legend-card p {
        margin: 0.35rem 0;
        color: #CBD5E1;
        font-size: 0.87rem;
        line-height: 1.45;
    }

    .legend-wrapper {
        padding-right: 4px;
    }

    .dot {
        height: 9px;
        width: 9px;
        border-radius: 50%;
        display: inline-block;
        margin-right: 8px;
    }

    div[data-testid="stMetric"] {
        background: linear-gradient(
            180deg,
            rgba(17,24,39,0.96) 0%,
            rgba(15,23,42,0.96) 100%
        );
        border: 1px solid #1E293B;
        padding: 16px;
        border-radius: 16px;
    }

    div[data-testid="stMetricLabel"] {
        color: #CBD5E1;
        font-size: 0.9rem;
    }

    div[data-testid="stMetricValue"] {
        font-size: 1.8rem;
        font-weight: 700;
    }

    div[data-testid="stDataFrame"] {
        border-radius: 14px;
        overflow: hidden;
    }
    </style>
    """,
    unsafe_allow_html=True
)

st.title("Real-Time Traffic Anomaly Detection")
st.markdown(
    "<div class='small-muted'>Window-level monitoring over streamed NYC yellow and green taxi trips, enriched with hourly NOAA weather data.</div>",
    unsafe_allow_html=True
)


@st.cache_resource
def create_consumer():
    return KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_SERVER,
        value_deserializer=lambda message: json.loads(message.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="streamlit-dashboard-weather-v1"
    )


@st.cache_data
def load_zone_lookup():
    zones = pd.read_csv(ZONE_LOOKUP_FILE)

    zones = zones.rename(
        columns={
            "LocationID": "location_id",
            "Borough": "borough",
            "Zone": "zone",
            "service_zone": "service_zone"
        }
    )

    zones["location_id"] = zones["location_id"].astype(int)
    zones["zone_label"] = zones["borough"] + " - " + zones["zone"]

    return zones


def contains_anomaly_type(types, target_type):
    if isinstance(types, list):
        return target_type in types

    if isinstance(types, str):
        return target_type in types

    return False


def prepare_display_table(input_df):
    columns = [
        "event_time",
        "service_type",
        "location_id",
        "borough",
        "zone",
        "window_status",
        "severity",
        "anomaly_types",
        "trip_count",
        "avg_speed_kmh",
        "avg_duration_minutes",
        "slow_trip_count",
        "slow_trip_ratio",
        "speed_stddev",
        "temperature_c",
        "precipitation_mm",
        "wind_speed_kmh",
        "visibility_km",
        "weather_context_flags",
        "window_size_minutes"
    ]

    available_columns = [col for col in columns if col in input_df.columns]
    display_df = input_df[available_columns].copy()

    display_df = display_df.rename(
        columns={
            "event_time": "Window time",
            "service_type": "Service",
            "location_id": "Location ID",
            "borough": "Borough",
            "zone": "Zone",
            "window_status": "Status",
            "severity": "Severity",
            "anomaly_types": "Anomaly types",
            "trip_count": "Trips",
            "avg_speed_kmh": "Avg speed (km/h)",
            "avg_duration_minutes": "Avg duration (min)",
            "slow_trip_count": "Slow trips",
            "slow_trip_ratio": "Slow trip ratio",
            "speed_stddev": "Speed variability",
            "temperature_c": "Temp (°C)",
            "precipitation_mm": "Precipitation (mm)",
            "wind_speed_kmh": "Wind (km/h)",
            "visibility_km": "Visibility (km)",
            "weather_context_flags": "Weather flags",
            "window_size_minutes": "Window size (min)"
        }
    )

    return display_df


def highlight_anomalous_rows(row):
    if row.get("Status") == "anomalous":
        return ["background-color: rgba(245, 158, 11, 0.18);"] * len(row)

    return [""] * len(row)


consumer = create_consumer()
zone_lookup = load_zone_lookup()

if "anomalies" not in st.session_state:
    st.session_state.anomalies = []

messages = consumer.poll(timeout_ms=1000)

for _, records in messages.items():
    for message in records:
        st.session_state.anomalies.append(message.value)

df = pd.DataFrame(st.session_state.anomalies)

with st.expander("Dashboard legend", expanded=False):
    legend_col_1, legend_col_2, legend_col_3 = st.columns([1, 1, 1], gap="medium")

    with legend_col_1:
        st.markdown(
            """
            <div class="legend-wrapper">
                <div class="legend-card">
                    <h4>Window</h4>
                    <p>A window is a short event-time interval.</p>
                    <p>Trips are grouped by pickup zone and service type.</p>
                    <p>Hourly NOAA weather data is joined by window timestamp.</p>
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )

    with legend_col_2:
        st.markdown(
            """
            <div class="legend-wrapper">
                <div class="legend-card">
                    <h4>Weather context</h4>
                    <p><b>Precipitation</b>: rain or snow signal in the hour.</p>
                    <p><b>Low visibility</b>: reduced visibility conditions.</p>
                    <p><b>Windy</b>: stronger wind conditions.</p>
                    <p><b>Cold</b>: temperature below 0°C.</p>
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )

    with legend_col_3:
        st.markdown(
            """
            <div class="legend-wrapper">
                <div class="legend-card">
                    <h4>Anomaly types</h4>
                    <p><b>Congestion</b>: low speed and many slow trips.</p>
                    <p><b>Slow area</b>: unusually low average speed.</p>
                    <p><b>High volume</b>: many trips in one window.</p>
                    <p><b>Long duration</b>: increased trip duration.</p>
                    <p><b>High variability</b>: unstable speeds.</p>
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )

if df.empty:
    st.info("Waiting for processed window events. Start the producer to send new data.")

else:
    if "service_type" not in df.columns:
        df["service_type"] = "unknown"

    weather_columns_defaults = {
        "temperature_c": None,
        "precipitation_mm": 0,
        "wind_speed_kmh": None,
        "visibility_km": None,
        "is_precipitation": False,
        "is_low_visibility": False,
        "is_windy": False,
        "is_cold": False,
        "weather_context_flags": []
    }

    for column, default_value in weather_columns_defaults.items():
        if column not in df.columns:
            df[column] = default_value

    df["location_id"] = df["location_id"].astype(int)

    df = df.merge(
        zone_lookup,
        on="location_id",
        how="left"
    )

    if "window_status" not in df.columns:
        df["window_status"] = df["severity"].apply(
            lambda value: "normal" if value == "normal" else "anomalous"
        )

    df["zone_label"] = df["zone_label"].fillna("Unknown borough - Unknown zone")
    df["borough"] = df["borough"].fillna("Unknown borough")
    df["zone"] = df["zone"].fillna("Unknown zone")

    df["precipitation_label"] = df["is_precipitation"].apply(
        lambda value: "Precipitation" if value else "No precipitation"
    )
    df["visibility_label"] = df["is_low_visibility"].apply(
        lambda value: "Low visibility" if value else "Normal visibility"
    )
    df["wind_label"] = df["is_windy"].apply(
        lambda value: "Windy" if value else "Not windy"
    )
    df["cold_label"] = df["is_cold"].apply(
        lambda value: "Cold" if value else "Not cold"
    )

    with st.expander("Filters", expanded=True):
        filter_col_1, filter_col_2, filter_col_3, filter_col_4 = st.columns(4)

        with filter_col_1:
            selected_services = st.multiselect(
                "Service type",
                options=sorted(df["service_type"].dropna().unique().tolist()),
                default=sorted(df["service_type"].dropna().unique().tolist())
            )

            selected_boroughs = st.multiselect(
                "Borough",
                options=sorted(df["borough"].dropna().unique().tolist()),
                default=sorted(df["borough"].dropna().unique().tolist())
            )

        with filter_col_2:
            selected_statuses = st.multiselect(
                "Window status",
                options=sorted(df["window_status"].dropna().unique().tolist()),
                default=sorted(df["window_status"].dropna().unique().tolist())
            )

            selected_severities = st.multiselect(
                "Severity",
                options=sorted(df["severity"].dropna().unique().tolist()),
                default=sorted(df["severity"].dropna().unique().tolist())
            )

        with filter_col_3:
            selected_anomaly_types = st.multiselect(
                "Anomaly type",
                options=ANOMALY_TYPES,
                default=[]
            )

            selected_precipitation = st.multiselect(
                "Precipitation",
                options=sorted(df["precipitation_label"].dropna().unique().tolist()),
                default=sorted(df["precipitation_label"].dropna().unique().tolist())
            )

        with filter_col_4:
            selected_visibility = st.multiselect(
                "Visibility",
                options=sorted(df["visibility_label"].dropna().unique().tolist()),
                default=sorted(df["visibility_label"].dropna().unique().tolist())
            )

            max_recent_windows = st.slider(
                "Recent windows used in speed charts",
                min_value=100,
                max_value=1000,
                value=300,
                step=100
            )

    base_filtered_df = df[
        df["service_type"].isin(selected_services) &
        df["borough"].isin(selected_boroughs) &
        df["window_status"].isin(selected_statuses) &
        df["severity"].isin(selected_severities) &
        df["precipitation_label"].isin(selected_precipitation) &
        df["visibility_label"].isin(selected_visibility)
    ].copy()

    speed_chart_df = base_filtered_df.copy()
    weather_chart_df = base_filtered_df.copy()
    filtered_df = base_filtered_df.copy()

    if selected_anomaly_types:
        filtered_df = filtered_df[
            filtered_df["anomaly_types"].apply(
                lambda types: any(
                    contains_anomaly_type(types, anomaly_type)
                    for anomaly_type in selected_anomaly_types
                )
            )
        ]

    if filtered_df.empty:
        st.warning("No anomaly/table data matches the selected filters. Speed and weather charts may still use the broader filter context.")

    if speed_chart_df.empty:
        st.warning("No speed chart data matches the selected filters.")

    if not filtered_df.empty:
        anomaly_df = filtered_df[filtered_df["window_status"] == "anomalous"]
        normal_df = filtered_df[filtered_df["window_status"] == "normal"]

        if not anomaly_df.empty and "anomaly_types" in anomaly_df.columns:
            exploded_anomalies = anomaly_df.explode("anomaly_types")
        else:
            exploded_anomalies = pd.DataFrame()

        st.markdown("<div class='section-title'>Overview</div>", unsafe_allow_html=True)

        col1, col2, col3, col4 = st.columns(4)

        col1.metric("Processed windows", len(filtered_df))
        col2.metric("Anomalous windows", len(anomaly_df))
        col3.metric("Normal windows", len(normal_df))

        anomaly_rate = (len(anomaly_df) / len(filtered_df)) * 100 if len(filtered_df) > 0 else 0
        col4.metric("Anomaly rate", f"{anomaly_rate:.1f}%")

        st.markdown("<div class='section-title'>Service comparison</div>", unsafe_allow_html=True)

        service_col_1, service_col_2 = st.columns([1, 2])

        with service_col_1:
            with st.container(border=True):
                service_summary = (
                    filtered_df.groupby("service_type")
                    .agg(
                        windows=("service_type", "count"),
                        anomalous_windows=("window_status", lambda x: (x == "anomalous").sum()),
                        avg_speed_kmh=("avg_speed_kmh", "mean"),
                        avg_duration_minutes=("avg_duration_minutes", "mean"),
                        total_trips=("trip_count", "sum")
                    )
                    .reset_index()
                )

                service_summary["anomaly_rate"] = (
                    service_summary["anomalous_windows"] / service_summary["windows"] * 100
                )

                service_summary["avg_speed_kmh"] = service_summary["avg_speed_kmh"].round(2)
                service_summary["avg_duration_minutes"] = service_summary["avg_duration_minutes"].round(2)
                service_summary["anomaly_rate"] = service_summary["anomaly_rate"].round(1)

                service_summary = service_summary.rename(
                    columns={
                        "service_type": "Service",
                        "windows": "Windows",
                        "anomalous_windows": "Anomalous windows",
                        "avg_speed_kmh": "Avg speed (km/h)",
                        "avg_duration_minutes": "Avg duration (min)",
                        "total_trips": "Total trips",
                        "anomaly_rate": "Anomaly rate (%)"
                    }
                )

                st.subheader("Yellow vs green summary")
                st.dataframe(service_summary, width="stretch", hide_index=True)

        with service_col_2:
            with st.container(border=True):
                service_anomaly_counts = (
                    anomaly_df.groupby("service_type")
                    .size()
                    .reset_index(name="anomalous_windows")
                    if not anomaly_df.empty
                    else pd.DataFrame(columns=["service_type", "anomalous_windows"])
                )

                fig = px.bar(
                    service_anomaly_counts,
                    x="service_type",
                    y="anomalous_windows",
                    color="service_type",
                    color_discrete_map=SERVICE_COLORS,
                    title="Anomalous windows by service type"
                )

                fig.update_layout(
                    xaxis_title="Service type",
                    yaxis_title="Anomalous windows",
                    showlegend=False
                )

                st.plotly_chart(fig, width="stretch", key="chart_service_anomalies")

        st.markdown("<div class='section-title'>Anomaly type indicators</div>", unsafe_allow_html=True)

        indicator_cols = st.columns(len(ANOMALY_TYPES))

        for col, anomaly_type in zip(indicator_cols, ANOMALY_TYPES):
            if not exploded_anomalies.empty and "anomaly_types" in exploded_anomalies.columns:
                count = len(
                    exploded_anomalies[
                        exploded_anomalies["anomaly_types"] == anomaly_type
                    ]
                )
            else:
                count = 0

            col.metric(ANOMALY_LABELS[anomaly_type], count)

        st.markdown("<div class='section-title'>Recent stream output</div>", unsafe_allow_html=True)

        table_col_1, table_col_2 = st.columns(2)

        with table_col_1:
            with st.container(border=True):
                st.subheader("Latest processed windows")

                latest_windows_table = prepare_display_table(filtered_df.tail(15))

                st.dataframe(
                    latest_windows_table.style.apply(highlight_anomalous_rows, axis=1),
                    width="stretch",
                    hide_index=True
                )

        with table_col_2:
            with st.container(border=True):
                st.subheader("Latest anomaly events")

                if anomaly_df.empty:
                    st.info("No anomaly events detected yet.")
                else:
                    st.dataframe(
                        prepare_display_table(anomaly_df.tail(15)),
                        width="stretch",
                        hide_index=True
                    )

        st.markdown("<div class='section-title'>Anomaly overview</div>", unsafe_allow_html=True)

        chart_col_1, chart_col_2 = st.columns([2, 1])

        with chart_col_1:
            with st.container(border=True):
                if not exploded_anomalies.empty and "anomaly_types" in exploded_anomalies.columns:
                    anomaly_counts = exploded_anomalies["anomaly_types"].value_counts().reset_index()
                    anomaly_counts.columns = ["anomaly_type", "count"]
                    anomaly_counts["label"] = anomaly_counts["anomaly_type"].map(ANOMALY_LABELS)

                    fig = px.bar(
                        anomaly_counts,
                        x="label",
                        y="count",
                        color="anomaly_type",
                        color_discrete_map=ANOMALY_COLORS,
                        title="Detected anomaly types"
                    )

                    fig.update_layout(
                        xaxis_title="Anomaly type",
                        yaxis_title="Count",
                        showlegend=False
                    )

                    st.plotly_chart(
                        fig,
                        width="stretch",
                        key="chart_anomaly_types"
                    )
                else:
                    st.info("No anomaly type distribution available yet.")

        with chart_col_2:
            with st.container(border=True):
                if "severity" in anomaly_df.columns and not anomaly_df.empty:
                    severity_counts = anomaly_df["severity"].value_counts().reset_index()
                    severity_counts.columns = ["severity", "count"]

                    fig = px.pie(
                        severity_counts,
                        names="severity",
                        values="count",
                        color="severity",
                        color_discrete_map=SEVERITY_COLORS,
                        title="Severity split",
                        hole=0.45
                    )

                    st.plotly_chart(
                        fig,
                        width="stretch",
                        key="chart_anomaly_severity"
                    )
                else:
                    st.info("No severity distribution available yet.")

        congestion_df = (
            anomaly_df[
                anomaly_df["anomaly_types"].apply(
                    lambda types: contains_anomaly_type(types, "possible_congestion")
                )
            ]
            if not anomaly_df.empty and "anomaly_types" in anomaly_df.columns
            else pd.DataFrame()
        )

        st.markdown("<div class='section-title'>Location insights</div>", unsafe_allow_html=True)

        with st.container(border=True):
            st.subheader("Top 10 congested pickup locations")

            if not congestion_df.empty:
                top_congestion_locations = (
                    congestion_df
                    .groupby(["location_id", "borough", "zone", "service_type"])
                    .agg(
                        congestion_events=("location_id", "count"),
                        avg_speed_kmh=("avg_speed_kmh", "mean"),
                        avg_duration_minutes=("avg_duration_minutes", "mean"),
                        avg_slow_trip_ratio=("slow_trip_ratio", "mean"),
                        total_trips_observed=("trip_count", "sum")
                    )
                    .reset_index()
                    .sort_values("congestion_events", ascending=False)
                    .head(10)
                )

                top_congestion_locations["avg_speed_kmh"] = top_congestion_locations["avg_speed_kmh"].round(2)
                top_congestion_locations["avg_duration_minutes"] = top_congestion_locations["avg_duration_minutes"].round(2)
                top_congestion_locations["avg_slow_trip_ratio"] = top_congestion_locations["avg_slow_trip_ratio"].round(2)

                top_congestion_locations = top_congestion_locations.rename(
                    columns={
                        "location_id": "Location ID",
                        "borough": "Borough",
                        "zone": "Zone",
                        "service_type": "Service",
                        "congestion_events": "Congestion events",
                        "avg_speed_kmh": "Avg speed (km/h)",
                        "avg_duration_minutes": "Avg duration (min)",
                        "avg_slow_trip_ratio": "Avg slow trip ratio",
                        "total_trips_observed": "Total trips observed"
                    }
                )

                st.dataframe(
                    top_congestion_locations,
                    width="stretch",
                    hide_index=True
                )
            else:
                st.info("No congestion-specific locations detected yet.")

    if not weather_chart_df.empty:
        st.markdown("<div class='section-title'>Weather impact overview</div>", unsafe_allow_html=True)

        weather_col_1, weather_col_2 = st.columns(2)

        with weather_col_1:
            with st.container(border=True):
                weather_summary = (
                    weather_chart_df
                    .groupby("precipitation_label")
                    .agg(
                        windows=("precipitation_label", "count"),
                        anomalous_windows=("window_status", lambda x: (x == "anomalous").sum()),
                        avg_speed_kmh=("avg_speed_kmh", "mean"),
                        avg_duration_minutes=("avg_duration_minutes", "mean"),
                        avg_precipitation_mm=("precipitation_mm", "mean")
                    )
                    .reset_index()
                )

                weather_summary["anomaly_rate"] = (
                    weather_summary["anomalous_windows"] / weather_summary["windows"] * 100
                )

                weather_summary["avg_speed_kmh"] = weather_summary["avg_speed_kmh"].round(2)
                weather_summary["avg_duration_minutes"] = weather_summary["avg_duration_minutes"].round(2)
                weather_summary["avg_precipitation_mm"] = weather_summary["avg_precipitation_mm"].round(2)
                weather_summary["anomaly_rate"] = weather_summary["anomaly_rate"].round(1)

                weather_summary = weather_summary.rename(
                    columns={
                        "precipitation_label": "Weather context",
                        "windows": "Windows",
                        "anomalous_windows": "Anomalous windows",
                        "avg_speed_kmh": "Avg speed (km/h)",
                        "avg_duration_minutes": "Avg duration (min)",
                        "avg_precipitation_mm": "Avg precipitation (mm)",
                        "anomaly_rate": "Anomaly rate (%)"
                    }
                )

                st.subheader("Precipitation summary")
                st.dataframe(weather_summary, width="stretch", hide_index=True)

        with weather_col_2:
            with st.container(border=True):
                weather_speed_summary = (
                    weather_chart_df
                    .groupby(["precipitation_label", "service_type"])
                    .agg(
                        avg_speed_kmh=("avg_speed_kmh", "mean"),
                        windows=("service_type", "count")
                    )
                    .reset_index()
                )

                weather_speed_summary["avg_speed_kmh"] = weather_speed_summary["avg_speed_kmh"].round(2)

                fig = px.bar(
                    weather_speed_summary,
                    x="precipitation_label",
                    y="avg_speed_kmh",
                    color="service_type",
                    barmode="group",
                    color_discrete_map=SERVICE_COLORS,
                    title="Average speed by precipitation context"
                )

                fig.update_layout(
                    xaxis_title="Weather context",
                    yaxis_title="Average speed (km/h)",
                    legend_title_text="Service"
                )

                st.plotly_chart(
                    fig,
                    width="stretch",
                    key="chart_weather_speed"
                )

        with st.container(border=True):
            st.subheader("Anomaly rate by weather context")

            weather_rate = (
                weather_chart_df
                .groupby(["precipitation_label", "visibility_label"])
                .agg(
                    windows=("window_status", "count"),
                    anomalous_windows=("window_status", lambda x: (x == "anomalous").sum()),
                    avg_speed_kmh=("avg_speed_kmh", "mean")
                )
                .reset_index()
            )

            weather_rate["anomaly_rate"] = (
                weather_rate["anomalous_windows"] / weather_rate["windows"] * 100
            )

            weather_rate["avg_speed_kmh"] = weather_rate["avg_speed_kmh"].round(2)
            weather_rate["anomaly_rate"] = weather_rate["anomaly_rate"].round(1)

            fig = px.scatter(
                weather_rate,
                x="avg_speed_kmh",
                y="anomaly_rate",
                size="windows",
                color="precipitation_label",
                symbol="visibility_label",
                color_discrete_map=WEATHER_COLORS,
                hover_data=[
                    "precipitation_label",
                    "visibility_label",
                    "windows",
                    "anomalous_windows",
                    "avg_speed_kmh",
                    "anomaly_rate"
                ],
                title="Traffic anomaly rate under weather contexts"
            )

            fig.update_layout(
                xaxis_title="Average speed (km/h)",
                yaxis_title="Anomaly rate (%)",
                legend_title_text="Weather context"
            )

            st.plotly_chart(
                fig,
                width="stretch",
                key="chart_weather_anomaly_rate"
            )

    if not speed_chart_df.empty:
        st.markdown("<div class='section-title'>Speed overview</div>", unsafe_allow_html=True)

        with st.container(border=True):
            st.subheader("Average speed by borough and zone")

            speed_df = speed_chart_df.tail(max_recent_windows).copy()

            borough_summary = (
                speed_df
                .groupby(["borough", "service_type"])
                .agg(
                    avg_speed_kmh=("avg_speed_kmh", "mean"),
                    total_trips=("trip_count", "sum"),
                    windows=("borough", "count")
                )
                .reset_index()
            )

            borough_summary["avg_speed_kmh"] = borough_summary["avg_speed_kmh"].round(2)

            fig = px.scatter(
                borough_summary,
                x="borough",
                y="avg_speed_kmh",
                color="service_type",
                size="total_trips",
                color_discrete_map=SERVICE_COLORS,
                hover_data=[
                    "borough",
                    "service_type",
                    "avg_speed_kmh",
                    "total_trips",
                    "windows"
                ],
                title=None
            )

            fig.update_layout(
                xaxis_title="Borough",
                yaxis_title="Average speed (km/h)",
                legend_title_text="Service"
            )

            st.plotly_chart(
                fig,
                width="stretch",
                key="chart_avg_speed_by_borough_service"
            )

            available_boroughs = sorted(speed_df["borough"].dropna().unique().tolist())

            selected_borough = st.selectbox(
                "Drill down to pickup zones",
                options=available_boroughs,
                index=0,
                key="borough_drilldown_select"
            )

            zone_speed_df = speed_df[speed_df["borough"] == selected_borough].copy()

            zone_summary = (
                zone_speed_df
                .groupby(["zone_label", "service_type"])
                .agg(
                    avg_speed_kmh=("avg_speed_kmh", "mean"),
                    total_trips=("trip_count", "sum"),
                    windows=("zone_label", "count")
                )
                .reset_index()
                .sort_values("avg_speed_kmh", ascending=True)
                .head(25)
            )

            zone_summary["avg_speed_kmh"] = zone_summary["avg_speed_kmh"].round(2)

            fig_zone = px.scatter(
                zone_summary,
                x="zone_label",
                y="avg_speed_kmh",
                color="service_type",
                size="total_trips",
                color_discrete_map=SERVICE_COLORS,
                hover_data=[
                    "zone_label",
                    "service_type",
                    "avg_speed_kmh",
                    "total_trips",
                    "windows"
                ],
                title=f"Pickup zones in {selected_borough}"
            )

            fig_zone.update_layout(
                xaxis_title="Pickup zone",
                yaxis_title="Average speed (km/h)",
                xaxis_tickangle=-45,
                legend_title_text="Service"
            )

            st.plotly_chart(
                fig_zone,
                width="stretch",
                key="chart_avg_speed_zone_drilldown_service"
            )

time.sleep(1)
st.rerun()
