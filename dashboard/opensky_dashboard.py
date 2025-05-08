#!/usr/bin/env python3
"""
opensky_dashboard.py

Interactive Streamlit dashboard for visualizing flight data,
including real-time aircraft positions, metrics, and spatial aggregates.
"""
import os
import json
import time
import datetime
import pandas as pd
import numpy as np
import streamlit as st
import pydeck as pdk
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient
from influxdb_client.client.query_api import QueryApi
import matplotlib.pyplot as plt

# Try to load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("Loaded environment variables from .env file")
except ImportError:
    print("python-dotenv not installed. Using environment variables from the system.")

# Configure page
st.set_page_config(
    page_title="Airspace Congestion Monitor",
    page_icon="✈️", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# Set Mapbox token for map rendering
mapbox_token = os.environ.get("MAPBOX_TOKEN", "pk.eyJ1IjoiZGVtby1tYXBib3giLCJhIjoiY2xvMGJuMDZ3MHI3ZjJpbnMwNHJ2ZnM1bCJ9.NQiBw0-YjBCzv_pI8kGsLw")
pdk.settings.mapbox_key = mapbox_token

# Print debug info
print(f"Mapbox token: {mapbox_token}")

# Global variables
UPDATE_INTERVAL = 3  # seconds
MAX_FLIGHTS = 5000  # maximum flights to display

# Helper functions
@st.cache_resource
def get_influx_client():
    """Connect to InfluxDB and return the client"""
    influx_url = os.environ.get('INFLUXDB_URL', 'http://localhost:8086')
    influx_token = os.environ.get('INFLUXDB_TOKEN', '')
    influx_org = os.environ.get('INFLUXDB_ORG', 'airspace')
    
    try:
        client = InfluxDBClient(
            url=influx_url,
            token=influx_token,
            org=influx_org
        )
        return client
    except Exception as e:
        st.error(f"Failed to connect to InfluxDB: {e}")
        return None

@st.cache_resource
def get_kafka_consumer(topic="flight-stream"):
    """Connect to Kafka and return the consumer"""
    bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP', 'localhost:9092')
    print(f"Connecting to Kafka at {bootstrap_servers} for topic {topic}")
    
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',  # Changed from 'latest' to not miss messages
            enable_auto_commit=True,
            group_id=f'dashboard-consumer-{topic}',  # Unique group ID per topic
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=3000,  # Increased timeout to 3s
            session_timeout_ms=10000,  # Added session timeout
            max_poll_interval_ms=300000  # Added max poll interval
        )
        print(f"Successfully connected to Kafka topic {topic}")
        return consumer
    except Exception as e:
        st.error(f"Failed to connect to Kafka: {e}")
        print(f"Kafka connection error: {str(e)}")
        return None

def query_flight_metrics(query_api, bucket="flight_metrics", time_range="5m"):
    """Query InfluxDB for flight metrics"""
    query = f'''
    from(bucket: "{bucket}")
      |> range(start: -{time_range})
      |> filter(fn: (r) => r["_measurement"] == "flight_metrics")
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''
    
    try:
        result = query_api.query_data_frame(query)
        if isinstance(result, list):
            if not result:
                return pd.DataFrame()
            # Concatenate multiple dataframes if returned as a list
            return pd.concat(result)
        return result
    except Exception as e:
        st.error(f"Error querying flight metrics: {e}")
        return pd.DataFrame()

def query_grid_metrics(query_api, bucket="flight_metrics", time_range="5m"):
    """Query InfluxDB for grid cell metrics"""
    query = f'''
    from(bucket: "{bucket}")
      |> range(start: -{time_range})
      |> filter(fn: (r) => r["_measurement"] == "grid_metrics")
      |> pivot(rowKey:["_time", "lat_bin", "lon_bin"], columnKey: ["_field"], valueColumn: "_value")
    '''
    
    try:
        result = query_api.query_data_frame(query)
        if isinstance(result, list):
            if not result:
                return pd.DataFrame()
            return pd.concat(result)
        return result
    except Exception as e:
        st.error(f"Error querying grid metrics: {e}")
        return pd.DataFrame()

def fetch_recent_flights(consumer, max_messages=1000, timeout_sec=5):  # Increased limits
    """Fetch recent flight data from Kafka stream"""
    messages = []
    start_time = time.time()
    
    try:
        print(f"Fetching messages from Kafka (max: {max_messages}, timeout: {timeout_sec}s)")
        while len(messages) < max_messages and (time.time() - start_time) < timeout_sec:
            msg_batch = consumer.poll(timeout_ms=1000, max_records=max_messages)
            if not msg_batch:
                print("No messages in this batch")
                continue
            
            print(f"Received batch of {sum(len(records) for records in msg_batch.values())} messages")
            for _, records in msg_batch.items():
                for record in records:
                    if isinstance(record.value, dict):
                        messages.append(record.value)
        
        print(f"Total messages fetched: {len(messages)}")
        return messages
    except Exception as e:
        st.error(f"Error fetching flight data: {e}")
        print(f"Error fetching messages: {str(e)}")
        return []

def process_flight_data(messages):
    """Convert Kafka messages to a DataFrame"""
    if not messages:
        return pd.DataFrame()
    
    # Convert to DataFrame
    df = pd.DataFrame(messages)
    
    # Clean up data
    for col in ['longitude', 'latitude', 'baro_altitude', 'velocity', 'vertical_rate']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Filter out invalid coordinates
    df = df.dropna(subset=['longitude', 'latitude'])
    
    return df

def create_flight_map(df, risk_column=None):
    """Create a PyDeck map visualization of flights"""
    if df.empty:
        return None
    
    try:
        # Try PyDeck first
        # Default column for coloring if risk score not available
        if risk_column in df.columns:
            color_by = risk_column
        elif 'velocity' in df.columns:
            color_by = 'velocity'
        else:
            df['default_color'] = 0.5
            color_by = 'default_color'
        
        # Normalize values for coloring
        min_val = df[color_by].min()
        max_val = df[color_by].max()
        range_val = max(1.0, max_val - min_val)
        
        def get_color(val):
            if pd.isna(val):
                return [100, 100, 100]
            normalized = (val - min_val) / range_val
            if color_by == 'risk_score':
                r = int(255 * normalized)
                g = int(255 * (1 - normalized))
                return [r, g, 0]
            elif color_by == 'velocity':
                r = int(255 * normalized)
                b = int(255 * (1 - normalized))
                return [r, 0, b]
            else:
                return [0, 0, 255]
        
        df['color'] = df[color_by].apply(get_color)
        
        flight_layer = pdk.Layer(
            "ScatterplotLayer",
            data=df,
            get_position=["longitude", "latitude"],
            get_fill_color="color",
            get_radius=3000,
            pickable=True,
            opacity=0.8,
            stroked=True,
            filled=True,
            radius_scale=6,
            radius_min_pixels=5,
            radius_max_pixels=100,
            line_width_min_pixels=1
        )

        view_state = pdk.ViewState(
            latitude=df["latitude"].mean(),
            longitude=df["longitude"].mean(),
            zoom=4,
            pitch=0
        )

        deck = pdk.Deck(
            map_style="light",
            initial_view_state=view_state,
            layers=[flight_layer],
            tooltip={
                "html": """
                <div style="font-family: Arial; padding: 10px; background-color: rgba(0, 0, 0, 0.7); color: white; border-radius: 5px;">
                    <b>ICAO24:</b> {icao24}<br/>
                    """ + 
                    ("""<b>Flight:</b> {callsign}<br/>""" if 'callsign' in df.columns else "") +
                    ("""<b>Country:</b> {origin_country}<br/>""" if 'origin_country' in df.columns else "") +
                    ("""<b>Altitude:</b> {baro_altitude} m<br/>""" if 'baro_altitude' in df.columns else "") +
                    ("""<b>Speed:</b> {velocity} m/s<br/>""" if 'velocity' in df.columns else "") +
                    ("""<b>Climb Rate:</b> {vertical_rate} m/s<br/>""" if 'vertical_rate' in df.columns else "") +
                    ("""<b>Risk Score:</b> {risk_score}""" if 'risk_score' in df.columns else "") +
                """
                </div>
                """,
                "style": {"color": "white"}
            }
        )
        
        try:
            st.pydeck_chart(deck)
            return True
        except Exception as e:
            st.warning("PyDeck map failed to render, falling back to basic map")
            return False
            
    except Exception as e:
        st.error(f"Error creating flight map: {e}")
        return None
    
def display_basic_map(df):
    """Display a basic map using Streamlit's built-in map function"""
    if not df.empty and 'latitude' in df.columns and 'longitude' in df.columns:
        st.map(df[['latitude', 'longitude']], zoom=3)
        
        # Display flight information in a table below the map
        with st.expander("Flight Details", expanded=False):
            display_cols = [
                "icao24", "callsign", "origin_country",
                "longitude", "latitude", "baro_altitude", 
                "velocity", "vertical_rate"
            ]
            available_cols = [c for c in display_cols if c in df.columns]
            st.dataframe(df[available_cols])

def create_grid_heatmap(df):
    """Create a heatmap of flight density by grid cell"""
    if df.empty:
        print("Warning: Empty dataframe passed to create_grid_heatmap")
        return None
    
    print(f"Creating heatmap with {len(df)} grid cells")
    print("Columns available:", df.columns.tolist())
    
    # Convert lat/lon bins to coordinates
    if 'lat_bin' in df.columns and 'lon_bin' in df.columns:
        # Create center points from grid cells
        df['latitude'] = (df['lat_bin'] + 0.5) - 90
        df['longitude'] = (df['lon_bin'] + 0.5) - 180
        print("Converted grid cells to coordinates")
        print("Sample data:", df[['lat_bin', 'lon_bin', 'latitude', 'longitude', 'flight_count']].head())
    else:
        print("Error: Missing lat_bin or lon_bin columns")
        return None

    # Create a scatter plot layer instead of heatmap
    scatter_layer = pdk.Layer(
        "ScatterplotLayer",
        df,
        get_position=['longitude', 'latitude'],
        get_radius='flight_count * 5000',  # Scale the size based on flight count
        get_fill_color=[255, 140, 0, 140],  # Orange with some transparency
        pickable=True
    )
    
    # Create the deck
    deck = pdk.Deck(
        map_style='mapbox://styles/mapbox/dark-v10',
        initial_view_state=pdk.ViewState(
            latitude=20,
            longitude=0,
            zoom=1,
            pitch=0,
        ),
        layers=[scatter_layer],
        tooltip={
            "html": "<b>Flights:</b> {flight_count}<br/><b>Risk:</b> {avg_risk:.2f}"
        }
    )
    
    return deck

def display_metrics_charts(metrics_df):
    """Display charts for key flight metrics"""
    if metrics_df.empty:
        st.warning("No metrics data available")
        return
    
    # Filter out non-numeric columns
    numeric_df = metrics_df.select_dtypes(include=[np.number])
    
    # Group of key metrics to visualize
    key_metrics = ['avg_risk', 'acceleration', 'turn_rate', 'alt_stability_idx']
    available_metrics = [m for m in key_metrics if m in numeric_df.columns]
    
    if not available_metrics:
        st.warning("No numeric metrics available to chart")
        return
    
    # Layout with multiple charts
    cols = st.columns(min(len(available_metrics), 4))
    
    for i, metric in enumerate(available_metrics):
        with cols[i % len(cols)]:
            st.subheader(f"{metric.replace('_', ' ').title()}")
            st.line_chart(numeric_df[metric])

def main():
    """Main dashboard application"""
    # Sidebar for controls
    st.sidebar.title("✈️ Airspace Congestion Monitor")
    
    view_mode = st.sidebar.radio(
        "View Mode",
        ["Real-time Map", "Grid Heatmap", "Flight Metrics"]
    )
    
    # Data source selection
    data_source = st.sidebar.radio(
        "Data Source",
        ["Kafka Stream", "InfluxDB Historical"]
    )
    
    # Time range for historical data
    if data_source == "InfluxDB Historical":
        time_range = st.sidebar.select_slider(
            "Time Range",
            options=["5m", "15m", "30m", "1h", "3h", "6h", "12h", "24h"]
        )
    
    # Filter controls
    with st.sidebar.expander("Filters", expanded=False):
        min_altitude = st.slider("Min Altitude (m)", 0, 15000, 0)
        max_risk = st.slider("Max Risk Score", 0.0, 2.0, 2.0, 0.1)
    
    # Main content area
    st.title("Airspace Congestion Monitoring")
    
    if view_mode == "Real-time Map":
        st.header("Real-time Flight Positions")
        
        if data_source == "Kafka Stream":
            # Get Kafka consumer and fetch data
            consumer = get_kafka_consumer("flight-stream")
            if consumer:
                with st.spinner("Fetching flight data..."):
                    messages = fetch_recent_flights(consumer)
                    
                if messages:
                    df = process_flight_data(messages)
                    
                    if not df.empty:
                        st.success(f"Displaying {len(df)} flights")
                        
                        # Apply filters
                        if 'baro_altitude' in df.columns:
                            df = df[df['baro_altitude'] >= min_altitude]
                        
                        if 'risk_score' in df.columns:
                            df = df[df['risk_score'] <= max_risk]
                        
                        # Try PyDeck first, fall back to basic map if it fails
                        if not create_flight_map(df, risk_column='risk_score'):
                            display_basic_map(df)
                        
                        # Display stats
                        st.subheader("Flight Statistics")
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            st.metric("Total Flights", len(df))
                        
                        with col2:
                            if 'risk_score' in df.columns:
                                st.metric("Avg Risk Score", f"{df['risk_score'].mean():.2f}")
                            
                        with col3:
                            if 'velocity' in df.columns:
                                st.metric("Avg Speed (m/s)", f"{df['velocity'].mean():.1f}")
                        
                        # Data table with raw data
                        with st.expander("Raw Flight Data", expanded=False):
                            display_cols = [
                                "icao24", "callsign", "origin_country",
                                "longitude", "latitude", "baro_altitude", 
                                "velocity", "vertical_rate"
                            ]
                            available_cols = [c for c in display_cols if c in df.columns]
                            st.dataframe(df[available_cols])
                    else:
                        st.warning("No valid flight data available")
                else:
                    st.warning("No messages received from Kafka")
            else:
                st.error("Could not connect to Kafka")
        
        else:  # InfluxDB Historical
            influx_client = get_influx_client()
            if influx_client:
                query_api = influx_client.query_api()
                
                with st.spinner("Querying flight metrics..."):
                    metrics_df = query_flight_metrics(query_api, time_range=time_range)
                
                if not metrics_df.empty:
                    st.success(f"Displaying {len(metrics_df)} flight records")
                    
                    # Apply filters
                    if 'baro_altitude' in metrics_df.columns:
                        metrics_df = metrics_df[metrics_df['baro_altitude'] >= min_altitude]
                    
                    if 'avg_risk' in metrics_df.columns:
                        metrics_df = metrics_df[metrics_df['avg_risk'] <= max_risk]
                    
                    # Try PyDeck first, fall back to basic map if it fails
                    if not create_flight_map(metrics_df, risk_column='avg_risk'):
                        display_basic_map(metrics_df)
                    
                    # Display metrics
                    display_metrics_charts(metrics_df)
                else:
                    st.warning("No historical metrics available in the selected time range")
            else:
                st.error("Could not connect to InfluxDB")
    
    elif view_mode == "Grid Heatmap":
        st.header("Flight Density Heatmap")
        
        if data_source == "Kafka Stream":
            # For Kafka, we'd need to use the aggregates topic
            consumer = get_kafka_consumer("flight-aggregates")
            if consumer:
                with st.spinner("Fetching spatial aggregates..."):
                    messages = fetch_recent_flights(consumer, max_messages=1000)
                
                if messages:
                    df = pd.DataFrame(messages)
                    
                    if not df.empty:
                        st.success(f"Displaying {len(df)} grid cells")
                        
                        # Create heatmap
                        grid_map = create_grid_heatmap(df)
                        if grid_map:
                            try:
                                st.pydeck_chart(grid_map)
                            except Exception as e:
                                st.error(f"Error displaying heatmap: {e}")
                                st.warning("Falling back to basic heatmap display...")
                                
                                # Fallback to a simple heatmap using Matplotlib
                                if 'lat_bin' in df.columns and 'lon_bin' in df.columns and 'flight_count' in df.columns:
                                    fig, ax = plt.subplots(figsize=(10, 6))
                                    pivoted = df.pivot_table(
                                        index='lat_bin', 
                                        columns='lon_bin', 
                                        values='flight_count',
                                        aggfunc='max',
                                        fill_value=0
                                    )
                                    im = ax.imshow(pivoted, cmap='viridis', interpolation='none', aspect='auto')
                                    plt.colorbar(im, ax=ax, label='Flight Count')
                                    ax.set_xlabel('Longitude Bin')
                                    ax.set_ylabel('Latitude Bin')
                                    ax.set_title('Flight Density Heatmap')
                                    st.pyplot(fig)
                        
                        # Display stats
                        st.subheader("Grid Statistics")
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.metric("Active Grid Cells", len(df))
                        
                        with col2:
                            if 'avg_risk' in df.columns:
                                st.metric("Avg Cell Risk", f"{df['avg_risk'].mean():.2f}")
                        
                        # Data table
                        with st.expander("Raw Grid Data", expanded=False):
                            display_cols = ["lat_bin", "lon_bin", "flight_count", "avg_risk"]
                            available_cols = [c for c in display_cols if c in df.columns]
                            st.dataframe(df[available_cols])
                    else:
                        st.warning("No valid grid data available")
                else:
                    st.warning("No aggregates received from Kafka")
            else:
                st.error("Could not connect to Kafka")
        
        else:  # InfluxDB Historical
            influx_client = get_influx_client()
            if influx_client:
                query_api = influx_client.query_api()
                
                with st.spinner("Querying grid metrics..."):
                    grid_df = query_grid_metrics(query_api, time_range=time_range)
                
                if not grid_df.empty:
                    st.success(f"Displaying {len(grid_df)} grid cells")
                    
                    # Create heatmap
                    grid_map = create_grid_heatmap(grid_df)
                    if grid_map:
                        try:
                            st.pydeck_chart(grid_map)
                        except Exception as e:
                            st.error(f"Error displaying heatmap: {e}")
                            st.warning("Falling back to basic heatmap display...")
                            
                            # Fallback to a simple heatmap using Matplotlib
                            if 'lat_bin' in grid_df.columns and 'lon_bin' in grid_df.columns and 'flight_count' in grid_df.columns:
                                fig, ax = plt.subplots(figsize=(10, 6))
                                pivoted = grid_df.pivot_table(
                                    index='lat_bin', 
                                    columns='lon_bin', 
                                    values='flight_count',
                                    aggfunc='max',
                                    fill_value=0
                                )
                                im = ax.imshow(pivoted, cmap='viridis', interpolation='none', aspect='auto')
                                plt.colorbar(im, ax=ax, label='Flight Count')
                                ax.set_xlabel('Longitude Bin')
                                ax.set_ylabel('Latitude Bin')
                                ax.set_title('Flight Density Heatmap')
                                st.pyplot(fig)
                    
                    # Display aggregated data over time
                    st.subheader("Grid Cell Occupancy Over Time")
                    
                    # Group by time and get total counts
                    if "_time" in grid_df.columns and "flight_count" in grid_df.columns:
                        time_series = grid_df.groupby(pd.Grouper(key="_time", freq="1min")).agg({
                            "flight_count": "sum",
                            "avg_risk": "mean"
                        }).reset_index()
                        
                        st.line_chart(time_series.set_index("_time")["flight_count"])
                else:
                    st.warning("No historical grid data available in the selected time range")
            else:
                st.error("Could not connect to InfluxDB")
    
    elif view_mode == "Flight Metrics":
        st.header("Flight Metrics Analysis")
        
        influx_client = get_influx_client()
        if influx_client:
            query_api = influx_client.query_api()
            
            time_range_for_metrics = "1h" if data_source == "Kafka Stream" else time_range
            
            with st.spinner("Fetching flight metrics..."):
                metrics_df = query_flight_metrics(query_api, time_range=time_range_for_metrics)
            
            if not metrics_df.empty:
                st.success(f"Analyzing metrics for {metrics_df['icao24'].nunique()} unique flights")
                
                # Select a specific flight for detailed analysis
                flight_ids = sorted(metrics_df['icao24'].unique())
                selected_flight = st.selectbox("Select Flight for Analysis", flight_ids)
                
                # Filter for the selected flight
                flight_data = metrics_df[metrics_df['icao24'] == selected_flight]
                
                # Display detailed metrics
                if not flight_data.empty:
                    st.subheader(f"Detailed Metrics for Flight {selected_flight}")
                    
                    # Display key metrics in expandable sections
                    with st.expander("Risk Profile", expanded=True):
                        if 'avg_risk' in flight_data.columns:
                            st.line_chart(flight_data.set_index("_time")["avg_risk"])
                    
                    with st.expander("Movement Metrics", expanded=True):
                        movement_cols = [c for c in ['acceleration', 'turn_rate', 'velocity'] 
                                          if c in flight_data.columns]
                        
                        if movement_cols:
                            st.line_chart(flight_data.set_index("_time")[movement_cols])
                    
                    with st.expander("Altitude Profile", expanded=True):
                        altitude_cols = [c for c in ['baro_altitude', 'vertical_rate', 'alt_stability_idx'] 
                                          if c in flight_data.columns]
                        
                        if altitude_cols:
                            st.line_chart(flight_data.set_index("_time")[altitude_cols])
                    
                    # Raw data table
                    with st.expander("Raw Metrics Data", expanded=False):
                        st.dataframe(flight_data)
                else:
                    st.warning(f"No metrics data available for flight {selected_flight}")
            else:
                st.warning("No metrics data available for analysis")
        else:
            st.error("Could not connect to InfluxDB")
    
    # Auto-refresh logic
    if data_source == "Kafka Stream":
        time.sleep(UPDATE_INTERVAL)
        st.rerun()

if __name__ == "__main__":
    main()