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
from typing import Dict, List
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient
from influxdb_client.client.query_api import QueryApi
import matplotlib.pyplot as plt
import glob
import plotly.express as px
import plotly.graph_objects as go

# Add folium imports
import folium
from streamlit_folium import folium_static
from folium.plugins import HeatMap, MarkerCluster

# Import anomaly detection functions
from anomaly_detection import (
    analyze_flight_anomalies, 
    get_anomaly_summary, 
    get_anomaly_timestamps,
    classify_anomaly_severity
)

# Remove simulated data import
try:
    import sys
    import os.path
    # Add the current directory to the path to ensure imports work
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
except ImportError as ie:
    print(f"Warning: Could not import module: {str(ie)}")

# Try to load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("Loaded environment variables from .env file")
    # Debug output for InfluxDB credentials
    print(f"InfluxDB URL: {os.environ.get('INFLUXDB_URL', 'not set')}")
    print(f"InfluxDB token: {os.environ.get('INFLUXDB_TOKEN', 'not set')[:10]}... (truncated)")
    print(f"InfluxDB org: {os.environ.get('INFLUXDB_ORG', 'not set')}")
except ImportError:
    print("python-dotenv not installed. Using environment variables from the system.")

# Configure page
st.set_page_config(
    page_title="Airspace Congestion Monitor",
    page_icon="✈️", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# Print debug info
print(f"Starting dashboard...")

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
    
    print(f"Connecting to InfluxDB at {influx_url} with org {influx_org}")
    print(f"Token length: {len(influx_token)} characters")
    
    try:
        # Make sure we have a valid token
        if not influx_token or len(influx_token) < 10:
            print("InfluxDB token is missing or too short, checking for token from environment")
            # Try to get token from direct environment variable
            influx_token = os.environ.get('INFLUXDB_TOKEN', '')
            
        client = InfluxDBClient(
            url=influx_url,
            token=influx_token,
            org=influx_org
        )
        # Test the connection
        health = client.health()
        print(f"InfluxDB connection successful. Status: {health.status}")
        return client
    except Exception as e:
        st.error(f"Failed to connect to InfluxDB: {e}")
        print(f"InfluxDB connection error details: {str(e)}")
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
        print(f"InfluxDB query error: {str(e)}")
        
        # Try to get data directly from Kafka
        print("Attempting to fetch data directly from Kafka flight-metrics topic...")
        return get_flight_metrics_from_kafka()

def get_flight_metrics_from_kafka():
    """Get flight metrics directly from Kafka when InfluxDB is unavailable"""
    try:
        consumer = get_kafka_consumer("flight-metrics")
        if consumer:
            messages = fetch_recent_flights(consumer, max_messages=1000, timeout_sec=5)
            if messages:
                # Convert messages to DataFrame
                df = pd.DataFrame(messages)
                
                # Convert string timestamp to datetime if it exists
                if '_time' in df.columns:
                    try:
                        df['_time'] = pd.to_datetime(df['_time'])
                        # Set _time as index for compatibility with anomaly detection
                        df = df.set_index('_time')
                    except Exception as e:
                        print(f"Warning: Could not convert _time to datetime: {str(e)}")
                
                print(f"Successfully fetched {len(df)} records from Kafka flight-metrics topic")
                return df
            else:
                print("No messages received from Kafka flight-metrics topic")
        
        # If we couldn't get data from Kafka metrics, try the original flight stream
        print("Attempting to fetch data from flight-stream topic...")
        consumer = get_kafka_consumer("flight-stream")
        if consumer:
            messages = fetch_recent_flights(consumer, max_messages=1000, timeout_sec=5)
            if messages:
                df = process_flight_data(messages)
                
                # Create a datetime index if not present
                if not isinstance(df.index, pd.DatetimeIndex):
                    # Create a timestamp column if not present
                    if 'timestamp' in df.columns:
                        try:
                            df['timestamp'] = pd.to_datetime(df['timestamp'])
                            df = df.set_index('timestamp')
                        except Exception as e:
                            print(f"Warning: Could not convert timestamp to datetime: {str(e)}")
                    else:
                        # Create artificial timestamps
                        now = pd.Timestamp.now()
                        timestamps = [now - pd.Timedelta(seconds=i) for i in range(len(df)-1, -1, -1)]
                        df.index = pd.DatetimeIndex(timestamps)
                
                print(f"Successfully fetched {len(df)} records from Kafka flight-stream topic")
                return df
        
        # If all else fails, return empty DataFrame
        print("Could not fetch data from Kafka, returning empty DataFrame")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error fetching data from Kafka: {str(e)}")
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
        print(f"InfluxDB grid metrics query error: {str(e)}")
        
        # Try to get grid metrics directly from Kafka
        print("Attempting to fetch grid data directly from Kafka flight-aggregates topic...")
        return get_grid_metrics_from_kafka()

def get_grid_metrics_from_kafka():
    """Get grid metrics directly from Kafka when InfluxDB is unavailable"""
    try:
        consumer = get_kafka_consumer("flight-aggregates")
        if consumer:
            messages = fetch_recent_flights(consumer, max_messages=1000, timeout_sec=5)
            if messages:
                # Convert messages to DataFrame
                df = pd.DataFrame(messages)
                
                # Convert string timestamp to datetime if it exists
                if '_time' in df.columns:
                    try:
                        df['_time'] = pd.to_datetime(df['_time'])
                    except Exception as e:
                        print(f"Warning: Could not convert _time to datetime: {str(e)}")
                
                # Ensure lat_bin and lon_bin are present for grid visualization
                if 'lat' in df.columns and 'lon' in df.columns and 'lat_bin' not in df.columns:
                    try:
                        # Create grid bins from coordinates
                        df['lat_bin'] = (df['lat'] + 90).astype(int)
                        df['lon_bin'] = (df['lon'] + 180).astype(int)
                    except Exception as e:
                        print(f"Warning: Could not create grid bins: {str(e)}")
                
                print(f"Successfully fetched {len(df)} grid records from Kafka flight-aggregates topic")
                return df
        
        # If all else fails, return empty DataFrame
        print("Could not fetch grid data from Kafka, returning empty DataFrame")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error fetching grid data from Kafka: {str(e)}")
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

# Create a Folium-based map function
def create_folium_flight_map(df, risk_column=None):
    """Create a Folium map visualization of flights"""
    if df.empty:
        return None
    
    try:
        # Determine center of the map
        center_lat = df["latitude"].mean()
        center_lon = df["longitude"].mean()
        
        # Create the map
        m = folium.Map(location=[center_lat, center_lon], zoom_start=5)
        
        # Create marker cluster for better performance with many points
        marker_cluster = MarkerCluster().add_to(m)
        
        # Determine color field
        if risk_column in df.columns:
            color_by = risk_column
        elif 'velocity' in df.columns:
            color_by = 'velocity'
        else:
            df['default_color'] = 0.5
            color_by = 'default_color'
        
        # Add markers for each flight
        for _, row in df.iterrows():
            if pd.isna(row['latitude']) or pd.isna(row['longitude']):
                continue
                
            # Create popup text
            popup_text = f"ICAO24: {row.get('icao24', 'Unknown')}<br>"
            if 'callsign' in row and not pd.isna(row['callsign']):
                popup_text += f"Callsign: {row['callsign']}<br>"
            if 'origin_country' in row and not pd.isna(row['origin_country']):
                popup_text += f"Country: {row['origin_country']}<br>"
            if 'baro_altitude' in row and not pd.isna(row['baro_altitude']):
                popup_text += f"Altitude: {row['baro_altitude']} m<br>"
            if 'velocity' in row and not pd.isna(row['velocity']):
                popup_text += f"Speed: {row['velocity']} m/s<br>"
            if 'vertical_rate' in row and not pd.isna(row['vertical_rate']):
                popup_text += f"Vertical Rate: {row['vertical_rate']} m/s<br>"
            if risk_column in row and not pd.isna(row[risk_column]):
                popup_text += f"Risk: {row[risk_column]:.2f}"
                
            # Set color based on value
            if color_by in row and not pd.isna(row[color_by]):
                value = row[color_by]
                if color_by == 'risk_score' or color_by == 'avg_risk':
                    # Red for high risk
                    color = 'red' if value > 1.0 else ('orange' if value > 0.5 else 'green')
                else:
                    # Blue shades for speed
                    color = 'blue'
            else:
                color = 'blue'
                
            # Add marker to cluster
            folium.Marker(
                location=[row['latitude'], row['longitude']],
                popup=folium.Popup(popup_text, max_width=300),
                icon=folium.Icon(icon="plane", prefix="fa", color=color),
            ).add_to(marker_cluster)
        
        return m
    except Exception as e:
        st.error(f"Error creating Folium map: {e}")
        return None

def create_folium_grid_heatmap(df):
    """Create a Folium heatmap of flight density by grid cell"""
    if df.empty:
        print("Warning: Empty dataframe passed to create_folium_grid_heatmap")
        return None
    
    try:
        # Convert lat/lon bins to coordinates if needed
        if 'lat_bin' in df.columns and 'lon_bin' in df.columns and 'latitude' not in df.columns:
            # Create center points from grid cells
            df['latitude'] = (df['lat_bin'] + 0.5) - 90
            df['longitude'] = (df['lon_bin'] + 0.5) - 180
        
        # Filter out NaN values in coordinates
        df = df.dropna(subset=['latitude', 'longitude'])
        
        if df.empty:
            print("Warning: No valid coordinates after filtering NaNs")
            return None
        
        # Check for flight count column
        if 'flight_count' in df.columns:
            # Use flight count as weight
            heat_data = [[row.latitude, row.longitude, row.flight_count] for i, row in df.iterrows() 
                        if not pd.isna(row.latitude) and not pd.isna(row.longitude) and not pd.isna(row.flight_count)]
        else:
            # No weight, just use coordinates
            heat_data = [[row.latitude, row.longitude] for i, row in df.iterrows()
                        if not pd.isna(row.latitude) and not pd.isna(row.longitude)]
        
        if not heat_data:
            print("Warning: No valid data points for heatmap")
            return None
            
        # Create basic map centered on the data
        center_lat = df["latitude"].mean()
        center_lon = df["longitude"].mean()
        
        # Fallback to default center if NaN
        if pd.isna(center_lat) or pd.isna(center_lon):
            center_lat, center_lon = 0, 0
            
        m = folium.Map(location=[center_lat, center_lon], zoom_start=2)
        
        # Add the heatmap
        HeatMap(heat_data).add_to(m)
        
        return m
    except Exception as e:
        st.error(f"Error creating Folium heatmap: {e}")
        print(f"Folium heatmap error details: {str(e)}")
        return None

def create_matplotlib_heatmap(df):
    """Create a Matplotlib heatmap of flight density by grid cell"""
    if df.empty:
        st.warning("No data available for heatmap visualization")
        return
    
    # Check if we have the necessary columns
    if 'lat_bin' not in df.columns or 'lon_bin' not in df.columns:
        if 'latitude' in df.columns and 'longitude' in df.columns:
            # Create bins from coordinates
            df['lat_bin'] = (df['latitude'] + 90).astype(int)
            df['lon_bin'] = (df['longitude'] + 180).astype(int)
        else:
            st.error("Missing coordinate data for heatmap")
            return
    
    # Check if we have flight count
    if 'flight_count' not in df.columns:
        # Count occurrences in each grid cell
        flight_counts = df.groupby(['lat_bin', 'lon_bin']).size().reset_index(name='flight_count')
        df = flight_counts
    
    try:
        # Create pivot table for heatmap
        pivoted = df.pivot_table(
            index='lat_bin', 
            columns='lon_bin', 
            values='flight_count',
            aggfunc='max',
            fill_value=0
        )
        
        # Create the plot
        fig, ax = plt.subplots(figsize=(10, 6))
        im = ax.imshow(pivoted, cmap='viridis', interpolation='none', aspect='auto')
        plt.colorbar(im, ax=ax, label='Flight Count')
        
        # Add labels and title
        ax.set_xlabel('Longitude Bin')
        ax.set_ylabel('Latitude Bin')
        ax.set_title('Flight Density Heatmap')
        
        # Display in Streamlit
        st.pyplot(fig)
        
    except Exception as e:
        st.error(f"Error creating Matplotlib heatmap: {e}")
        print(f"Matplotlib heatmap error details: {str(e)}")
        
        # Last resort: display the raw data
        st.write("Raw Grid Data:")
        display_cols = ["lat_bin", "lon_bin", "flight_count"]
        available_cols = [c for c in display_cols if c in df.columns]
        st.dataframe(df[available_cols])

# Helper functions for historical analytics
def load_json_files(path_pattern):
    """Load JSON files matching the pattern and combine into a DataFrame"""
    files = glob.glob(path_pattern)
    if not files:
        return pd.DataFrame()
    
    dfs = []
    for file in files:
        try:
            with open(file, 'r') as f:
                data = json.load(f)
                dfs.append(pd.DataFrame([data]))
        except json.JSONDecodeError:
            # Handle multi-line JSON files
            with open(file, 'r') as f:
                lines = f.readlines()
                for line in lines:
                    try:
                        data = json.loads(line.strip())
                        dfs.append(pd.DataFrame([data]))
                    except:
                        pass
    
    if not dfs:
        return pd.DataFrame()
    
    return pd.concat(dfs, ignore_index=True)

def load_markdown_report(file_path):
    """Load markdown report file"""
    if not os.path.exists(file_path):
        return "No report available. Run the historical analytics first."
    
    with open(file_path, 'r') as f:
        return f.read()

def create_hourly_chart(hourly_df):
    """Create hourly pattern chart"""
    if hourly_df.empty:
        return None
    
    fig = go.Figure()
    
    # Add flight count bars
    fig.add_trace(go.Bar(
        x=hourly_df['hour_of_day'],
        y=hourly_df['flight_count'],
        name='Flight Count',
        marker_color='royalblue',
        opacity=0.7
    ))
    
    # Add risk score line on secondary y-axis
    fig.add_trace(go.Scatter(
        x=hourly_df['hour_of_day'],
        y=hourly_df['avg_risk'],
        name='Avg Risk Score',
        mode='lines+markers',
        marker=dict(size=8, color='firebrick'),
        line=dict(width=2),
        yaxis='y2'
    ))
    
    # Update layout
    fig.update_layout(
        title='Hourly Flight Patterns',
        xaxis=dict(
            title='Hour of Day',
            tickmode='linear',
            tick0=0,
            dtick=1
        ),
        yaxis=dict(
            title='Flight Count',
            titlefont=dict(color='royalblue'),
            tickfont=dict(color='royalblue')
        ),
        yaxis2=dict(
            title='Avg Risk Score',
            titlefont=dict(color='firebrick'),
            tickfont=dict(color='firebrick'),
            anchor='x',
            overlaying='y',
            side='right'
        ),
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=1.02,
            xanchor='right',
            x=1
        ),
        margin=dict(l=20, r=20, t=40, b=20),
        hovermode='x unified'
    )
    
    return fig

def create_daily_chart(daily_df):
    """Create daily pattern chart"""
    if daily_df.empty:
        return None
    
    # Map day numbers to names
    day_names = {
        1: 'Sunday',
        2: 'Monday',
        3: 'Tuesday',
        4: 'Wednesday',
        5: 'Thursday',
        6: 'Friday',
        7: 'Saturday'
    }
    
    daily_df['day_name'] = daily_df['day_of_week'].map(day_names)
    
    # Sort by day of week
    daily_df = daily_df.sort_values('day_of_week')
    
    fig = go.Figure()
    
    # Add flight count bars
    fig.add_trace(go.Bar(
        x=daily_df['day_name'],
        y=daily_df['flight_count'],
        name='Flight Count',
        marker_color='mediumseagreen',
        opacity=0.7
    ))
    
    # Add risk score line on secondary y-axis
    fig.add_trace(go.Scatter(
        x=daily_df['day_name'],
        y=daily_df['avg_risk'],
        name='Avg Risk Score',
        mode='lines+markers',
        marker=dict(size=8, color='darkorange'),
        line=dict(width=2),
        yaxis='y2'
    ))
    
    # Update layout
    fig.update_layout(
        title='Daily Flight Patterns',
        xaxis=dict(
            title='Day of Week',
            categoryorder='array',
            categoryarray=[day_names[i] for i in range(1, 8)]
        ),
        yaxis=dict(
            title='Flight Count',
            titlefont=dict(color='mediumseagreen'),
            tickfont=dict(color='mediumseagreen')
        ),
        yaxis2=dict(
            title='Avg Risk Score',
            titlefont=dict(color='darkorange'),
            tickfont=dict(color='darkorange'),
            anchor='x',
            overlaying='y',
            side='right'
        ),
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=1.02,
            xanchor='right',
            x=1
        ),
        margin=dict(l=20, r=20, t=40, b=20),
        hovermode='x unified'
    )
    
    return fig

def create_hotspot_map(hotspots_df):
    """Create a folium map with congestion hotspots"""
    if hotspots_df.empty:
        return None
    
    # Center map on the average of all hotspots
    center_lat = hotspots_df['center_latitude'].mean()
    center_lon = hotspots_df['center_longitude'].mean()
    
    # Create map
    m = folium.Map(location=[center_lat, center_lon], zoom_start=2)
    
    # Add hotspots as circles
    for _, row in hotspots_df.iterrows():
        # Scale radius based on flight count
        radius = np.sqrt(row['flight_count']) * 10000
        
        # Color based on risk (green to red)
        risk = row['avg_risk']
        color = 'green' if risk < 0.3 else 'orange' if risk < 0.7 else 'red'
        
        # Create popup text
        popup_text = f"""
            <b>Flight Count:</b> {row['flight_count']}<br>
            <b>Avg Risk:</b> {row['avg_risk']:.2f}<br>
            <b>Avg Altitude:</b> {row['avg_altitude']:.0f} m<br>
            <b>Coordinates:</b> {row['center_latitude']:.2f}, {row['center_longitude']:.2f}
        """
        
        # Add circle
        folium.Circle(
            location=[row['center_latitude'], row['center_longitude']],
            radius=radius,
            color=color,
            fill=True,
            fill_opacity=0.4,
            popup=folium.Popup(popup_text, max_width=300)
        ).add_to(m)
    
    return m

def create_anomaly_timeline(flight_data, anomaly_results, parameter):
    """
    Create a timeline chart showing parameter values and detected anomalies.
    
    Args:
        flight_data: DataFrame with flight parameters
        anomaly_results: Results from analyze_flight_anomalies
        parameter: Parameter to visualize (e.g., 'baro_altitude', 'velocity')
        
    Returns:
        Plotly figure object
    """
    if parameter not in flight_data.columns or parameter not in anomaly_results:
        return None
    
    # Get parameter data and anomalies
    param_data = flight_data[parameter]
    param_anomalies = anomaly_results[parameter]
    
    # Create figure
    fig = go.Figure()
    
    # Add parameter line
    fig.add_trace(go.Scatter(
        x=param_data.index,
        y=param_data,
        mode='lines',
        name=parameter.replace('_', ' ').title(),
        line=dict(color='royalblue', width=2)
    ))
    
    # Add anomaly points with different colors for each type
    colors = {
        'statistical_outliers': 'red',
        'sudden_changes': 'orange',
        'trend_deviations': 'purple',
        'vertical_inconsistencies': 'brown',
        'extreme_acceleration': 'darkred',
        'extreme_turn_rate': 'magenta'
    }
    
    for anomaly_type, anomaly_series in param_anomalies.items():
        if anomaly_type != 'any_anomaly' and anomaly_series.any():
            # Get points where anomalies occurred
            anomaly_points = param_data[anomaly_series]
            
            if not anomaly_points.empty:
                fig.add_trace(go.Scatter(
                    x=anomaly_points.index,
                    y=anomaly_points,
                    mode='markers',
                    name=anomaly_type.replace('_', ' ').title(),
                    marker=dict(
                        color=colors.get(anomaly_type, 'gray'),
                        size=10,
                        symbol='circle',
                        line=dict(width=1, color='black')
                    )
                ))
    
    # Update layout
    fig.update_layout(
        title=f"{parameter.replace('_', ' ').title()} Anomalies",
        xaxis=dict(title='Time'),
        yaxis=dict(title=parameter.replace('_', ' ').title()),
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=1.02,
            xanchor='right',
            x=1
        ),
        margin=dict(l=20, r=20, t=40, b=20),
        hovermode='closest'
    )
    
    return fig

def create_anomaly_severity_chart(flight_data, severity):
    """
    Create a chart showing anomaly severity over time.
    
    Args:
        flight_data: DataFrame with flight parameters
        severity: Series with severity levels from classify_anomaly_severity
        
    Returns:
        Plotly figure object
    """
    if severity.empty:
        return None
    
    # Create a color map for severity levels
    colors = {
        0: 'green',   # No anomaly
        1: 'yellow',  # Low severity
        2: 'orange',  # Medium severity
        3: 'red'      # High severity
    }
    
    # Create a continuous color scale
    color_scale = [
        [0, 'green'],
        [0.33, 'yellow'],
        [0.66, 'orange'],
        [1, 'red']
    ]
    
    # Create figure
    fig = go.Figure()
    
    # Add severity as a heatmap-like visualization
    fig.add_trace(go.Scatter(
        x=severity.index,
        y=[1] * len(severity),  # Constant y value for heatmap-like effect
        mode='markers',
        marker=dict(
            color=severity,
            colorscale=color_scale,
            cmin=0,
            cmax=3,
            size=15,
            showscale=True,
            colorbar=dict(
                title='Severity',
                tickvals=[0, 1, 2, 3],
                ticktext=['None', 'Low', 'Medium', 'High']
            )
        ),
        hovertemplate='Time: %{x}<br>Severity: %{marker.color}<extra></extra>'
    ))
    
    # Update layout
    fig.update_layout(
        title='Anomaly Severity Timeline',
        xaxis=dict(title='Time'),
        yaxis=dict(
            showticklabels=False,
            showgrid=False,
            zeroline=False
        ),
        height=200,
        margin=dict(l=20, r=20, t=40, b=20)
    )
    
    return fig

def create_anomaly_map(
    flight_data: pd.DataFrame,
    anomaly_results: Dict[str, Dict[str, pd.Series]]
) -> folium.Map:
    """
    Create a map highlighting flights with anomalies.
    
    Args:
        flight_data: DataFrame with flight parameters
        anomaly_results: Results from analyze_flight_anomalies
        
    Returns:
        folium.Map object
    """
    if flight_data.empty or 'overall' not in anomaly_results:
        return None
    
    # Get overall anomaly indicator
    any_anomaly = anomaly_results['overall']['any_anomaly']
    
    # Get severity if available
    severity = classify_anomaly_severity(anomaly_results)
    
    # Add severity to the flight data
    flight_data_with_anomalies = flight_data.copy()
    flight_data_with_anomalies['has_anomaly'] = any_anomaly
    flight_data_with_anomalies['anomaly_severity'] = severity
    
    # Create folium map
    center_lat = flight_data['latitude'].mean()
    center_lon = flight_data['longitude'].mean()
    m = folium.Map(location=[center_lat, center_lon], zoom_start=5)
    
    # Add markers for flights with anomalies
    for _, row in flight_data_with_anomalies[flight_data_with_anomalies['has_anomaly']].iterrows():
        # Determine color based on severity
        if 'anomaly_severity' in row and not pd.isna(row['anomaly_severity']):
            severity_level = int(row['anomaly_severity'])
            color = 'green' if severity_level == 1 else ('orange' if severity_level == 2 else 'red')
        else:
            color = 'red'  # Default to red if severity not available
        
        # Create popup text
        popup_text = f"<b>ICAO24:</b> {row.get('icao24', 'Unknown')}<br>"
        if 'callsign' in row and not pd.isna(row['callsign']):
            popup_text += f"<b>Callsign:</b> {row['callsign']}<br>"
        if 'baro_altitude' in row and not pd.isna(row['baro_altitude']):
            popup_text += f"<b>Altitude:</b> {row['baro_altitude']} m<br>"
        if 'velocity' in row and not pd.isna(row['velocity']):
            popup_text += f"<b>Speed:</b> {row['velocity']} m/s<br>"
        if 'anomaly_severity' in row and not pd.isna(row['anomaly_severity']):
            severity_text = ['None', 'Low', 'Medium', 'High'][int(row['anomaly_severity'])]
            popup_text += f"<b>Anomaly Severity:</b> {severity_text}<br>"
        
        # Add marker
        folium.Marker(
            location=[row['latitude'], row['longitude']],
            popup=folium.Popup(popup_text, max_width=300),
            icon=folium.Icon(icon="warning", prefix="fa", color=color),
        ).add_to(m)
    
    # Add normal flights as small blue markers
    normal_flights = folium.FeatureGroup(name="Normal Flights")
    for _, row in flight_data_with_anomalies[~flight_data_with_anomalies['has_anomaly']].iterrows():
        folium.CircleMarker(
            location=[row['latitude'], row['longitude']],
            radius=3,
            color='blue',
            fill=True,
            fill_opacity=0.6,
            popup=f"ICAO24: {row.get('icao24', 'Unknown')}"
        ).add_to(normal_flights)
    
    normal_flights.add_to(m)
    
    # Add layer control
    folium.LayerControl().add_to(m)
    
    return m

def create_anomaly_summary_cards(anomaly_summary):
    """
    Create summary cards for anomaly detection results.
    
    Args:
        anomaly_summary: Summary dictionary from get_anomaly_summary
        
    Returns:
        None (directly renders to Streamlit)
    """
    # Create columns for metrics
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(
            "Total Anomalies", 
            anomaly_summary['total_anomalies'],
            delta=None
        )
    
    with col2:
        st.metric(
            "Anomaly Percentage", 
            f"{anomaly_summary['anomaly_percentage']:.1f}%",
            delta=None
        )
    
    with col3:
        st.metric(
            "Max Simultaneous Anomalies", 
            anomaly_summary['max_anomaly_count'],
            delta=None
        )
    
    # Add parameter breakdown
    if anomaly_summary['parameter_breakdown']:
        st.subheader("Anomaly Breakdown by Parameter")
        
        # Create columns for each parameter
        param_cols = st.columns(len(anomaly_summary['parameter_breakdown']))
        
        for i, (param, details) in enumerate(anomaly_summary['parameter_breakdown'].items()):
            with param_cols[i]:
                st.markdown(f"**{param.title()}**")
                st.caption(f"Total: {details['total']} ({details['percentage']:.1f}%)")
                
                # Show breakdown by type
                for anomaly_type, type_details in details['types'].items():
                    if type_details['count'] > 0:
                        st.caption(f"- {anomaly_type.replace('_', ' ').title()}: {type_details['count']}")

def display_anomaly_details_table(flight_data, anomaly_events):
    """
    Display a table with detailed information about anomaly events.
    
    Args:
        flight_data: DataFrame with flight parameters
        anomaly_events: List of anomaly events from get_anomaly_timestamps
        
    Returns:
        None (directly renders to Streamlit)
    """
    if not anomaly_events:
        st.info("No anomalies detected")
        return
    
    # Create a DataFrame for the anomaly events
    events_data = []
    
    for event in anomaly_events:
        timestamp = event['timestamp']
        
        # Get flight data at this timestamp
        flight_row = flight_data.loc[timestamp] if timestamp in flight_data.index else {}
        
        # Create a row for the table
        event_row = {
            'Timestamp': timestamp,
            'ICAO24': flight_row.get('icao24', 'Unknown'),
            'Callsign': flight_row.get('callsign', 'Unknown'),
            'Altitude (m)': flight_row.get('baro_altitude', None),
            'Speed (m/s)': flight_row.get('velocity', None),
            'Anomaly Count': event['anomaly_count'],
            'Parameters': ', '.join(event['parameters'].keys())
        }
        
        events_data.append(event_row)
    
    # Create DataFrame and display
    events_df = pd.DataFrame(events_data)
    st.dataframe(events_df, use_container_width=True)

def run_historical_analytics():
    """Run historical analytics process"""
    import subprocess
    import sys
    
    try:
        # Get the current directory
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(current_dir)
        script_path = os.path.join(project_root, "scripts", "run_historical_analytics.sh")
        
        # Check if the script exists
        if not os.path.exists(script_path):
            st.error(f"Analytics script not found at: {script_path}")
            return False
        
        # Run the script with analytics-only mode
        with st.spinner("Running historical analytics... This may take a few minutes."):
            result = subprocess.run(
                [script_path, "--dashboard-only=false"],
                capture_output=True,
                text=True
            )
        
        if result.returncode == 0:
            st.success("Historical analytics completed successfully!")
            return True
        else:
            st.error(f"Error running analytics: {result.stderr}")
            return False
    except Exception as e:
        st.error(f"Failed to run historical analytics: {e}")
        return False

# Add new diagnostic function
def check_flight_metrics_data(query_api, bucket="flight_metrics", time_range="24h"):
    """Diagnostic function to check if flight metrics data exists and is accessible"""
    results = {
        "connection_status": False,
        "buckets_available": [],
        "flight_metrics_exists": False,
        "sample_data": None,
        "unique_flights": 0,
        "time_range": time_range,
        "error": None
    }
    
    try:
        # Test connection by listing buckets
        client = query_api._influxdb_client
        buckets_api = client.buckets_api()
        buckets = buckets_api.find_buckets().buckets
        results["connection_status"] = True
        results["buckets_available"] = [bucket.name for bucket in buckets]
        
        # Check if flight_metrics bucket exists
        if bucket in results["buckets_available"]:
            results["flight_metrics_exists"] = True
            
            # Try to get sample data
            query = f'''
            from(bucket: "{bucket}")
              |> range(start: -{time_range})
              |> filter(fn: (r) => r["_measurement"] == "flight_metrics")
              |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
              |> limit(n: 10)
            '''
            
            sample_data = query_api.query_data_frame(query)
            
            if isinstance(sample_data, list) and sample_data:
                sample_data = pd.concat(sample_data)
            
            if not sample_data.empty:
                results["sample_data"] = sample_data
                
                # Count unique flights
                count_query = f'''
                from(bucket: "{bucket}")
                  |> range(start: -{time_range})
                  |> filter(fn: (r) => r["_measurement"] == "flight_metrics")
                  |> group(columns: ["icao24"])
                  |> count()
                  |> group()
                  |> count()
                '''
                
                try:
                    count_result = query_api.query_data_frame(count_query)
                    if isinstance(count_result, list) and count_result:
                        count_result = pd.concat(count_result)
                    
                    if not count_result.empty and "_value" in count_result.columns:
                        results["unique_flights"] = count_result["_value"].iloc[0]
                except:
                    # If count query fails, try another approach
                    try:
                        distinct_query = f'''
                        from(bucket: "{bucket}")
                          |> range(start: -{time_range})
                          |> filter(fn: (r) => r["_measurement"] == "flight_metrics" and r["_field"] == "icao24")
                          |> group(columns: ["icao24"])
                          |> distinct(column: "icao24")
                          |> count()
                        '''
                        distinct_result = query_api.query_data_frame(distinct_query)
                        if isinstance(distinct_result, list) and distinct_result:
                            distinct_result = pd.concat(distinct_result)
                        
                        if not distinct_result.empty:
                            results["unique_flights"] = len(distinct_result)
                    except:
                        pass
        
        return results
    except Exception as e:
        results["error"] = str(e)
        return results

def main():
    """Main dashboard application"""
    # Sidebar for controls
    st.sidebar.title("✈️ Airspace Congestion Monitor")
    
    # Add refresh controls
    refresh_col1, refresh_col2 = st.sidebar.columns([1, 1])
    
    with refresh_col1:
        if st.button("🔄 Refresh Now"):
            st.rerun()
    
    with refresh_col2:
        auto_refresh = st.sidebar.checkbox("Auto-refresh", value=False)
    
    if auto_refresh:
        refresh_interval = st.sidebar.slider("Refresh interval (seconds)", 
                                            min_value=5, 
                                            max_value=60, 
                                            value=10)
    
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
    
    # Define anomaly detection parameters (moved to global scope)
    # Default values that will be used unless overridden in the UI
    detection_params = {
        'altitude': {
            'z_threshold': 3.0,
            'change_threshold': 2.5,
            'trend_threshold': 2.0
        },
        'velocity': {
            'z_threshold': 3.0,
            'change_threshold': 2.0,
            'trend_threshold': 2.0
        },
        'heading': {
            'z_threshold': 3.0,
            'change_threshold': 2.5
        }
    }
    
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
                        
                        # Use Folium map
                        m = create_folium_flight_map(df, risk_column='risk_score')
                        if m:
                            folium_static(m)
                        else:
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
                    
                    # Use Folium map
                    m = create_folium_flight_map(metrics_df, risk_column='avg_risk')
                    if m:
                        folium_static(m)
                    else:
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
                        
                        # Create Folium heatmap
                        m = create_folium_grid_heatmap(df)
                        if m:
                            folium_static(m)
                        else:
                            st.error("Failed to create heatmap")
                            # Fallback to a simple heatmap using Matplotlib
                            create_matplotlib_heatmap(df)
                        
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
                    
                    # Create Folium heatmap
                    m = create_folium_grid_heatmap(grid_df)
                    if m:
                        folium_static(m)
                    else:
                        # Fallback to a simple heatmap using Matplotlib
                        create_matplotlib_heatmap(grid_df)
                    
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
        
        # Add tabs for real-time metrics, historical analytics, and anomaly detection
        metrics_tab, historical_tab, anomalies_tab = st.tabs([
            "Real-time Metrics", 
            "Historical Analytics", 
            "Anomaly Detection"
        ])
        
        with metrics_tab:
            influx_client = get_influx_client()
            if influx_client:
                query_api = influx_client.query_api()
                
                time_range_for_metrics = "1h" if data_source == "Kafka Stream" else time_range
                
                with st.spinner("Fetching flight metrics..."):
                    metrics_df = query_flight_metrics(query_api, time_range=time_range_for_metrics)
                
                if not metrics_df.empty:
                    st.success(f"Analyzing metrics for {metrics_df['icao24'].nunique()} unique flights")
                    
                    # Select a specific flight for anomaly detection in the metrics tab
                    flight_ids = sorted(metrics_df['icao24'].unique())
                    selected_flight = st.selectbox(
                        "Select Flight for Analysis", 
                        flight_ids,
                        key="metrics_tab_flight_selector"  # Add a unique key
                    )
                    
                    # Filter for the selected flight
                    flight_data = metrics_df[metrics_df['icao24'] == selected_flight]
                    
                    if not flight_data.empty:
                        # Run anomaly detection with the globally defined parameters
                        anomaly_results = analyze_flight_anomalies(flight_data, parameters=detection_params)
                        
                        if anomaly_results:
                            # Get summary statistics
                            anomaly_summary = get_anomaly_summary(anomaly_results)
                            
                            # Display summary cards
                            create_anomaly_summary_cards(anomaly_summary)
                            
                            # Create severity chart
                            severity = classify_anomaly_severity(anomaly_results)
                            severity_chart = create_anomaly_severity_chart(flight_data, severity)
                            
                            if severity_chart:
                                st.plotly_chart(severity_chart, use_container_width=True)
                            
                            # Create tabs for different visualizations
                            map_tab, timeline_tab, details_tab = st.tabs([
                                "Anomaly Map", "Parameter Timelines", "Anomaly Details"
                            ])
                            
                            with map_tab:
                                # Create map with anomalies highlighted
                                anomaly_map = create_anomaly_map(
                                    flight_data, 
                                    anomaly_results
                                )
                                
                                if anomaly_map:
                                    folium_static(anomaly_map, width=800, height=500)
                                else:
                                    st.warning("Could not create anomaly map")
                            
                            with timeline_tab:
                                # Create parameter timelines with anomalies
                                if 'baro_altitude' in flight_data.columns and 'altitude' in anomaly_results:
                                    altitude_chart = create_anomaly_timeline(
                                        flight_data, anomaly_results, 'baro_altitude'
                                    )
                                    if altitude_chart:
                                        st.plotly_chart(altitude_chart, use_container_width=True)
                                
                                if 'velocity' in flight_data.columns and 'velocity' in anomaly_results:
                                    velocity_chart = create_anomaly_timeline(
                                        flight_data, anomaly_results, 'velocity'
                                    )
                                    if velocity_chart:
                                        st.plotly_chart(velocity_chart, use_container_width=True)
                                
                                if 'heading' in flight_data.columns and 'heading' in anomaly_results:
                                    heading_chart = create_anomaly_timeline(
                                        flight_data, anomaly_results, 'heading'
                                    )
                                    if heading_chart:
                                        st.plotly_chart(heading_chart, use_container_width=True)
                            
                            with details_tab:
                                # Get anomaly events
                                min_anomaly_count = st.slider(
                                    "Minimum Anomaly Count",
                                    min_value=1,
                                    max_value=3,
                                    value=1,
                                    help="Minimum number of simultaneous anomalies to include",
                                    key="metrics_tab_min_anomaly_count"  # Add a unique key
                                )
                                
                                anomaly_events = get_anomaly_timestamps(
                                    anomaly_results, 
                                    min_anomaly_count=min_anomaly_count
                                )
                                
                                # Display anomaly details table
                                display_anomaly_details_table(flight_data, anomaly_events)
                                
                                # Show raw anomaly data
                                with st.expander("Raw Anomaly Data", expanded=False):
                                    for param, param_results in anomaly_results.items():
                                        if param != 'overall':
                                            st.subheader(f"{param.title()} Anomalies")
                                            
                                            # Count anomalies by type
                                            anomaly_counts = {
                                                anomaly_type: anomaly_series.sum()
                                                for anomaly_type, anomaly_series in param_results.items()
                                                if anomaly_type != 'any_anomaly'
                                            }
                                            
                                            # Display as a table
                                            st.write(pd.Series(anomaly_counts).to_frame('Count'))
                        else:
                            st.warning("No anomalies detected for this flight")
                    else:
                        st.warning(f"No data available for flight {selected_flight}")
                else:
                    st.warning("No flight metrics data available for analysis")
            else:
                st.error("Could not connect to InfluxDB")
        
        with historical_tab:
            st.subheader("Historical Flight Data Analytics")
            
            # Check if analytics data exists
            data_path = "data/analytics"
            analytics_data_exists = os.path.exists(data_path)
            
            # Add button to run analytics
            col1, col2 = st.columns([1, 3])
            with col1:
                if st.button("Run Analytics"):
                    run_historical_analytics()
                    # Refresh the page to show new results
                    st.rerun()
            
            with col2:
                if not analytics_data_exists:
                    st.warning("No historical analytics data found. Click 'Run Analytics' to generate insights.")
            
            if analytics_data_exists:
                # Create tabs for different analytics views
                analytics_tabs = st.tabs([
                    "Overview", 
                    "Temporal Patterns", 
                    "Congestion Hotspots", 
                    "Risk Anomalies",
                    "Flight Clusters"
                ])
                
                # Load analytics data
                hourly_patterns = load_json_files(f"{data_path}/hourly_patterns/*.json")
                daily_patterns = load_json_files(f"{data_path}/daily_patterns/*.json")
                congestion_hotspots = load_json_files(f"{data_path}/congestion_hotspots/*.json")
                risk_anomalies = load_json_files(f"{data_path}/risk_anomalies/*.json")
                flight_clusters = load_json_files(f"{data_path}/flight_clusters/*.json")
                report = load_markdown_report(f"{data_path}/insights_report.md")
                
                # Overview tab
                with analytics_tabs[0]:
                    # Display report
                    st.markdown(report)
                    
                    # Show key metrics
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        if not hourly_patterns.empty:
                            peak_hour = hourly_patterns.loc[hourly_patterns['flight_count'].idxmax()]
                            st.metric("Peak Hour", f"{int(peak_hour['hour_of_day'])}:00", f"{int(peak_hour['flight_count'])} flights")
                    
                    with col2:
                        if not congestion_hotspots.empty:
                            top_hotspot = congestion_hotspots.iloc[0]
                            st.metric("Top Congestion", f"Lat {top_hotspot['center_latitude']:.1f}, Lon {top_hotspot['center_longitude']:.1f}", 
                                     f"{int(top_hotspot['flight_count'])} flights")
                    
                    with col3:
                        if not risk_anomalies.empty:
                            max_risk = risk_anomalies['max_anomaly_risk'].max()
                            st.metric("Max Risk Score", f"{max_risk:.2f}", f"{len(risk_anomalies)} anomalies")
                
                # Temporal Patterns tab
                with analytics_tabs[1]:
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.subheader("Hourly Patterns")
                        if not hourly_patterns.empty:
                            hourly_chart = create_hourly_chart(hourly_patterns)
                            st.plotly_chart(hourly_chart, use_container_width=True)
                        else:
                            st.info("No hourly pattern data available")
                    
                    with col2:
                        st.subheader("Daily Patterns")
                        if not daily_patterns.empty:
                            daily_chart = create_daily_chart(daily_patterns)
                            st.plotly_chart(daily_chart, use_container_width=True)
                        else:
                            st.info("No daily pattern data available")
                    
                    # Add detailed tables
                    with st.expander("View Raw Data"):
                        tab1, tab2 = st.tabs(["Hourly Data", "Daily Data"])
                        
                        with tab1:
                            if not hourly_patterns.empty:
                                st.dataframe(hourly_patterns)
                            else:
                                st.info("No hourly data available")
                        
                        with tab2:
                            if not daily_patterns.empty:
                                st.dataframe(daily_patterns)
                            else:
                                st.info("No daily data available")
                
                # Congestion Hotspots tab
                with analytics_tabs[2]:
                    if not congestion_hotspots.empty:
                        # Create map
                        st.subheader("Hotspot Map")
                        hotspot_map = create_hotspot_map(congestion_hotspots)
                        if hotspot_map:
                            folium_static(hotspot_map, width=1000, height=600)
                        
                        # Show data table
                        st.subheader("Top 20 Congestion Hotspots")
                        st.dataframe(
                            congestion_hotspots[['center_latitude', 'center_longitude', 'flight_count', 'avg_risk', 'avg_altitude']]
                            .sort_values('flight_count', ascending=False)
                        )
                    else:
                        st.info("No congestion hotspot data available")
                
                # Risk Anomalies tab
                with analytics_tabs[3]:
                    if not risk_anomalies.empty:
                        # Create timeline chart
                        st.subheader("Anomaly Timeline")
                        anomaly_chart = create_anomaly_chart(risk_anomalies)
                        if anomaly_chart:
                            st.plotly_chart(anomaly_chart, use_container_width=True)
                        
                        # Show data table
                        st.subheader("Risk Anomaly Windows")
                        st.dataframe(
                            risk_anomalies[['window_start', 'window_end', 'anomaly_count', 'avg_anomaly_risk', 'max_anomaly_risk']]
                            .sort_values('max_anomaly_risk', ascending=False)
                        )
                    else:
                        st.info("No risk anomaly data available")
                
                # Flight Clusters tab
                with analytics_tabs[4]:
                    if not flight_clusters.empty:
                        # Create cluster chart
                        st.subheader("Cluster Analysis")
                        cluster_chart = create_cluster_chart(flight_clusters)
                        if cluster_chart:
                            st.plotly_chart(cluster_chart, use_container_width=True)
                        
                        # Show data table
                        st.subheader("Cluster Characteristics")
                        st.dataframe(flight_clusters)
                        
                        # Add explanation
                        st.markdown("""
                            ### Cluster Interpretation
                            
                            The parallel coordinates plot above shows the characteristics of each flight behavior cluster:
                            
                            - **Flight Count**: Number of flights in each cluster
                            - **Risk Score**: Average risk score for flights in the cluster
                            - **Velocity**: Average speed in meters per second
                            - **Altitude**: Average altitude in meters
                            - **Vertical Rate**: Average vertical speed in meters per second
                            - **Acceleration**: Average acceleration in meters per second squared
                            - **Turn Rate**: Average turn rate in degrees per second
                            
                            Each line represents a cluster, and the color corresponds to the cluster number.
                        """)
                    else:
                        st.info("No flight cluster data available")
        
        # New Anomaly Detection tab
        with anomalies_tab:
            st.subheader("Flight Anomaly Detection")
            
            st.markdown("""
            This feature detects unusual flight behavior that may indicate equipment issues, 
            dangerous maneuvers, or other anomalies. The system analyzes:
            
            - **Statistical Outliers**: Values that deviate significantly from normal patterns
            - **Sudden Changes**: Rapid shifts in altitude, speed, or heading
            - **Trend Deviations**: Departures from established flight trajectory patterns
            - **Physical Inconsistencies**: Behaviors that violate expected aircraft physics
            """)
            
            # Add sensitivity controls
            with st.expander("Detection Settings", expanded=False):
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    z_threshold = st.slider(
                        "Statistical Outlier Threshold (σ)",
                        min_value=1.0,
                        max_value=5.0,
                        value=detection_params['altitude']['z_threshold'],
                        step=0.5,
                        help="Lower values detect more outliers (more sensitive)"
                    )
                
                with col2:
                    change_threshold = st.slider(
                        "Sudden Change Sensitivity",
                        min_value=1.0,
                        max_value=5.0,
                        value=detection_params['altitude']['change_threshold'],
                        step=0.5,
                        help="Lower values detect more sudden changes (more sensitive)"
                    )
                
                with col3:
                    trend_threshold = st.slider(
                        "Trend Deviation Sensitivity",
                        min_value=1.0,
                        max_value=5.0,
                        value=detection_params['altitude']['trend_threshold'],
                        step=0.5,
                        help="Lower values detect more trend deviations (more sensitive)"
                    )
                
                # Update detection parameters dictionary
                detection_params['altitude']['z_threshold'] = z_threshold
                detection_params['altitude']['change_threshold'] = change_threshold
                detection_params['altitude']['trend_threshold'] = trend_threshold
                detection_params['velocity']['z_threshold'] = z_threshold
                detection_params['velocity']['change_threshold'] = change_threshold
                detection_params['velocity']['trend_threshold'] = trend_threshold
                detection_params['heading']['z_threshold'] = z_threshold
                detection_params['heading']['change_threshold'] = change_threshold
            
            # Analyze flight data
            influx_client = get_influx_client()
            if influx_client:
                query_api = influx_client.query_api()
                
                time_range_for_metrics = "1h" if data_source == "Kafka Stream" else time_range
                
                with st.spinner("Analyzing flight data for anomalies..."):
                    metrics_df = query_flight_metrics(query_api, time_range=time_range_for_metrics)
                
                if not metrics_df.empty:
                    # Select a specific flight for anomaly detection
                    flight_ids = sorted(metrics_df['icao24'].unique())
                    selected_flight = st.selectbox(
                        "Select Flight for Analysis", 
                        flight_ids,
                        key="anomalies_tab_flight_selector"  # Add a unique key
                    )
                    
                    # Filter for the selected flight
                    flight_data = metrics_df[metrics_df['icao24'] == selected_flight]
                    
                    if not flight_data.empty:
                        # Run anomaly detection
                        anomaly_results = analyze_flight_anomalies(flight_data, parameters=detection_params)
                        
                        if anomaly_results:
                            # Get summary statistics
                            anomaly_summary = get_anomaly_summary(anomaly_results)
                            
                            # Display summary cards
                            create_anomaly_summary_cards(anomaly_summary)
                            
                            # Create severity chart
                            severity = classify_anomaly_severity(anomaly_results)
                            severity_chart = create_anomaly_severity_chart(flight_data, severity)
                            
                            if severity_chart:
                                st.plotly_chart(severity_chart, use_container_width=True)
                            
                            # Create tabs for different visualizations
                            map_tab, timeline_tab, details_tab = st.tabs([
                                "Anomaly Map", "Parameter Timelines", "Anomaly Details"
                            ])
                            
                            with map_tab:
                                # Create map with anomalies highlighted
                                anomaly_map = create_anomaly_map(
                                    flight_data, 
                                    anomaly_results
                                )
                                
                                if anomaly_map:
                                    folium_static(anomaly_map, width=800, height=500)
                                else:
                                    st.warning("Could not create anomaly map")
                            
                            with timeline_tab:
                                # Create parameter timelines with anomalies
                                if 'baro_altitude' in flight_data.columns and 'altitude' in anomaly_results:
                                    altitude_chart = create_anomaly_timeline(
                                        flight_data, anomaly_results, 'baro_altitude'
                                    )
                                    if altitude_chart:
                                        st.plotly_chart(altitude_chart, use_container_width=True)
                                
                                if 'velocity' in flight_data.columns and 'velocity' in anomaly_results:
                                    velocity_chart = create_anomaly_timeline(
                                        flight_data, anomaly_results, 'velocity'
                                    )
                                    if velocity_chart:
                                        st.plotly_chart(velocity_chart, use_container_width=True)
                                
                                if 'heading' in flight_data.columns and 'heading' in anomaly_results:
                                    heading_chart = create_anomaly_timeline(
                                        flight_data, anomaly_results, 'heading'
                                    )
                                    if heading_chart:
                                        st.plotly_chart(heading_chart, use_container_width=True)
                            
                            with details_tab:
                                # Get anomaly events
                                min_anomaly_count = st.slider(
                                    "Minimum Anomaly Count",
                                    min_value=1,
                                    max_value=3,
                                    value=1,
                                    help="Minimum number of simultaneous anomalies to include",
                                    key="anomalies_tab_min_anomaly_count"  # Add a unique key
                                )
                                
                                anomaly_events = get_anomaly_timestamps(
                                    anomaly_results, 
                                    min_anomaly_count=min_anomaly_count
                                )
                                
                                # Display anomaly details table
                                display_anomaly_details_table(flight_data, anomaly_events)
                                
                                # Show raw anomaly data
                                with st.expander("Raw Anomaly Data", expanded=False):
                                    for param, param_results in anomaly_results.items():
                                        if param != 'overall':
                                            st.subheader(f"{param.title()} Anomalies")
                                            
                                            # Count anomalies by type
                                            anomaly_counts = {
                                                anomaly_type: anomaly_series.sum()
                                                for anomaly_type, anomaly_series in param_results.items()
                                                if anomaly_type != 'any_anomaly'
                                            }
                                            
                                            # Display as a table
                                            st.write(pd.Series(anomaly_counts).to_frame('Count'))
                        else:
                            st.warning("No anomalies detected for this flight")
                    else:
                        st.warning(f"No data available for flight {selected_flight}")
                else:
                    st.warning("No flight metrics data available for analysis")
            else:
                st.error("Could not connect to InfluxDB")
    
    # Auto-refresh logic
    if data_source == "Kafka Stream" and auto_refresh:
        # Initialize session state for refresh timing
        if 'last_refresh' not in st.session_state:
            st.session_state.last_refresh = time.time()
            
        # Calculate time since last refresh
        time_since_refresh = time.time() - st.session_state.last_refresh
        
        # Check if it's time to refresh
        if time_since_refresh >= refresh_interval:
            st.session_state.last_refresh = time.time()
            st.rerun()
        else:
            # Show a small progress bar at the bottom of the page
            st.sidebar.caption("Next auto-refresh in:")
            progress = st.sidebar.progress(time_since_refresh / refresh_interval)
            
            # Update the time display without full page refresh
            if st.sidebar.button("Cancel auto-refresh"):
                st.session_state.auto_refresh = False
                st.rerun()

if __name__ == "__main__":
    main()