#!/usr/bin/env python3
"""
opensky_dashboard.py

Interactive Streamlit dashboard for visualizing flight data,
including real-time aircraft positions, metrics, and spatial aggregates.
"""
import os
import sys
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
import glob
import plotly.express as px
import plotly.graph_objects as go

# Import anomaly detection functionality
from spark.anomaly_detection import analyze_flight_anomalies, get_anomaly_summary, classify_anomaly_severity, get_anomaly_timestamps

# Add folium imports
import folium
from streamlit_folium import folium_static
from folium.plugins import HeatMap, MarkerCluster, Fullscreen

# Try to import MongoDB utilities (optional)
try:
    from spark.mongodb_utils import (
        get_mongodb_client, get_date_range, query_flights_by_timerange,
        get_hourly_patterns, get_daily_patterns, get_congestion_hotspots,
        get_flight_density_timeline, get_anomaly_windows, get_top_flights_by_risk
    )
    MONGODB_AVAILABLE = True
except ImportError:
    MONGODB_AVAILABLE = False

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
MAX_FLIGHTS = 5000  # maximum flights to display - standardized across all sections

# Add the current directory to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

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
    bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP', 'localhost:9092')  # Updated to use port 9092
    print(f"Connecting to Kafka at {bootstrap_servers} for topic {topic}")
    
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=None,  # Remove group ID to always read all messages
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=3000
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

def fetch_recent_flights(consumer, max_messages=MAX_FLIGHTS, timeout_sec=15):
    """Fetch recent flight data from Kafka stream - using standardized MAX_FLIGHTS"""
    messages = []
    start_time = time.time()
    
    try:
        print(f"Fetching messages from Kafka (max: {max_messages}, timeout: {timeout_sec}s)")
        while len(messages) < max_messages and (time.time() - start_time) < timeout_sec:
            msg_batch = consumer.poll(timeout_ms=3000, max_records=max_messages)
            if not msg_batch:
                print("No messages in this batch")
                continue
            
            print(f"Received batch of {sum(len(records) for records in msg_batch.values())} messages")
            for topic_partition, records in msg_batch.items():
                print(f"  - Topic: {topic_partition.topic}, Partition: {topic_partition.partition}, Records: {len(records)}")
                for record in records:
                    if record.value:  # Add null check
                        try:
                            # Try to parse the message
                            if isinstance(record.value, dict):
                                messages.append(record.value)
                            elif isinstance(record.value, str):
                                # Try to parse JSON string
                                # Fix missing commas in JSON - common issue with grid data
                                fixed_json = record.value.replace('"\n"', '",\n"')
                                fixed_json = fixed_json.replace('""', '","')
                                try:
                                    messages.append(json.loads(fixed_json))
                                except:
                                    # If that fails, try the original
                                    messages.append(json.loads(record.value))
                            else:
                                print(f"Unknown message type: {type(record.value)}")
                                messages.append(record.value)
                        except Exception as e:
                            print(f"Error parsing message: {e}")
                            # Try to fix common JSON issues in grid data
                            if isinstance(record.value, str):
                                try:
                                    # Fix missing commas between key-value pairs
                                    fixed_value = record.value.replace('"\n"', '",\n"')
                                    # Replace double quotes between values and keys with comma + quote
                                    fixed_value = fixed_value.replace('"}"', '"},"')
                                    # Try to parse the fixed JSON
                                    parsed = json.loads(fixed_value)
                                    messages.append(parsed)
                                    print("Successfully fixed and parsed JSON")
                                except Exception as e2:
                                    print(f"Failed to fix JSON: {e2}")
                                    # Still add the raw message for debugging
                                    messages.append({"raw_value": record.value})
        
        print(f"Total messages fetched: {len(messages)}")
        return messages[:MAX_FLIGHTS]  # Ensure we don't exceed MAX_FLIGHTS
    except Exception as e:
        st.error(f"Error fetching flight data: {e}")
        print(f"Error fetching messages: {str(e)}")
        return []

def process_flight_data(messages):
    """Convert Kafka flight messages to a DataFrame"""
    if not messages:
        return pd.DataFrame()
    
    # Fix any formatting issues in the messages
    fixed_messages = []
    for msg in messages:
        if isinstance(msg, dict):
            if "payload" in msg and isinstance(msg["payload"], dict):
                # Handle Kafka connect format
                fixed_messages.append(msg["payload"])
            elif "value" in msg and isinstance(msg["value"], dict):
                # Handle Kafka REST Proxy format
                fixed_messages.append(msg["value"])
            else:
                # Regular flight message
                fixed_messages.append(msg)
        elif isinstance(msg, str):
            try:
                # Try to parse as JSON
                parsed = json.loads(msg)
                if "value" in parsed and isinstance(parsed["value"], dict):
                    # Kafka REST Proxy format
                    fixed_messages.append(parsed["value"])
                else:
                    # Regular flight message
                    fixed_messages.append(parsed)
            except json.JSONDecodeError:
                # Invalid JSON, skip
                continue
    
    # Create DataFrame from fixed messages
    df = pd.DataFrame(fixed_messages)
    
    # ── convert fetch_time (ms) into datetime, coercing bad values
    if "fetch_time" in df.columns:
        # Check for zero or very small timestamps (likely errors)
        current_time_ms = int(time.time() * 1000)
        df["fetch_time"] = df["fetch_time"].apply(
            lambda x: current_time_ms if pd.isna(x) or x < 946684800000 else x  # timestamps before 2000-01-01 are considered invalid
        )
        df["fetch_time"] = pd.to_datetime(df["fetch_time"], unit="ms", errors="coerce")
        
        # Replace NaT values with current time
        df["fetch_time"] = df["fetch_time"].fillna(pd.Timestamp.now())
    
    # ── Clean numeric columns & replace nulls with zero
    for col in ['longitude', 'latitude', 'baro_altitude', 'velocity', 'vertical_rate', 
                'lat_bin', 'lon_bin', 'flight_count', 'avg_risk']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # ── Drop rows with no valid coords (for flight data) or bin info (for grid data)
    if 'lat_bin' in df.columns and 'lon_bin' in df.columns:
        # Grid data
        df = df.dropna(subset=['lat_bin', 'lon_bin'])
    elif 'longitude' in df.columns and 'latitude' in df.columns:
        # Flight data
        df = df.dropna(subset=['longitude', 'latitude'])
    
    # ── Fill common string fields so UI shows empty strings instead of NaN
    for col in ['icao24', 'callsign', 'origin_country', 'squawk', 'position_source']:
        if col in df.columns:
            df[col] = df[col].fillna("")

    # ── Handle airline names: show "No Data Available" if missing or blank
    # ── Extract airline from nested aircraft metadata (fallback: No Data Available)
    if 'aircraft' in df.columns:
        df['airline'] = df['aircraft'].apply(
            lambda x: x.get('registered_owner')
                      if isinstance(x, dict) and x.get('registered_owner')
                      else "No Data Available"
        )
    return df

def create_flight_map(df, risk_column=None):
    """Create a PyDeck map visualization of flights"""
    if df.empty:
        return None
    
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

def display_basic_map(df):
    """Display a basic map using Streamlit's built-in map function"""
    if not df.empty and 'latitude' in df.columns and 'longitude' in df.columns:
        st.map(df[['latitude', 'longitude']], zoom=3)
        +    st.dataframe(df)
        # Display flight information in a table below the map
        # with st.expander("Flight Details", expanded=False):
        #     display_cols = [
        #         "icao24", "callsign", "origin_country",
        #         "longitude", "latitude", "baro_altitude", 
        #         "velocity", "vertical_rate"
        #     ]
        #     available_cols = [c for c in display_cols if c in df.columns]
        #     st.dataframe(df[available_cols])
        
def create_folium_flight_map(df, risk_column=None):
    """Create a Folium map visualization of flights"""
    if df.empty:
        return None
    
    try:
        # Determine center of the map
        center_lat = df["latitude"].mean()
        center_lon = df["longitude"].mean()
        
        # Create the map with a more attractive style
        m = folium.Map(
            location=[center_lat, center_lon], 
            zoom_start=5,
            tiles='CartoDB positron',  # More modern, clean style
            width='100%',              # Use full width
            height='700px'             # Increase height
        )
        
        # Add fullscreen control
        Fullscreen(position='topright').add_to(m)
        
        # Create marker cluster with custom options for better visualization
        marker_cluster = MarkerCluster(
            options={
                'maxClusterRadius': 40,       # Smaller clusters for better detail
                'disableClusteringAtZoom': 9,  # Show individual planes at higher zoom
                'spiderfyOnMaxZoom': True     # Spread markers when clicked
            }
        ).add_to(m)
        
        # Determine color field
        if risk_column in df.columns:
            color_by = risk_column
        elif 'velocity' in df.columns:
            color_by = 'velocity'
        else:
            df['default_color'] = 0.5
            color_by = 'default_color'
        
        # Add markers for each flight with improved styling
        for _, row in df.iterrows():
            if pd.isna(row['latitude']) or pd.isna(row['longitude']):
                continue
                
            # Create popup text with better formatting
            popup_text = f"""
            <div style="font-family: Arial, sans-serif; min-width: 200px; max-width: 300px;">
                <h4 style="margin-top: 0; color: #2c3e50;">Flight {row.get('callsign', row.get('icao24', 'Unknown'))}</h4>
                <table style="width: 100%; border-collapse: collapse;">
                    <tr><td style="padding: 3px; border-bottom: 1px solid #eee;"><b>ICAO24:</b></td><td style="padding: 3px; border-bottom: 1px solid #eee;">{row.get('icao24', 'Unknown')}</td></tr>
            """
            
            if 'callsign' in row and not pd.isna(row['callsign']) and row['callsign']:
                popup_text += f'<tr><td style="padding: 3px; border-bottom: 1px solid #eee;"><b>Callsign:</b></td><td style="padding: 3px; border-bottom: 1px solid #eee;">{row["callsign"]}</td></tr>'
            
            if 'origin_country' in row and not pd.isna(row['origin_country']) and row['origin_country']:
                popup_text += f'<tr><td style="padding: 3px; border-bottom: 1px solid #eee;"><b>Country:</b></td><td style="padding: 3px; border-bottom: 1px solid #eee;">{row["origin_country"]}</td></tr>'
            
            if 'airline' in row and not pd.isna(row['airline']) and row['airline'] != "No Data Available":
                popup_text += f'<tr><td style="padding: 3px; border-bottom: 1px solid #eee;"><b>Airline:</b></td><td style="padding: 3px; border-bottom: 1px solid #eee;">{row["airline"]}</td></tr>'
            
            if 'baro_altitude' in row and not pd.isna(row['baro_altitude']):
                popup_text += f'<tr><td style="padding: 3px; border-bottom: 1px solid #eee;"><b>Altitude:</b></td><td style="padding: 3px; border-bottom: 1px solid #eee;">{row["baro_altitude"]:.0f} m</td></tr>'
            
            if 'velocity' in row and not pd.isna(row['velocity']):
                popup_text += f'<tr><td style="padding: 3px; border-bottom: 1px solid #eee;"><b>Speed:</b></td><td style="padding: 3px; border-bottom: 1px solid #eee;">{row["velocity"]:.1f} m/s</td></tr>'
            
            if 'vertical_rate' in row and not pd.isna(row['vertical_rate']):
                popup_text += f'<tr><td style="padding: 3px; border-bottom: 1px solid #eee;"><b>Vertical Rate:</b></td><td style="padding: 3px; border-bottom: 1px solid #eee;">{row["vertical_rate"]:.1f} m/s</td></tr>'
            
            if risk_column in row and not pd.isna(row[risk_column]):
                popup_text += f'<tr><td style="padding: 3px;"><b>Risk:</b></td><td style="padding: 3px;">{row[risk_column]:.2f}</td></tr>'
            
            popup_text += """
                </table>
            </div>
            """
                
            # Set color based on value with better color scheme
            if color_by in row and not pd.isna(row[color_by]):
                value = row[color_by]
                if color_by == 'risk_score' or color_by == 'avg_risk':
                    # Enhanced color scheme for risk
                    if value is None or pd.isna(value):
                        color = 'blue'  # Default color for unknown risk
                    elif value > 1.5:
                        color = 'red'   # High risk
                    elif value > 1.0:
                        color = 'orange' # Medium-high risk
                    elif value > 0.5:
                        color = 'gold'  # Medium risk
                    else:
                        color = 'green' # Low risk
                elif color_by == 'velocity':
                    # Enhanced color scheme for velocity
                    if value > 250:
                        color = 'darkblue'
                    elif value > 200:
                        color = 'blue'
                    elif value > 150:
                        color = 'cadetblue'
                    elif value > 100:
                        color = 'lightblue'
                    else:
                        color = 'gray'
                else:
                    color = 'blue'
            else:
                color = 'blue'
            
            # Create custom plane icon with rotation if heading available
            icon_options = {
                'icon': 'plane',
                'prefix': 'fa',
                'color': color,
                'iconColor': 'white'
            }
            
            # Add marker to cluster with improved popup
            folium.Marker(
                location=[row['latitude'], row['longitude']],
                popup=folium.Popup(popup_text, max_width=300),
                icon=folium.Icon(**icon_options),
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
        # Create base map centered on the data
        center_lat = df['latitude'].mean()
        center_lon = df['longitude'].mean()
        
        # Create map with a dark theme for better contrast and increased size
        m = folium.Map(
            location=[center_lat, center_lon],
            zoom_start=4,
            tiles='cartodbdark_matter',
            width='100%',
            height='750px'  # Increased height
        )

        # Add fullscreen control for better user experience
        Fullscreen(position='topright').add_to(m)
        
        # Prepare heatmap data - must be a list of [lat, lon, intensity]
        heat_data = []
        
        # Normalize flight counts for intensity
        max_flights = df['flight_count'].max()
        
        for _, row in df.iterrows():
            try:
                lat = float(row['latitude'])
                lon = float(row['longitude'])
                # Normalize intensity between 0 and 1
                intensity = float(row['flight_count']) / max_flights if max_flights > 0 else 0
                heat_data.append([lat, lon, intensity])
            except (ValueError, TypeError) as e:
                print(f"Error processing row: {e}")
                continue

        # Add heatmap layer with enhanced gradient - KEYS MUST BE STRINGS for Folium
        HeatMap(
            data=heat_data,
            min_opacity=0.2,
            max_opacity=0.9,
            radius=30,  # Slightly larger radius
            blur=15,
            gradient={
                "0.2": "#1a1a1a",  # Very dark gray for low density
                "0.4": "#0077b6",  # Dark blue
                "0.6": "#00b4d8",  # Light blue
                "0.8": "#ff9e00",  # Orange
                "1.0": "#ff0000"   # Red for high density
            }
        ).add_to(m)

        return m

    except Exception as e:
        import traceback
        print(f"Error creating grid heatmap: {str(e)}")
        print(traceback.format_exc())
        return None

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
            title_font=dict(color='royalblue'),
            tickfont=dict(color='royalblue')
        ),
        yaxis2=dict(
            title='Avg Risk Score',
            title_font=dict(color='firebrick'),
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
            title_font=dict(color='mediumseagreen'),
            tickfont=dict(color='mediumseagreen')
        ),
        yaxis2=dict(
            title='Avg Risk Score',
            title_font=dict(color='darkorange'),
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
        # Handle None values in risk
        if risk is None or pd.isna(risk):
            color = 'blue'  # Default color for unknown risk
        else:
            color = 'green' if risk < 0.3 else 'orange' if risk < 0.7 else 'red'
        
        # Create popup text with proper handling of None values
        risk_text = f"{risk:.2f}" if risk is not None and not pd.isna(risk) else "Unknown"
        altitude_text = f"{row['avg_altitude']:.0f}" if row['avg_altitude'] is not None and not pd.isna(row['avg_altitude']) else "Unknown"
        
        popup_text = f"""
            <b>Flight Count:</b> {row['flight_count']}<br>
            <b>Avg Risk:</b> {risk_text}<br>
            <b>Avg Altitude:</b> {altitude_text} m<br>
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

def create_anomaly_chart(anomalies_df):
    """Create anomaly timeline chart"""
    if anomalies_df.empty:
        return None
    
    # Convert string timestamps to datetime
    anomalies_df['start_time'] = pd.to_datetime(anomalies_df['window_start'])
    anomalies_df['end_time'] = pd.to_datetime(anomalies_df['window_end'])
    
    # Sort by time
    anomalies_df = anomalies_df.sort_values('start_time')
    
    fig = go.Figure()
    
    # Add scatter plot for anomalies
    fig.add_trace(go.Scatter(
        x=anomalies_df['start_time'],
        y=anomalies_df['max_anomaly_risk'],
        mode='markers',
        name='Max Risk',
        marker=dict(
            size=anomalies_df['anomaly_count'] * 2,
            color=anomalies_df['max_anomaly_risk'],
            colorscale='Reds',
            showscale=True,
            colorbar=dict(title='Risk Score')
        ),
        text=[f"Count: {c}<br>Avg Risk: {a:.2f}<br>Max Risk: {m:.2f}" 
              for c, a, m in zip(anomalies_df['anomaly_count'], 
                                 anomalies_df['avg_anomaly_risk'], 
                                 anomalies_df['max_anomaly_risk'])],
        hoverinfo='text+x'
    ))
    
    # Update layout
    fig.update_layout(
        title='Risk Anomalies Timeline',
        xaxis=dict(title='Time'),
        yaxis=dict(title='Max Risk Score'),
        margin=dict(l=20, r=20, t=40, b=20),
        hovermode='closest'
    )
    
    return fig

def create_cluster_chart(clusters_df):
    """Create cluster analysis chart"""
    if clusters_df.empty:
        return None
    
    # Sort by cluster number
    clusters_df = clusters_df.sort_values('prediction')
    
    # Create parallel coordinates plot
    dimensions = [
        dict(range=[0, clusters_df['flight_count'].max()],
             label='Flight Count', values=clusters_df['flight_count']),
        dict(range=[0, clusters_df['cluster_avg_risk'].max()],
             label='Risk Score', values=clusters_df['cluster_avg_risk']),
        dict(range=[0, clusters_df['cluster_avg_velocity'].max()],
             label='Velocity (m/s)', values=clusters_df['cluster_avg_velocity']),
        dict(range=[0, clusters_df['cluster_avg_altitude'].max()],
             label='Altitude (m)', values=clusters_df['cluster_avg_altitude']),
        dict(range=[clusters_df['cluster_avg_vertical_rate'].min(), 
                    clusters_df['cluster_avg_vertical_rate'].max()],
             label='Vertical Rate (m/s)', values=clusters_df['cluster_avg_vertical_rate']),
        dict(range=[clusters_df['cluster_avg_acceleration'].min(), 
                    clusters_df['cluster_avg_acceleration'].max()],
             label='Acceleration (m/s²)', values=clusters_df['cluster_avg_acceleration']),
        dict(range=[clusters_df['cluster_avg_turn_rate'].min(), 
                    clusters_df['cluster_avg_turn_rate'].max()],
             label='Turn Rate (°/s)', values=clusters_df['cluster_avg_turn_rate'])
    ]
    
    fig = go.Figure(data=
        go.Parcoords(
            line=dict(
                color=clusters_df['prediction'],
                colorscale='Viridis',
                showscale=True,
                colorbar=dict(title='Cluster')
            ),
            dimensions=dimensions
        )
    )
    
    # Update layout
    fig.update_layout(
        title='Flight Behavior Clusters',
        margin=dict(l=80, r=80, t=80, b=40),
    )
    
    return fig

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

def check_kafka_topic(topic, bootstrap_servers="localhost:9092"):
    """Check if a Kafka topic exists and has data"""
    try:
        from kafka.admin import KafkaAdminClient, NewTopic
        from kafka import KafkaConsumer
        import json
        
        # Try to get a consumer for the topic
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None
        )
        
        # Check if the topic exists
        topics = consumer.topics()
        if topic not in topics:
            return {
                "exists": False,
                "message": f"Topic '{topic}' does not exist",
                "topics": list(topics)
            }
        
        # Try to get a message from the topic
        messages = []
        for i, message in enumerate(consumer):
            if i >= 1:  # Just get one message
                break
            messages.append(message.value)
        
        consumer.close()
        
        if messages:
            return {
                "exists": True,
                "has_data": True,
                "message": f"Topic '{topic}' exists and has data",
                "sample": messages[0]
            }
        else:
            return {
                "exists": True,
                "has_data": False,
                "message": f"Topic '{topic}' exists but has no data"
            }
    except Exception as e:
        return {
            "exists": None,
            "error": str(e),
            "message": f"Error checking topic '{topic}': {e}"
        }

def process_grid_data(messages):
    """Convert Kafka grid messages to a DataFrame"""
    if not messages:
        return pd.DataFrame()
    
    # Fix any formatting issues in the messages
    fixed_messages = []
    for msg in messages:
        if isinstance(msg, dict):
            # Check if this looks like a grid message
            if any(key in msg for key in ['lat_bin', 'lon_bin', 'flight_count']):
                fixed_msg = {}
                for k, v in msg.items():
                    # Clean up any string values that should be numeric
                    if k in ['lat_bin', 'lon_bin', 'flight_count']:
                        try:
                            fixed_msg[k] = int(str(v).strip())
                        except (ValueError, TypeError):
                            continue
                    elif k == 'avg_risk':
                        try:
                            fixed_msg[k] = float(str(v).strip())
                        except (ValueError, TypeError):
                            fixed_msg[k] = 0.0
                    else:
                        fixed_msg[k] = str(v).strip()
                if all(k in fixed_msg for k in ['lat_bin', 'lon_bin', 'flight_count']):
                    fixed_messages.append(fixed_msg)
        elif isinstance(msg, str):
            # Try to fix malformed JSON
            try:
                # Add missing commas between key-value pairs
                fixed_json = msg.replace('"\n"', '",\n"')
                fixed_json = fixed_json.replace('""', '","')
                # Ensure proper JSON structure
                if not fixed_json.startswith('{'):
                    fixed_json = '{' + fixed_json
                if not fixed_json.endswith('}'):
                    fixed_json = fixed_json + '}'
                # Parse the fixed JSON
                parsed_msg = json.loads(fixed_json)
                if any(key in parsed_msg for key in ['lat_bin', 'lon_bin', 'flight_count']):
                    fixed_msg = {}
                    for k, v in parsed_msg.items():
                        if k in ['lat_bin', 'lon_bin', 'flight_count']:
                            try:
                                fixed_msg[k] = int(str(v).strip())
                            except (ValueError, TypeError):
                                continue
                        elif k == 'avg_risk':
                            try:
                                fixed_msg[k] = float(str(v).strip())
                            except (ValueError, TypeError):
                                fixed_msg[k] = 0.0
                        else:
                            fixed_msg[k] = str(v).strip()
                    if all(k in fixed_msg for k in ['lat_bin', 'lon_bin', 'flight_count']):
                        fixed_messages.append(fixed_msg)
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON message: {e}")
                continue
    
    # Create DataFrame
    df = pd.DataFrame(fixed_messages)
    
    # Convert numeric columns
    if not df.empty:
        # Convert grid coordinates to map coordinates
        df['latitude'] = df['lat_bin'].astype(float).apply(lambda x: (x - 90) + 0.5)
        df['longitude'] = df['lon_bin'].astype(float).apply(lambda x: (x - 180) + 0.5)
        
        # Ensure flight_count is numeric
        df['flight_count'] = pd.to_numeric(df['flight_count'], errors='coerce')
        
        # Drop any rows with missing essential data
        df = df.dropna(subset=['latitude', 'longitude', 'flight_count'])
        
        # Print debug info
        print(f"Processed {len(df)} valid grid cells")
        print("DataFrame info:")
        print(df.info())
        print("\nSample data:")
        print(df.head())
    
    return df

# New function to check MongoDB availability
def check_mongodb_connection():
    """Check if MongoDB is available and has flight data"""
    if not MONGODB_AVAILABLE:
        return False, "MongoDB utilities not available. Install pymongo with: pip install pymongo"
    
    client = get_mongodb_client()
    if client is None:
        return False, "Could not connect to MongoDB. Is it running?"
    
    try:
        # Check if we can get date range
        min_date, max_date = get_date_range()
        if min_date is None or max_date is None:
            return False, "No flight data found in MongoDB. Import data first."
        
        return True, f"MongoDB connected."
    except Exception as e:
        return False, f"Error checking MongoDB: {e}"

# New function to display flight density timeline
def display_flight_density_timeline():
    """Display flight density timeline visualization"""
    st.subheader("Flight Density Timeline")
    
    # Get timeline data
    interval = st.slider("Time interval (minutes)", 10, 120, 30, 10)
    timeline_df = get_flight_density_timeline(interval_minutes=interval)
    
    if timeline_df.empty:
        st.warning("No timeline data available")
        return
    
    # Create timeline visualization
    fig = go.Figure()
    
    # Add flight count bars
    fig.add_trace(go.Bar(
        x=timeline_df['timestamp'],
        y=timeline_df['flight_count'],
        name='Flight Count',
        marker_color='royalblue',
        opacity=0.7
    ))
    
    # Add risk score line on secondary y-axis
    if 'avg_risk' in timeline_df.columns:
        fig.add_trace(go.Scatter(
            x=timeline_df['timestamp'],
            y=timeline_df['avg_risk'],
            name='Avg Risk Score',
            mode='lines+markers',
            marker=dict(size=6, color='firebrick'),
            line=dict(width=2),
            yaxis='y2'
        ))
    
    # Update layout
    fig.update_layout(
        title='Flight Density Over Time',
        xaxis=dict(title='Time'),
        yaxis=dict(
            title='Flight Count',
            title_font=dict(color='royalblue'),
            tickfont=dict(color='royalblue')
        ),
        yaxis2=dict(
            title='Avg Risk Score',
            title_font=dict(color='firebrick'),
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
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Show data table
    with st.expander("View Timeline Data", expanded=False):
        st.dataframe(timeline_df)
        
        # Download button
        csv = timeline_df.to_csv(index=False)
        st.download_button(
            label="Download Data as CSV",
            data=csv,
            file_name="flight_density_timeline.csv",
            mime="text/csv"
        )

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
                                            max_value=120, 
                                            value=60)
    
    view_mode = st.sidebar.radio(
        "View Mode",
        ["Real-time Map", "Grid Heatmap", "Flight Metrics"]
    )
    
    # Add flight limit control to sidebar
    flight_limit = st.sidebar.slider(
        "Maximum Flights to Display", 
        min_value=1000, 
        max_value=10000, 
        value=MAX_FLIGHTS,
        step=1000
    )
    
    # Filter controls
    with st.sidebar.expander("Filters", expanded=False):
        min_altitude = st.slider("Min Altitude (m)", 0, 15000, 0)
        max_risk = st.slider("Max Risk Score", 0.0, 2.0, 2.0, 0.1)
    
    # Main content area
    st.title("Airspace Congestion Monitoring")
    
    if view_mode == "Real-time Map":
        st.header("Real-time Flight Positions")
        
        # Get Kafka consumer and fetch data
        consumer = get_kafka_consumer("flight-stream")
        if consumer:
            # ── Fetch from Kafka
            with st.spinner("Fetching flight data..."):
                messages = fetch_recent_flights(consumer, max_messages=flight_limit)

            # ── DEBUG: how many messages came in?
            st.write(f"🔍 Fetched {len(messages) if messages is not None else 'None'} messages from Kafka")
            if messages is None:
                st.error("Error fetching messages from Kafka")
                return

            # ── Always process into a DataFrame (never None)
            df = process_flight_data(messages)
            # (process_flight_data always returns a DataFrame, so no need for `or`)

            # ── Render when we have data
            if not df.empty:
                st.success(f"Displaying {len(df)} flights")

                # Apply altitude & risk filters
                if 'baro_altitude' in df.columns:
                    df = df[df['baro_altitude'] >= min_altitude]
                if 'risk_score' in df.columns:
                    df = df[df['risk_score'] <= max_risk]

                # Map rendering
                m = create_folium_flight_map(df, risk_column='risk_score')
                # In the Real-time Map section of main(), replace the folium_static call:
                if m:
                    # Make the map larger for better visibility
                    folium_static(m, width=1200, height=700)
                else:
                    display_basic_map(df)

                # Stats
                st.subheader("Flight Statistics")
                c1, c2, c3 = st.columns(3)
                with c1: st.metric("Total Flights", len(df))
                with c2:
                    if 'risk_score' in df.columns:
                        st.metric("Avg Risk Score", f"{df['risk_score'].mean():.2f}")
                with c3:
                    if 'velocity' in df.columns:
                        st.metric("Avg Speed (m/s)", f"{df['velocity'].mean():.1f}")

                # Raw data expander
                with st.expander("Raw Flight Data", expanded=False):
                    st.dataframe(df)

            else:
                st.warning("No valid flight data available")
        else:
            st.error("Could not connect to Kafka")
    
    elif view_mode == "Grid Heatmap":
        st.header("Flight Density Heatmap")
        
        # Create dedicated consumer for flight-aggregates topic
        grid_consumer = get_kafka_consumer("flight-aggregates")
        
        if grid_consumer:
            with st.spinner("Fetching grid data..."):
                grid_messages = fetch_recent_flights(grid_consumer, max_messages=flight_limit)
            
            if grid_messages:
                # Convert messages to DataFrame using the specialized grid processor
                df = process_grid_data(grid_messages)
                
                if not df.empty:
                    st.success(f"Displaying heatmap with {len(df)} grid cells")
                    
                    # Create heatmap visualization using Folium (interactive)
                    m = create_folium_grid_heatmap(df)
                    if m:
                        # Make the map larger for better visibility
                        folium_static(m, width=1200, height=750)
                    
                        # Display statistics
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Active Grid Cells", len(df))
                        with col2:
                            st.metric("Total Flights", int(df['flight_count'].sum()))
                        with col3:
                            if 'avg_risk' in df.columns:
                                st.metric("Average Risk", f"{df['avg_risk'].mean():.2f}")
                    
                        # Interactive data visualization
                        st.subheader("Interactive Grid Data")
                        
                        # Create tabs for different visualizations - removing 3D view
                        grid_tabs = st.tabs(["Scatter Plot", "Data Table"])
                        
                        with grid_tabs[0]:
                            # Create a simple matplotlib scatter plot instead of Plotly
                            st.subheader("Flight Density by Grid Cell")
                            
                            # Add a toggle to switch between visualization types
                            viz_type = st.radio(
                                "Visualization Type",
                                ["Map View", "Scatter Plot"],
                                horizontal=True
                            )
                            
                            if viz_type == "Map View":
                                # Create a Folium map (which we know works well)
                                center_lat = df['latitude'].mean()
                                center_lon = df['longitude'].mean()
                                
                                m = folium.Map(
                                    location=[center_lat, center_lon],
                                    zoom_start=4,
                                    tiles='CartoDB positron'
                                )
                                
                                # Add fullscreen control
                                Fullscreen().add_to(m)
                                
                                # Add circle markers for each grid cell
                                for _, row in df.iterrows():
                                    # Scale radius based on flight count
                                    radius = np.sqrt(row['flight_count']) * 2000
                                    
                                    # Color based on risk
                                    risk = row.get('avg_risk', 0)
                                    if risk > 1.0:
                                        color = 'red'
                                    elif risk > 0.5:
                                        color = 'orange'
                                    else:
                                        color = 'blue'
                                    
                                    # Create popup with info
                                    popup_text = f"""
                                        <b>Grid Cell</b><br>
                                        Flights: {row['flight_count']}<br>
                                        Risk: {risk:.2f}<br>
                                        Lat: {row['latitude']:.2f}, Lon: {row['longitude']:.2f}
                                    """
                                    
                                    # Add circle to map
                                    folium.Circle(
                                        location=[row['latitude'], row['longitude']],
                                        radius=radius,
                                        color=color,
                                        fill=True,
                                        fill_opacity=0.4,
                                        popup=folium.Popup(popup_text)
                                    ).add_to(m)
                                
                                # Display the map
                                folium_static(m, width=800, height=600)
                            else:
                                # Create figure and axis
                                fig, ax = plt.subplots(figsize=(10, 8))
                                
                                # Create scatter plot with size and color mapping
                                scatter = ax.scatter(
                                    df["longitude"], 
                                    df["latitude"],
                                    s=df["flight_count"] * 2,  # Scale size for better visibility
                                    c=df["avg_risk"] if "avg_risk" in df.columns else [0.5] * len(df),
                                    cmap="viridis",
                                    alpha=0.7,
                                    edgecolors='black',
                                    linewidths=0.5
                                )
                                
                                # Add colorbar
                                cbar = plt.colorbar(scatter)
                                cbar.set_label('Risk Score')
                                
                                # Set labels and title
                                ax.set_xlabel('Longitude')
                                ax.set_ylabel('Latitude')
                                ax.set_title('Flight Density by Grid Cell')
                                
                                # Add grid
                                ax.grid(True, linestyle='--', alpha=0.7)
                                
                                # Make layout tight
                                plt.tight_layout()
                                
                                # Display the plot
                                st.pyplot(fig)
                        
                        with grid_tabs[1]:
                            # Create interactive data table with filters
                            st.write("Filter and explore grid cell data:")
                            
                            # Add filters
                            col1, col2 = st.columns(2)
                            with col1:
                                min_flights = st.slider("Minimum Flights", 
                                                      min_value=int(df['flight_count'].min()), 
                                                      max_value=int(df['flight_count'].max()),
                                                      value=int(df['flight_count'].min()))
                            
                            with col2:
                                if 'avg_risk' in df.columns:
                                    min_risk = st.slider("Minimum Risk", 
                                                       min_value=float(df['avg_risk'].min()), 
                                                       max_value=float(df['avg_risk'].max()),
                                                       value=float(df['avg_risk'].min()))
                            
                            # Apply filters
                            filtered_df = df[df['flight_count'] >= min_flights]
                            if 'avg_risk' in df.columns:
                                filtered_df = filtered_df[filtered_df['avg_risk'] >= min_risk]
                            
                            # Show filtered data
                            st.dataframe(
                                filtered_df[['lat_bin', 'lon_bin', 'latitude', 'longitude', 'flight_count', 'avg_risk']]
                                .sort_values('flight_count', ascending=False),
                                height=400
                            )
                            
                            # Download button
                            csv = filtered_df.to_csv(index=False)
                            st.download_button(
                                label="Download Data as CSV",
                                data=csv,
                                file_name="grid_data.csv",
                                mime="text/csv"
                            )
                    else:
                        st.error("Failed to create heatmap visualization")
                        
                        # Try a simple scatter plot as fallback
                        st.write("Falling back to simple scatter plot visualization:")
                        fig = px.scatter(
                            df, 
                            x="longitude", 
                            y="latitude", 
                            size="flight_count",
                            color="avg_risk",
                            hover_name="flight_count",
                            size_max=25,
                            color_continuous_scale="Viridis",
                            title="Flight Density by Grid Cell"
                        )
                        st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("No valid grid data received from Kafka")
            else:
                st.warning("No grid data received from Kafka")
        else:
            st.error("Could not connect to Kafka")
    
    elif view_mode == "Flight Metrics":
        st.header("Flight Metrics Analysis")
        
        # Add tabs for real-time metrics and historical analytics
        realtime_anomalies_tab, historical_tab = st.tabs(["Real-time Anomaly Detection", "Historical Analytics"])
        
        with realtime_anomalies_tab:
            st.subheader("Real-time Anomaly Detection")
            
            # Get Kafka consumer for flight data
            consumer = get_kafka_consumer("flight-stream")
            
            if consumer:
                with st.spinner("Fetching flight data for anomaly detection..."):
                    messages = fetch_recent_flights(consumer, max_messages=flight_limit)
                
                if messages:
                    # Process flight data
                    df = process_flight_data(messages)
                    
                    if not df.empty:
                        # Group by flight (icao24)
                        flight_groups = df.groupby('icao24')
                        
                        # Summary stats
                        total_flights = len(flight_groups)
                        flights_with_anomalies = 0
                        total_anomalies = 0
                        high_severity_anomalies = 0
                        anomaly_flights = []
                        
                        # Process each flight
                        for icao24, flight_data in flight_groups:
                            # Sort by time if available
                            if 'fetch_time' in flight_data.columns:
                                flight_data = flight_data.sort_values('fetch_time')
                            
                            # Run anomaly detection
                            anomaly_results = analyze_flight_anomalies(flight_data)
                            
                            if anomaly_results and 'overall' in anomaly_results and anomaly_results['overall']['any_anomaly'].any():
                                # Get anomaly summary
                                anomaly_summary = get_anomaly_summary(anomaly_results)
                                severity = classify_anomaly_severity(anomaly_results)
                                
                                flights_with_anomalies += 1
                                total_anomalies += anomaly_summary['total_anomalies']
                                high_severity_anomalies += (severity >= 3).sum()
                                
                                # Store flight info for display
                                anomaly_flights.append({
                                    'icao24': icao24,
                                    'callsign': flight_data['callsign'].iloc[0] if 'callsign' in flight_data.columns else 'Unknown',
                                    'anomalies': anomaly_summary['total_anomalies'],
                                    'max_severity': int(severity.max()),
                                    'anomaly_percentage': anomaly_summary['anomaly_percentage']
                                })
                        
                        # Display summary metrics
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric("Total Flights", total_flights)
                        with col2:
                            st.metric("Flights with Anomalies", flights_with_anomalies)
                        with col3:
                            st.metric("Total Anomalies", total_anomalies)
                        with col4:
                            st.metric("High Severity Anomalies", high_severity_anomalies)
                        
                        # Display flights with anomalies
                        if anomaly_flights:
                            st.subheader("Flights with Detected Anomalies")
                            
                            # Convert to DataFrame for display
                            anomaly_df = pd.DataFrame(anomaly_flights)
                            
                            # Sort by severity and anomaly count
                            anomaly_df = anomaly_df.sort_values(['max_severity', 'anomalies'], ascending=[False, False])
                            
                            # Color code by severity
                            def highlight_severity(val):
                                if val == 3:
                                    return 'background-color: #ffcccc'  # Red
                                elif val == 2:
                                    return 'background-color: #ffffcc'  # Yellow
                                elif val == 1:
                                    return 'background-color: #ccffcc'  # Green
                                return ''
                            
                            # Display the table with styling
                            st.dataframe(anomaly_df.style.applymap(highlight_severity, subset=['max_severity']))
                            
                            # Allow selecting a flight for detailed analysis
                            selected_anomaly_flight = st.selectbox(
                                "Select Flight for Detailed Anomaly Analysis",
                                options=anomaly_df['icao24'].tolist(),
                                format_func=lambda x: f"{x} ({anomaly_df[anomaly_df['icao24'] == x]['callsign'].iloc[0]})"
                            )
                            
                            if selected_anomaly_flight:
                                # Get the flight data
                                flight_data = df[df['icao24'] == selected_anomaly_flight]
                                
                                if not flight_data.empty:
                                    st.subheader(f"Anomaly Analysis for Flight {selected_anomaly_flight}")
                                    
                                    # Sort by time if available
                                    if 'fetch_time' in flight_data.columns:
                                        flight_data = flight_data.sort_values('fetch_time')
                                    
                                    # Run anomaly detection again
                                    anomaly_results = analyze_flight_anomalies(flight_data)
                                    
                                    if anomaly_results:
                                        # Get severity
                                        severity = classify_anomaly_severity(anomaly_results)
                                        
                                        # Display metrics with anomaly highlighting
                                        for param in ['altitude', 'velocity', 'heading']:
                                            param_col = 'baro_altitude' if param == 'altitude' else param
                                            
                                            if param_col in flight_data.columns and param in anomaly_results:
                                                st.subheader(f"{param.title()} Anomalies")
                                                
                                                fig = px.line(flight_data.reset_index(), 
                                                             x="fetch_time" if "fetch_time" in flight_data.columns else flight_data.index, 
                                                             y=param_col, 
                                                             title=f"{param.title()} Over Time")
                                                
                                                # Add anomaly markers
                                                anomaly_points = flight_data[anomaly_results[param]['any_anomaly']]
                                                if not anomaly_points.empty:
                                                    fig.add_scatter(
                                                        x=anomaly_points["fetch_time"] if "fetch_time" in anomaly_points.columns else anomaly_points.index,
                                                        y=anomaly_points[param_col],
                                                        mode='markers',
                                                        marker=dict(size=10, color='red', symbol='x'),
                                                        name='Anomalies'
                                                    )
                                                
                                                st.plotly_chart(fig)
                                                
                                                # Show anomaly types
                                                anomaly_types = []
                                                for anomaly_type, series in anomaly_results[param].items():
                                                    if anomaly_type != 'any_anomaly' and series.any():
                                                        anomaly_types.append({
                                                            'Type': anomaly_type.replace('_', ' ').title(),
                                                            'Count': series.sum(),
                                                            'Percentage': f"{(series.sum() / len(series) * 100):.1f}%"
                                                        })
                                                
                                                if anomaly_types:
                                                    st.write(f"**{param.title()} Anomaly Types:**")
                                                    st.dataframe(pd.DataFrame(anomaly_types))
                                        
                                        # Show raw data with anomalies highlighted
                                        with st.expander("Raw Flight Data with Anomalies", expanded=False):
                                            # Add anomaly column to the data
                                            flight_data_with_anomalies = flight_data.copy()
                                            flight_data_with_anomalies['has_anomaly'] = anomaly_results['overall']['any_anomaly']
                                            flight_data_with_anomalies['anomaly_severity'] = severity
                                            
                                            # Define styling function
                                            def highlight_anomalies(row):
                                                if row['has_anomaly']:
                                                    if row['anomaly_severity'] == 3:
                                                        return ['background-color: #ffcccc'] * len(row)  # Red
                                                    elif row['anomaly_severity'] == 2:
                                                        return ['background-color: #ffffcc'] * len(row)  # Yellow
                                                    else:
                                                        return ['background-color: #ccffcc'] * len(row)  # Green
                                                return [''] * len(row)
                                            
                                            # Display styled dataframe
                                            st.dataframe(flight_data_with_anomalies.style.apply(highlight_anomalies, axis=1))
                                    else:
                                        st.info("No anomalies detected for this flight")
                        else:
                            st.info("No anomalies detected in any flights")
                    else:
                        st.warning("No valid flight data available for anomaly detection")
                else:
                    st.warning("No flight data received from Kafka")
            else:
                st.error("Could not connect to Kafka")
        
        with historical_tab:
            st.subheader("Historical Flight Data Analytics")
            
            # Check if MongoDB is available
            mongodb_available, mongodb_message = check_mongodb_connection()
            
            if mongodb_available:
                st.success(mongodb_message)
                
                # Create tabs for different analytics views
                analytics_tabs = st.tabs([
                    "Flight Density Timeline",
                    "Hourly Patterns", 
                    "Daily Patterns", 
                    "Congestion Hotspots", 
                    "Risk Anomalies"
                ])
                
                # Flight Density Timeline tab
                with analytics_tabs[0]:
                    display_flight_density_timeline()
                
                # Hourly Patterns tab
                with analytics_tabs[1]:
                    st.subheader("Hourly Flight Patterns")
                    hourly_patterns = get_hourly_patterns()
                    
                    if not hourly_patterns.empty:
                        hourly_chart = create_hourly_chart(hourly_patterns)
                        if hourly_chart:
                            st.plotly_chart(hourly_chart, use_container_width=True)
                        
                        # Show data table
                        with st.expander("View Hourly Data", expanded=False):
                            st.dataframe(hourly_patterns)
                    else:
                        st.info("No hourly pattern data available")
                
                # Daily Patterns tab
                with analytics_tabs[2]:
                    st.subheader("Daily Flight Patterns")
                    daily_patterns = get_daily_patterns()
                    
                    if not daily_patterns.empty:
                        daily_chart = create_daily_chart(daily_patterns)
                        if daily_chart:
                            st.plotly_chart(daily_chart, use_container_width=True)
                        
                        # Show data table
                        with st.expander("View Daily Data", expanded=False):
                            st.dataframe(daily_patterns)
                    else:
                        st.info("No daily pattern data available")
                
                # Congestion Hotspots tab
                with analytics_tabs[3]:
                    st.subheader("Congestion Hotspots")
                    
                    min_flights_slider = st.slider("Minimum flights per hotspot", 5, 50, 10)
                    hotspots = get_congestion_hotspots(min_flights=min_flights_slider)
                    
                    if not hotspots.empty:
                        # Create map
                        st.write("Hotspot Map")
                        hotspot_map = create_hotspot_map(hotspots)
                        if hotspot_map:
                            folium_static(hotspot_map, width=1200, height=700)
                        
                        # Show data table
                        st.write("Top Congestion Hotspots")
                        st.dataframe(
                            hotspots[['center_latitude', 'center_longitude', 'flight_count', 'avg_risk', 'avg_altitude']]
                            .sort_values('flight_count', ascending=False)
                        )
                    else:
                        st.info("No congestion hotspot data available")
                
                # Risk Anomalies tab
                with analytics_tabs[4]:
                    st.subheader("Risk Anomalies")
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        risk_threshold = st.slider("Risk threshold", 0.5, 3.0, 1.5, 0.1)
                    with col2:
                        min_anomalies = st.slider("Minimum anomalies per window", 1, 10, 3)
                    
                    anomalies = get_anomaly_windows(risk_threshold=risk_threshold, min_anomalies=min_anomalies)
                    
                    if not anomalies.empty:
                        # Create timeline chart
                        anomaly_chart = create_anomaly_chart(anomalies)
                        if anomaly_chart:
                            st.plotly_chart(anomaly_chart, use_container_width=True)
                        
                        # Show data table
                        st.write("Risk Anomaly Windows")
                        st.dataframe(
                            anomalies[['window_start', 'window_end', 'anomaly_count', 'avg_anomaly_risk', 'max_anomaly_risk']]
                            .sort_values('max_anomaly_risk', ascending=False)
                        )
                    else:
                        st.info("No risk anomaly data available with current settings")
                
                # Add section for top risky flights
                st.subheader("Top Flights by Risk Score")
                top_flights = get_top_flights_by_risk()
                
                if not top_flights.empty:
                    st.dataframe(
                        top_flights[['icao24', 'callsign', 'origin_country', 'max_risk', 'avg_risk', 'count']]
                        .sort_values('max_risk', ascending=False)
                        .head(20)
                    )
                else:
                    st.info("No flight risk data available")
            else:
                # MongoDB not available, show instructions
                st.warning(mongodb_message)
                
                st.write("""
                ### Setting up MongoDB for Historical Analysis
                
                To enable historical analytics with MongoDB:
                
                1. **Install MongoDB and pymongo**:
                   ```
                   bash scripts/setup_mongodb.sh
                   ```
                   
                2. **Generate historical data**:
                   ```
                   python data/augment_historical_data.py
                   ```
                   
                3. **Import data to MongoDB**:
                   ```
                   python data/import_to_mongodb.py --drop
                   ```
                   
                4. **Restart the dashboard**
                
                Alternatively, you can use the existing file-based historical analytics:
                """)
                
                # Add button to run analytics
                col1, col2 = st.columns([1, 3])
                with col1:
                    if st.button("Run File-Based Analytics"):
                        run_historical_analytics()
                        # Refresh the page to show new results
                        st.rerun()
                
                # Check if analytics data exists
                data_path = "data/analytics"
                analytics_data_exists = os.path.exists(data_path)
    
    # Auto-refresh logic
    if auto_refresh:
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