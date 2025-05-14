#!/usr/bin/env python3
"""
mongodb_utils.py

Utility functions for querying MongoDB for historical flight data analysis.
Provides functions for common aggregation patterns and queries needed by the dashboard.
"""

import os
import datetime
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv

# Try to load environment variables
load_dotenv()

# MongoDB connection settings
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB = os.environ.get("MONGO_DB", "airspace_monitoring")
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "historical_flights")

def get_mongodb_client():
    """Get MongoDB client with connection pooling"""
    try:
        client = MongoClient(MONGO_URI)
        # Test connection
        client.admin.command('ping')
        return client
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return None

def get_collection(client):
    """Get the MongoDB collection for historical flights"""
    if client is None:
        return None
    
    db = client[MONGO_DB]
    return db[MONGO_COLLECTION]

def get_date_range():
    """Get the min and max dates available in the collection"""
    client = get_mongodb_client()
    if client is None:
        return None, None
    
    collection = get_collection(client)
    if collection is None:
        client.close()
        return None, None
    
    try:
        # Get min date
        min_date_pipeline = [
            {"$sort": {"value.fetch_time": 1}},
            {"$limit": 1},
            {"$project": {"date": "$value.date"}}
        ]
        min_result = list(collection.aggregate(min_date_pipeline))
        min_date = min_result[0]["date"] if min_result else None
        
        # Get max date
        max_date_pipeline = [
            {"$sort": {"value.fetch_time": -1}},
            {"$limit": 1},
            {"$project": {"date": "$value.date"}}
        ]
        max_result = list(collection.aggregate(max_date_pipeline))
        max_date = max_result[0]["date"] if max_result else None
        
        return min_date, max_date
    finally:
        client.close()

def calculate_risk_score(velocity, vertical_rate, on_ground):
    """
    Calculate risk score using the same formula as risk_model.py:
    1. speed component: risk_speed = min(velocity / 250.0, 1.0)
       • if aircraft on ground OR velocity is NULL ⇒ 0
    2. climb component: risk_climb = min(|vertical_rate| / 10.0, 1.0)
       • if vertical_rate is NULL ⇒ 0
    total risk_score = risk_speed + risk_climb
    """
    # Initialize components
    risk_speed = 0.0
    risk_climb = 0.0
    
    # Calculate speed component
    if not on_ground and velocity is not None:
        risk_speed = min(velocity / 250.0, 1.0)
    
    # Calculate climb component
    if vertical_rate is not None:
        risk_climb = min(abs(vertical_rate) / 10.0, 1.0)
    
    return risk_speed + risk_climb

def query_flights_by_timerange(start_date, end_date, limit=1000):
    """Query flights within a specific time range"""
    client = get_mongodb_client()
    if client is None:
        return pd.DataFrame()
    
    collection = get_collection(client)
    if collection is None:
        client.close()
        return pd.DataFrame()
    
    try:
        # Convert dates to datetime objects if they're strings
        if isinstance(start_date, str):
            start_date = datetime.datetime.fromisoformat(start_date)
        if isinstance(end_date, str):
            end_date = datetime.datetime.fromisoformat(end_date)
        
        # Create query filter
        query_filter = {
            "value.fetch_time_dt": {
                "$gte": start_date,
                "$lte": end_date
            }
        }
        
        # Execute query
        results = list(collection.find(query_filter).limit(limit))
        
        # Convert to DataFrame
        if not results:
            return pd.DataFrame()
        
        # Extract the 'value' fields from each document
        flight_data = [doc["value"] for doc in results if "value" in doc]
        
        # Create DataFrame
        df = pd.DataFrame(flight_data)
        
        # Calculate risk scores for each flight
        df['risk_score'] = df.apply(
            lambda row: calculate_risk_score(
                row.get('velocity'),
                row.get('vertical_rate'),
                row.get('on_ground', False)
            ),
            axis=1
        )
        
        return df
    finally:
        client.close()

def get_hourly_patterns():
    """Get flight patterns aggregated by hour of day"""
    client = get_mongodb_client()
    if client is None:
        return pd.DataFrame()
    
    collection = get_collection(client)
    if collection is None:
        client.close()
        return pd.DataFrame()
    
    try:
        # First get all flights
        all_flights = list(collection.find())
        if not all_flights:
            return pd.DataFrame()
            
        # Convert to DataFrame - extract 'value' field
        df = pd.DataFrame([doc["value"] for doc in all_flights if "value" in doc])
        
        # Calculate risk scores
        df['risk_score'] = df.apply(
            lambda row: calculate_risk_score(
                row.get('velocity'),
                row.get('vertical_rate'),
                row.get('on_ground', False)
            ),
            axis=1
        )
        
        # Use the hour_of_day field that's already in the data
        if 'hour_of_day' not in df.columns:
            df['hour_of_day'] = pd.to_datetime(df['fetch_time_dt']).dt.hour
        
        # Group by hour
        hourly_stats = df.groupby('hour_of_day').agg({
            'icao24': 'count',  # flight count
            'risk_score': 'mean',
            'baro_altitude': 'mean',
            'velocity': 'mean'
        }).reset_index()
        
        # Rename columns
        hourly_stats.rename(columns={
            'icao24': 'flight_count',
            'risk_score': 'avg_risk',
            'baro_altitude': 'avg_altitude',
            'velocity': 'avg_velocity'
        }, inplace=True)
        
        return hourly_stats
    finally:
        client.close()

def get_daily_patterns():
    """Get flight patterns aggregated by day of week"""
    client = get_mongodb_client()
    if client is None:
        return pd.DataFrame()
    
    collection = get_collection(client)
    if collection is None:
        client.close()
        return pd.DataFrame()
    
    try:
        # First get all flights
        all_flights = list(collection.find())
        if not all_flights:
            return pd.DataFrame()
            
        # Convert to DataFrame - extract 'value' field
        df = pd.DataFrame([doc["value"] for doc in all_flights if "value" in doc])
        
        # Calculate risk scores
        df['risk_score'] = df.apply(
            lambda row: calculate_risk_score(
                row.get('velocity'),
                row.get('vertical_rate'),
                row.get('on_ground', False)
            ),
            axis=1
        )
        
        # Use the day_of_week field that's already in the data
        if 'day_of_week' not in df.columns:
            df['day_of_week'] = pd.to_datetime(df['fetch_time_dt']).dt.dayofweek + 1
        
        # Group by day
        daily_stats = df.groupby('day_of_week').agg({
            'icao24': 'count',  # flight count
            'risk_score': 'mean',
            'baro_altitude': 'mean',
            'velocity': 'mean'
        }).reset_index()
        
        # Rename columns
        daily_stats.rename(columns={
            'icao24': 'flight_count',
            'risk_score': 'avg_risk',
            'baro_altitude': 'avg_altitude',
            'velocity': 'avg_velocity'
        }, inplace=True)
        
        return daily_stats
    finally:
        client.close()

def get_congestion_hotspots(min_flights=10):
    """Get geographical hotspots with high flight density"""
    client = get_mongodb_client()
    if client is None:
        return pd.DataFrame()
    
    collection = get_collection(client)
    if collection is None:
        client.close()
        return pd.DataFrame()
    
    try:
        # Get all flights
        all_flights = list(collection.find())
        if not all_flights:
            return pd.DataFrame()
            
        # Convert to DataFrame - extract 'value' field
        df = pd.DataFrame([doc["value"] for doc in all_flights if "value" in doc])
        
        # Calculate risk scores
        df['risk_score'] = df.apply(
            lambda row: calculate_risk_score(
                row.get('velocity'),
                row.get('vertical_rate'),
                row.get('on_ground', False)
            ),
            axis=1
        )
        
        # Create grid cells by rounding coordinates
        df['grid_lat'] = df['latitude'].round(1)
        df['grid_lon'] = df['longitude'].round(1)
        
        # Group by grid cell
        hotspots = df.groupby(['grid_lat', 'grid_lon']).agg({
            'icao24': 'count',  # flight count
            'risk_score': 'mean',
            'baro_altitude': 'mean'
        }).reset_index()
        
        # Rename columns
        hotspots.rename(columns={
            'grid_lat': 'center_latitude',
            'grid_lon': 'center_longitude',
            'icao24': 'flight_count',
            'risk_score': 'avg_risk',
            'baro_altitude': 'avg_altitude'
        }, inplace=True)
        
        # Filter for minimum flight count
        hotspots = hotspots[hotspots['flight_count'] >= min_flights]
        
        # Sort by flight count descending
        hotspots = hotspots.sort_values('flight_count', ascending=False)
        
        return hotspots
    finally:
        client.close()

def get_flight_density_timeline(interval_minutes=30):
    """Get flight density over time with specified interval"""
    client = get_mongodb_client()
    if client is None:
        return pd.DataFrame()
    
    collection = get_collection(client)
    if collection is None:
        client.close()
        return pd.DataFrame()
    
    try:
        # Get all flights
        all_flights = list(collection.find())
        if not all_flights:
            return pd.DataFrame()
            
        # Convert to DataFrame - extract 'value' field
        df = pd.DataFrame([doc["value"] for doc in all_flights if "value" in doc])
        
        # Calculate risk scores
        df['risk_score'] = df.apply(
            lambda row: calculate_risk_score(
                row.get('velocity'),
                row.get('vertical_rate'),
                row.get('on_ground', False)
            ),
            axis=1
        )
        
        # Create time buckets
        df['time_bucket'] = pd.to_datetime(df['fetch_time_dt']).dt.floor(f'{interval_minutes}min')
        
        # Group by time bucket
        timeline = df.groupby('time_bucket').agg({
            'icao24': 'count',  # flight count
            'risk_score': 'mean',
            'baro_altitude': 'mean',
            'velocity': 'mean'
        }).reset_index()
        
        # Rename columns
        timeline.rename(columns={
            'time_bucket': 'timestamp',
            'icao24': 'flight_count',
            'risk_score': 'avg_risk'
        }, inplace=True)
        
        return timeline
    finally:
        client.close()

def get_anomaly_windows(risk_threshold=1.5, min_anomalies=3):
    """Find time windows with anomalous risk scores"""
    client = get_mongodb_client()
    if client is None:
        return pd.DataFrame()
    
    collection = get_collection(client)
    if collection is None:
        client.close()
        return pd.DataFrame()
    
    try:
        # Get all flights
        all_flights = list(collection.find())
        if not all_flights:
            return pd.DataFrame()
            
        # Convert to DataFrame - extract 'value' field
        df = pd.DataFrame([doc["value"] for doc in all_flights if "value" in doc])
        
        # Ensure fetch_time_dt is datetime
        df['fetch_time_dt'] = pd.to_datetime(df['fetch_time_dt'])
        
        # Calculate risk scores
        df['risk_score'] = df.apply(
            lambda row: calculate_risk_score(
                row.get('velocity'),
                row.get('vertical_rate'),
                row.get('on_ground', False)
            ),
            axis=1
        )
        
        # Sort by time
        df = df.sort_values('fetch_time_dt')
        
        # Find flights with high risk scores
        high_risk_flights = df[df['risk_score'] >= risk_threshold]
        
        if high_risk_flights.empty:
            return pd.DataFrame()
        
        # Group into 15-minute windows
        high_risk_flights['window'] = high_risk_flights['fetch_time_dt'].dt.floor('15min')
        
        # Group by window
        windows = high_risk_flights.groupby('window').agg({
            'risk_score': ['count', 'mean', 'max']
        }).reset_index()
        
        # Flatten column names
        windows.columns = ['window_start', 'anomaly_count', 'avg_anomaly_risk', 'max_anomaly_risk']
        
        # Add window end (15 minutes after start)
        windows['window_end'] = windows['window_start'] + pd.Timedelta(minutes=15)
        
        # Filter for minimum anomalies
        windows = windows[windows['anomaly_count'] >= min_anomalies]
        
        # Convert timestamps to strings in ISO format
        windows['window_start'] = windows['window_start'].astype(str)
        windows['window_end'] = windows['window_end'].astype(str)
        
        return windows
    finally:
        client.close()

def get_flight_timeline_data(icao24, start_date=None, end_date=None):
    """Get timeline data for a specific flight"""
    client = get_mongodb_client()
    if client is None:
        return pd.DataFrame()
    
    collection = get_collection(client)
    if collection is None:
        client.close()
        return pd.DataFrame()
    
    try:
        # Build query
        query = {"value.icao24": icao24}
        
        if start_date and end_date:
            query["value.fetch_time_dt"] = {
                "$gte": start_date,
                "$lte": end_date
            }
        
        # Execute query
        results = list(collection.find(query).sort("value.fetch_time_dt", 1))
        
        # Convert to DataFrame
        if not results:
            return pd.DataFrame()
        
        # Extract the 'value' fields from each document
        flight_data = [doc["value"] for doc in results if "value" in doc]
        
        # Create DataFrame
        df = pd.DataFrame(flight_data)
        
        return df
    finally:
        client.close()

def get_top_flights_by_risk():
    """Get top flights by risk score"""
    client = get_mongodb_client()
    if client is None:
        return pd.DataFrame()
    
    collection = get_collection(client)
    if collection is None:
        client.close()
        return pd.DataFrame()
    
    try:
        # Get all flights
        all_flights = list(collection.find())
        if not all_flights:
            return pd.DataFrame()
            
        # Convert to DataFrame - extract 'value' field
        df = pd.DataFrame([doc["value"] for doc in all_flights if "value" in doc])
        
        # Calculate risk scores
        df['risk_score'] = df.apply(
            lambda row: calculate_risk_score(
                row.get('velocity'),
                row.get('vertical_rate'),
                row.get('on_ground', False)
            ),
            axis=1
        )
        
        # Group by flight (icao24)
        top_flights = df.groupby(['icao24', 'callsign', 'origin_country']).agg({
            'risk_score': ['mean', 'max', 'count'],
            'baro_altitude': 'mean',
            'velocity': 'mean'
        }).reset_index()
        
        # Flatten column names
        top_flights.columns = ['icao24', 'callsign', 'origin_country', 'avg_risk', 'max_risk', 'count', 'avg_altitude', 'avg_velocity']
        
        # Sort by max risk descending
        top_flights = top_flights.sort_values('max_risk', ascending=False)
        
        # Take top 100
        return top_flights.head(100)
    finally:
        client.close() 