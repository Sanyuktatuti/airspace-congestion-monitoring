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
        # Aggregate by hour of day
        pipeline = [
            {"$group": {
                "_id": "$value.hour_of_day",
                "flight_count": {"$sum": 1},
                "avg_risk": {"$avg": "$value.risk_score"},
                "avg_altitude": {"$avg": "$value.baro_altitude"},
                "avg_velocity": {"$avg": "$value.velocity"}
            }},
            {"$sort": {"_id": 1}}
        ]
        
        results = list(collection.aggregate(pipeline))
        
        # Convert to DataFrame
        if not results:
            return pd.DataFrame()
        
        df = pd.DataFrame(results)
        df.rename(columns={"_id": "hour_of_day"}, inplace=True)
        
        return df
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
        # Aggregate by day of week
        pipeline = [
            {"$group": {
                "_id": "$value.day_of_week",
                "flight_count": {"$sum": 1},
                "avg_risk": {"$avg": "$value.risk_score"},
                "avg_altitude": {"$avg": "$value.baro_altitude"},
                "avg_velocity": {"$avg": "$value.velocity"}
            }},
            {"$sort": {"_id": 1}}
        ]
        
        results = list(collection.aggregate(pipeline))
        
        # Convert to DataFrame
        if not results:
            return pd.DataFrame()
        
        df = pd.DataFrame(results)
        df.rename(columns={"_id": "day_of_week"}, inplace=True)
        
        return df
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
        # Use MongoDB's geospatial aggregation to find hotspots
        # We'll use the GeoJSON location field if available, otherwise use latitude/longitude
        pipeline = [
            # Filter out records without valid coordinates or location
            {"$match": {
                "$or": [
                    {"value.location": {"$exists": True}},
                    {
                        "value.latitude": {"$ne": None},
                        "value.longitude": {"$ne": None}
                    }
                ]
            }},
            # Project fields we need
            {"$project": {
                "location": "$value.location",
                "longitude": "$value.longitude",
                "latitude": "$value.latitude",
                "baro_altitude": "$value.baro_altitude",
                "risk_score": "$value.risk_score"
            }},
            # Create grid cells by rounding coordinates
            {"$project": {
                "grid_lat": {
                    "$cond": {
                        "if": {"$ne": ["$location", None]},
                        "then": {"$round": [{"$arrayElemAt": ["$location.coordinates", 1]}, 1]},
                        "else": {"$round": ["$latitude", 1]}
                    }
                },
                "grid_lon": {
                    "$cond": {
                        "if": {"$ne": ["$location", None]},
                        "then": {"$round": [{"$arrayElemAt": ["$location.coordinates", 0]}, 1]},
                        "else": {"$round": ["$longitude", 1]}
                    }
                },
                "baro_altitude": "$baro_altitude",
                "risk_score": "$risk_score"
            }},
            # Group by grid cell
            {"$group": {
                "_id": {
                    "lat": "$grid_lat",
                    "lon": "$grid_lon"
                },
                "flight_count": {"$sum": 1},
                "avg_altitude": {"$avg": "$baro_altitude"},
                "avg_risk": {"$avg": "$risk_score"}
            }},
            # Filter for cells with minimum flight count
            {"$match": {
                "flight_count": {"$gte": min_flights}
            }},
            # Sort by flight count descending
            {"$sort": {"flight_count": -1}}
        ]
        
        results = list(collection.aggregate(pipeline))
        
        # Convert to DataFrame
        if not results:
            return pd.DataFrame()
        
        # Flatten the _id field
        flattened_results = []
        for result in results:
            flat_result = {
                "center_latitude": result["_id"]["lat"],
                "center_longitude": result["_id"]["lon"],
                "flight_count": result["flight_count"],
                "avg_altitude": result["avg_altitude"],
                "avg_risk": result.get("avg_risk", 0)
            }
            flattened_results.append(flat_result)
        
        df = pd.DataFrame(flattened_results)
        
        return df
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
        # Create a time-based aggregation
        pipeline = [
            # Match only records with valid timestamps
            {"$match": {
                "value.fetch_time_dt": {"$ne": None}
            }},
            # Create time buckets based on interval
            {"$project": {
                "time_bucket": {
                    "$subtract": [
                        {"$toLong": "$value.fetch_time_dt"},
                        {"$mod": [
                            {"$toLong": "$value.fetch_time_dt"},
                            interval_minutes * 60 * 1000
                        ]}
                    ]
                },
                "risk_score": "$value.risk_score",
                "baro_altitude": "$value.baro_altitude",
                "velocity": "$value.velocity"
            }},
            # Group by time bucket
            {"$group": {
                "_id": "$time_bucket",
                "flight_count": {"$sum": 1},
                "avg_risk": {"$avg": "$risk_score"},
                "avg_altitude": {"$avg": "$baro_altitude"},
                "avg_velocity": {"$avg": "$velocity"}
            }},
            # Sort by time bucket
            {"$sort": {"_id": 1}}
        ]
        
        results = list(collection.aggregate(pipeline))
        
        # Convert to DataFrame
        if not results:
            return pd.DataFrame()
        
        df = pd.DataFrame(results)
        
        # Convert time bucket to datetime
        df["timestamp"] = pd.to_datetime(df["_id"], unit="ms")
        df.drop("_id", axis=1, inplace=True)
        
        return df
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
        # Find flights with high risk scores
        high_risk_query = {
            "value.risk_score": {"$gte": risk_threshold}
        }
        
        high_risk_flights = list(collection.find(high_risk_query).sort("value.fetch_time_dt", 1))
        
        if not high_risk_flights:
            return pd.DataFrame()
        
        # Extract timestamps and group into windows
        windows = []
        current_window = {
            "flights": [],
            "start_time": None,
            "end_time": None
        }
        
        # Time threshold for grouping (15 minutes)
        time_threshold = datetime.timedelta(minutes=15)
        
        for flight in high_risk_flights:
            if "value" in flight and "fetch_time_dt" in flight["value"]:
                timestamp = flight["value"]["fetch_time_dt"]
                
                if current_window["start_time"] is None:
                    # First flight in window
                    current_window["start_time"] = timestamp
                    current_window["end_time"] = timestamp
                    current_window["flights"].append(flight)
                elif timestamp - current_window["end_time"] <= time_threshold:
                    # Add to current window
                    current_window["end_time"] = timestamp
                    current_window["flights"].append(flight)
                else:
                    # Start new window if enough anomalies in current window
                    if len(current_window["flights"]) >= min_anomalies:
                        windows.append(current_window)
                    
                    # Start new window
                    current_window = {
                        "flights": [flight],
                        "start_time": timestamp,
                        "end_time": timestamp
                    }
        
        # Add last window if it has enough anomalies
        if len(current_window["flights"]) >= min_anomalies:
            windows.append(current_window)
        
        # Convert windows to DataFrame records
        window_records = []
        for window in windows:
            risk_scores = [flight["value"].get("risk_score", 0) for flight in window["flights"]]
            
            record = {
                "window_start": window["start_time"].isoformat(),
                "window_end": window["end_time"].isoformat(),
                "anomaly_count": len(window["flights"]),
                "avg_anomaly_risk": sum(risk_scores) / len(risk_scores) if risk_scores else 0,
                "max_anomaly_risk": max(risk_scores) if risk_scores else 0
            }
            window_records.append(record)
        
        return pd.DataFrame(window_records)
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
        # Aggregate by flight (icao24)
        pipeline = [
            # Match only records with risk scores
            {"$match": {
                "value.risk_score": {"$exists": True, "$ne": None}
            }},
            # Group by flight
            {"$group": {
                "_id": "$value.icao24",
                "callsign": {"$first": "$value.callsign"},
                "origin_country": {"$first": "$value.origin_country"},
                "avg_risk": {"$avg": "$value.risk_score"},
                "max_risk": {"$max": "$value.risk_score"},
                "count": {"$sum": 1},
                "avg_altitude": {"$avg": "$value.baro_altitude"},
                "avg_velocity": {"$avg": "$value.velocity"}
            }},
            # Sort by max risk descending
            {"$sort": {"max_risk": -1}},
            # Limit to top 100
            {"$limit": 100}
        ]
        
        results = list(collection.aggregate(pipeline))
        
        # Convert to DataFrame
        if not results:
            return pd.DataFrame()
        
        df = pd.DataFrame(results)
        df.rename(columns={"_id": "icao24"}, inplace=True)
        
        return df
    finally:
        client.close() 