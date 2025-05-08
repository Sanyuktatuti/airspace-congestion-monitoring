#!/usr/bin/env python3
"""
simulated_data.py

Provides simulated flight data for testing and demonstration purposes
when a live data source is not available.
"""

import pandas as pd
import numpy as np
import datetime

def generate_flight_path(
    flight_id="SIM001",
    callsign="DEMO123",
    duration_minutes=30,
    sampling_rate_seconds=10,
    start_lat=40.7128,
    start_lon=-74.0060,
    start_altitude=10000,
    start_velocity=250,
    include_anomalies=True
):
    """
    Generate a simulated flight path with optional anomalies.
    
    Args:
        flight_id: ICAO24 identifier
        callsign: Flight callsign
        duration_minutes: Duration of the flight in minutes
        sampling_rate_seconds: Sampling rate in seconds
        start_lat: Starting latitude
        start_lon: Starting longitude
        start_altitude: Starting altitude in meters
        start_velocity: Starting velocity in m/s
        include_anomalies: Whether to include anomalies in the data
        
    Returns:
        DataFrame with simulated flight data
    """
    # Calculate number of data points
    num_points = int((duration_minutes * 60) / sampling_rate_seconds)
    
    # Create time index
    start_time = datetime.datetime.now() - datetime.timedelta(minutes=duration_minutes)
    time_index = pd.date_range(
        start=start_time, 
        periods=num_points, 
        freq=f"{sampling_rate_seconds}S"
    )
    
    # Initialize dataframe
    df = pd.DataFrame(index=time_index)
    
    # Add flight identifiers
    df['icao24'] = flight_id
    df['callsign'] = callsign
    
    # Generate smooth flight path with some randomness
    df['latitude'] = start_lat + np.cumsum(np.random.normal(0, 0.001, num_points))
    df['longitude'] = start_lon + np.cumsum(np.random.normal(0, 0.001, num_points))
    
    # Generate smooth altitude profile
    altitude_trend = np.concatenate([
        np.linspace(0, 1, num_points // 3),
        np.linspace(1, 0.8, num_points // 3),
        np.linspace(0.8, 0, num_points - 2*(num_points // 3))
    ])
    df['baro_altitude'] = start_altitude + altitude_trend * 2000 + np.random.normal(0, 50, num_points)
    
    # Calculate vertical rate (derivative of altitude)
    df['vertical_rate'] = np.gradient(df['baro_altitude'].values) / sampling_rate_seconds
    
    # Generate velocity with some variations
    velocity_trend = np.concatenate([
        np.linspace(1, 1.1, num_points // 4),
        np.linspace(1.1, 0.9, num_points // 2),
        np.linspace(0.9, 1, num_points - 3*(num_points // 4))
    ])
    df['velocity'] = start_velocity * velocity_trend + np.random.normal(0, 5, num_points)
    
    # Generate heading (direction of travel)
    heading_base = np.degrees(np.arctan2(
        np.gradient(df['longitude'].values),
        np.gradient(df['latitude'].values)
    ))
    df['heading'] = (heading_base + 360) % 360 + np.random.normal(0, 2, num_points)
    
    # Add a simple risk score
    df['avg_risk'] = 0.2 + 0.1 * np.sin(np.linspace(0, 6*np.pi, num_points)) + np.random.normal(0, 0.05, num_points)
    
    # Add anomalies if requested
    if include_anomalies:
        # Add more anomalies and make them more extreme
        
        # Add sudden altitude drop (more extreme)
        anomaly_idx = num_points // 5
        df.iloc[anomaly_idx:anomaly_idx+5, df.columns.get_indexer(['baro_altitude'])] -= 800
        
        # Add sudden altitude spike
        anomaly_idx = num_points // 3
        df.iloc[anomaly_idx:anomaly_idx+3, df.columns.get_indexer(['baro_altitude'])] += 1000
        
        # Add sudden velocity spike
        anomaly_idx = num_points // 2
        df.iloc[anomaly_idx:anomaly_idx+4, df.columns.get_indexer(['velocity'])] += 80
        
        # Add sudden velocity drop
        anomaly_idx = 2 * num_points // 3
        df.iloc[anomaly_idx:anomaly_idx+4, df.columns.get_indexer(['velocity'])] -= 70
        
        # Add sudden heading change
        anomaly_idx = 3 * num_points // 4
        df.iloc[anomaly_idx:anomaly_idx+6, df.columns.get_indexer(['heading'])] += 60
        
        # Add statistical outlier
        anomaly_idx = 4 * num_points // 5
        df.iloc[anomaly_idx, df.columns.get_indexer(['baro_altitude'])] = df['baro_altitude'].iloc[anomaly_idx] + 1500
        
        # Add inconsistency between vertical rate and altitude
        anomaly_idx = 9 * num_points // 10
        df.iloc[anomaly_idx:anomaly_idx+3, df.columns.get_indexer(['vertical_rate'])] = 15  # Strong positive vertical rate
        df.iloc[anomaly_idx:anomaly_idx+3, df.columns.get_indexer(['baro_altitude'])] -= 200  # But altitude decreases
    
    return df

def get_simulated_flights(num_flights=5, include_anomalies=True):
    """
    Generate simulated data for multiple flights.
    
    Args:
        num_flights: Number of flights to generate
        include_anomalies: Whether to include anomalies
        
    Returns:
        DataFrame with simulated flight data
    """
    all_flights = []
    
    # Generate flights with different parameters
    for i in range(num_flights):
        flight_id = f"SIM{i+1:03d}"
        callsign = f"DEMO{i+1:03d}"
        
        # Randomize starting positions
        start_lat = 40.7128 + np.random.uniform(-5, 5)
        start_lon = -74.0060 + np.random.uniform(-5, 5)
        start_altitude = np.random.uniform(8000, 12000)
        start_velocity = np.random.uniform(220, 280)
        
        # Only include anomalies in some flights
        has_anomalies = include_anomalies and (i % 2 == 0)
        
        flight_df = generate_flight_path(
            flight_id=flight_id,
            callsign=callsign,
            start_lat=start_lat,
            start_lon=start_lon,
            start_altitude=start_altitude,
            start_velocity=start_velocity,
            include_anomalies=has_anomalies
        )
        
        all_flights.append(flight_df)
    
    # Combine all flights
    if all_flights:
        return pd.concat(all_flights)
    else:
        return pd.DataFrame()

if __name__ == "__main__":
    # Test the data generation
    df = get_simulated_flights(num_flights=3)
    print(df.head())
    print(f"Generated {len(df)} data points for {df['icao24'].nunique()} flights") 