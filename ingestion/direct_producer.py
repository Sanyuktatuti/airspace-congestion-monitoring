#!/usr/bin/env python3
import json
import time
from datetime import datetime
import requests
from confluent_kafka import Producer
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'opensky-direct-producer'
}

# Create Kafka producer
producer = Producer(kafka_config)

def fetch_flight_data():
    """Fetch flight data from OpenSky Network API"""
    url = "https://opensky-network.org/api/states/all"
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            data = response.json()
            return data.get('states', [])
        else:
            print(f"Error fetching data: {response.status_code}")
            return []
    except Exception as e:
        print(f"Exception while fetching data: {e}")
        return []

def process_flight_data(states):
    """Process and format flight data"""
    processed_data = []
    if not states:
        return processed_data
    
    for state in states:
        if state[5] and state[6]:  # Only include if has position
            flight_data = {
                'icao24': state[0],
                'callsign': state[1].strip() if state[1] else None,
                'origin_country': state[2],
                'time_position': state[3],
                'last_contact': state[4],
                'longitude': state[5],
                'latitude': state[6],
                'baro_altitude': state[7],
                'on_ground': state[8],
                'velocity': state[9],
                'true_track': state[10],
                'vertical_rate': state[11],
                'sensors': state[12],
                'geo_altitude': state[13],
                'squawk': state[14],
                'spi': state[15],
                'position_source': state[16],
                'fetch_time': int(time.time() * 1000),
                'aircraft': {}  # Placeholder for aircraft info
            }
            processed_data.append(flight_data)
    return processed_data

def delivery_report(err, msg):
    """Kafka delivery report callback"""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def main():
    """Main loop to fetch and publish flight data"""
    print("Starting OpenSky Direct Producer...")
    
    while True:
        try:
            # Fetch flight data
            raw_data = fetch_flight_data()
            
            # Process the data
            flights = process_flight_data(raw_data)
            
            # Publish each flight to Kafka
            for flight in flights:
                try:
                    producer.produce(
                        'flight-stream',
                        key=flight['icao24'],
                        value=json.dumps(flight),
                        callback=delivery_report
                    )
                except Exception as e:
                    print(f"Error producing message: {e}")
            
            # Flush producer
            producer.flush()
            
            print(f"Published {len(flights)} messages at {datetime.now()}")
            
            # Wait before next fetch
            time.sleep(15)  # 15 second delay between fetches
            
        except Exception as e:
            print(f"Error in main loop: {e}")
            time.sleep(5)  # Wait before retry on error

if __name__ == "__main__":
    main() 