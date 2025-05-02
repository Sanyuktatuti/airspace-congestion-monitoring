#!/usr/bin/env python3
"""
opensky_kafka_producer.py

Fetches real-time state vector data from the OpenSky API, enriches
with static aircraft metadata, and publishes messages to a Kafka topic.
"""
import time
import json
import csv
import logging
import requests
from kafka import KafkaProducer

# Configure logging
def setup_logging():
    logging.basicConfig(
        format='%(asctime)s %(levelname)s: %(message)s',
        level=logging.INFO
    )

# Load static aircraft metadata from CSV into a dict
def load_metadata(csv_path):
    metadata = {}
    with open(csv_path, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            metadata[row['icao24'].strip().lower()] = row
    logging.info(f"Loaded metadata for {len(metadata)} aircraft.")
    return metadata

# Fetch live state vectors from OpenSky REST API
def fetch_states(username=None, password=None):
    url = 'https://opensky-network.org/api/states/all'
    auth = (username, password) if username and password else None
    try:
        response = requests.get(url, auth=auth, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data.get('states', []), data.get('time')
    except requests.RequestException as e:
        logging.error(f"Error fetching states: {e}")
        return [], None

# Normalize and enrich each state vector into a dict
def enrich_state(state, metadata):
    # these keys match the positions in the OpenSky 'state' array
    keys = [
        'icao24', 'callsign', 'origin_country', 'time_position', 'last_contact',
        'longitude', 'latitude', 'baro_altitude', 'on_ground', 'velocity',
        'true_track', 'vertical_rate', 'sensors', 'geo_altitude',
        'squawk', 'spi', 'position_source'
    ]
    record = dict(zip(keys, state))
    # stamp with current time in ms
    record['fetch_time'] = int(time.time() * 1000)
    # attach any static metadata (or empty dict)
    meta = metadata.get(record['icao24'].strip().lower())
    record['aircraft'] = meta if meta else {}
    return record

def main():
    setup_logging()

    # Configuration
    METADATA_CSV      = 'data/aircraft_metadata.csv'
    KAFKA_BOOTSTRAP   = 'localhost:9092'
    TOPIC             = 'flight-stream'
    POLL_INTERVAL_SEC = 10  # seconds

    metadata = load_metadata(METADATA_CSV)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8'),
        retries=5,
        linger_ms=50
    )
    logging.info(f"Kafka producer connected to {KAFKA_BOOTSTRAP}, topic '{TOPIC}'")

    try:
        while True:
            states, api_ts = fetch_states()
            if not states:
                logging.warning("No state vectors fetched.")
            sent = 0
            for state in states:
                rec = enrich_state(state, metadata)
                key = rec.get('icao24', 'unknown')
                producer.send(TOPIC, key=key, value=rec)
                sent += 1
            producer.flush()
            if sent:
                logging.info(f"Published {sent} messages. latest fetch_time={rec['fetch_time']}")
            time.sleep(POLL_INTERVAL_SEC)
    except KeyboardInterrupt:
        logging.info("Shutdown requested, exiting…")
    finally:
        producer.close()

if __name__ == '__main__':
    main()