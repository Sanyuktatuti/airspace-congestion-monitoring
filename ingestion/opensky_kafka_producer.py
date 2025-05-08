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
import os
import random
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configure logging
def setup_logging():
    logging.basicConfig(
        format='%(asctime)s %(levelname)s: %(message)s',
        level=logging.INFO
    )
    return logging.getLogger("opensky-producer")

# Load static aircraft metadata from CSV into a dict
def load_metadata(csv_path, logger):
    metadata = {}
    try:
        with open(csv_path, newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                metadata[row['icao24'].strip().lower()] = row
        logger.info(f"Loaded metadata for {len(metadata)} aircraft.")
    except FileNotFoundError:
        logger.error(f"Metadata file not found: {csv_path}")
        logger.warning("Continuing without aircraft metadata")
    except Exception as e:
        logger.error(f"Error loading metadata: {e}")
        logger.warning("Continuing without aircraft metadata")
    return metadata

# Create a session with retry capability
def create_request_session():
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session

# Fetch live state vectors from OpenSky REST API with retries and rate limiting
def fetch_states(session, username=None, password=None, logger=None):
    url = "https://opensky-network.org/api/states/all"
    auth = None
    
    if username and password:
        auth = (username, password)
        logger.info("Using authenticated OpenSky API access")
    
    try:
        resp = session.get(url, auth=auth, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return data.get("states", []), data.get("time"), None
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            logger.warning("Rate limited by OpenSky API (429 error). Backing off.")
            return [], None, 30  # Suggest 30 second backoff
        logger.error(f"HTTP Error fetching states: {e}")
        return [], None, 10
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Connection error: {e}")
        return [], None, 5
    except requests.exceptions.Timeout as e:
        logger.error(f"Timeout error: {e}")
        return [], None, 5
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching states: {e}")
        return [], None, 3
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON response: {e}")
        return [], None, 2
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return [], None, 5

# Normalize and enrich each state vector into a dict
def enrich_state(state, metadata, fetch_time=None):
    # these keys match the positions in the OpenSky 'state' array
    keys = [
        'icao24', 'callsign', 'origin_country', 'time_position', 'last_contact',
        'longitude', 'latitude', 'baro_altitude', 'on_ground', 'velocity',
        'true_track', 'vertical_rate', 'sensors', 'geo_altitude',
        'squawk', 'spi', 'position_source'
    ]
    
    # Create dict from state array with appropriate keys
    record = dict(zip(keys, state))
    
    # Clean and convert values
    if record['callsign']:
        record['callsign'] = record['callsign'].strip()
    if record['icao24']:
        record['icao24'] = record['icao24'].strip().lower()
    
    # Convert numeric fields from strings to appropriate types
    for field in ['time_position', 'last_contact']:
        if record[field]:
            try:
                record[field] = int(record[field])
            except (ValueError, TypeError):
                record[field] = None
    
    for field in ['longitude', 'latitude', 'baro_altitude', 'velocity', 
                 'true_track', 'vertical_rate', 'geo_altitude']:
        if record[field]:
            try:
                record[field] = float(record[field])
            except (ValueError, TypeError):
                record[field] = None
    
    # Convert boolean fields
    if record['on_ground']:
        record['on_ground'] = bool(record['on_ground'])
    
    # stamp with current time in ms
    record['fetch_time'] = fetch_time if fetch_time else int(time.time() * 1000)
    
    # attach any static metadata (or empty dict)
    meta = metadata.get(record['icao24']) if record['icao24'] else None
    record['aircraft'] = meta if meta else {}
    
    # ── Fill missing numeric fields with zero
    for field in [
        'time_position', 'last_contact', 'longitude', 'latitude',
        'baro_altitude', 'velocity', 'true_track',
        'vertical_rate', 'geo_altitude'
    ]:
        if record.get(field) is None:
            record[field] = 0
    
    # Always ensure fetch_time has a valid timestamp
    if record.get('fetch_time') is None or record.get('fetch_time') == 0:
        record['fetch_time'] = int(time.time() * 1000)  # Current time in milliseconds
    
    # ── Fill missing string fields with empty string
    for field in ['icao24', 'callsign', 'origin_country', 'squawk']:
        if not record.get(field):
            record[field] = ""
    
    # ── Ensure booleans and enums get defaults
    record['on_ground'] = bool(record.get('on_ground', False))
    if record.get('position_source') is None:
        record['position_source'] = 0
    
    return record

def main():
    logger = setup_logging()

    # Configuration (can be moved to env vars or config file)
    METADATA_CSV      = 'data/aircraft_metadata.csv'
    KAFKA_BOOTSTRAP   = os.environ.get('KAFKA_BOOTSTRAP', 'localhost:9092')
    TOPIC             = os.environ.get('KAFKA_TOPIC', 'flight-stream')
    POLL_INTERVAL_SEC = int(os.environ.get('POLL_INTERVAL', 60))  # seconds, changed from 10 to 60
    OPENSKY_USERNAME  = os.environ.get('OPENSKY_USERNAME')
    OPENSKY_PASSWORD  = os.environ.get('OPENSKY_PASSWORD')
    
    logger.info(f"Starting OpenSky Kafka producer with poll interval of {POLL_INTERVAL_SEC}s")
    logger.info(f"Using Kafka bootstrap servers: {KAFKA_BOOTSTRAP}")
    logger.info(f"Publishing to topic: {TOPIC}")
    
    # Load aircraft metadata
    metadata = load_metadata(METADATA_CSV, logger)
    
    # Create HTTP session for OpenSky API
    session = create_request_session()

    # Configure and create Kafka producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8'),
            retries=5,
            acks='all',  # Wait for all replicas
            linger_ms=50,  # Batch messages for 50ms
            compression_type='gzip'  # Compress messages
        )
        logger.info(f"Kafka producer connected to {KAFKA_BOOTSTRAP}, topic '{TOPIC}'")
    except KafkaError as e:
        logger.error(f"Failed to connect to Kafka: {e}")
        return

    # Add jitter to avoid synchronized polling with other instances
    jitter = random.uniform(0, 1)
    time.sleep(jitter)
    
    consecutive_failures = 0
    consecutive_empty = 0
    backoff_time = POLL_INTERVAL_SEC

    try:
        while True:
            # Fetch data from OpenSky API
            start_time = time.time()
            fetch_time_ms = int(start_time * 1000)
            
            states, api_ts, suggested_backoff = fetch_states(
                session, 
                OPENSKY_USERNAME, 
                OPENSKY_PASSWORD, 
                logger
            )
            
            if suggested_backoff:
                backoff_time = suggested_backoff
                logger.info(f"Using suggested backoff time: {backoff_time}s")
            
            if not states:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    logger.warning(f"No state vectors fetched for {consecutive_empty} consecutive polls")
                    # Increase backoff if we keep getting empty results
                    backoff_time = min(60, POLL_INTERVAL_SEC * (consecutive_empty // 3))
                time.sleep(backoff_time)
                continue
            
            consecutive_empty = 0
            sent = 0
            failed = 0
            
            # Process each flight state
            for state in states:
                try:
                    # Skip states with missing crucial data
                    if not state[0] or not state[5] or not state[6]:  # icao24, lon, lat
                        continue
                        
                    rec = enrich_state(state, metadata, fetch_time_ms)
                    key = rec.get('icao24', 'unknown')
                    
                    # Send to Kafka (non-blocking)
                    producer.send(TOPIC, key=key, value=rec)
                    sent += 1
                except Exception as e:
                    logger.error(f"Error processing state {state[0] if state else 'unknown'}: {e}")
                    failed += 1
            
            # Ensure messages are sent
            producer.flush()
            
            # Log statistics
            elapsed = time.time() - start_time
            if sent:
                logger.info(f"Published {sent} messages in {elapsed:.2f}s. Fetch timestamp: {fetch_time_ms}")
                consecutive_failures = 0
            
            if failed:
                logger.warning(f"Failed to process {failed} messages")
            
            # Calculate sleep time to maintain polling interval
            sleep_time = max(0.1, POLL_INTERVAL_SEC - elapsed)
            time.sleep(sleep_time)
            
    except KeyboardInterrupt:
        logger.info("Shutdown requested, exiting...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        consecutive_failures += 1
        if consecutive_failures >= 5:
            logger.critical("Too many consecutive failures, exiting")
            raise
    finally:
        producer.close()
        logger.info("Producer closed")

if __name__ == '__main__':
    main()
