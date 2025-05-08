#!/usr/bin/env python3
"""
influx_writer.py

Consumes data from Kafka topics and writes it to InfluxDB for time series visualization.
Optimized for metrics and aggregation data.
"""

import os
import sys
import json
import time
import logging
import argparse
from datetime import datetime
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS

def setup_logging():
    """Configure logging"""
    logging.basicConfig(
        format='%(asctime)s %(levelname)s: %(message)s',
        level=logging.INFO
    )
    return logging.getLogger("influx-writer")

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Write Kafka data to InfluxDB')
    
    parser.add_argument(
        '--bootstrap-servers', '-b', 
        type=str, 
        default=os.environ.get('KAFKA_BOOTSTRAP', 'localhost:9092'),
        help='Kafka bootstrap servers (default: localhost:9092)'
    )
    
    parser.add_argument(
        '--topics', '-t', 
        type=str, 
        default='flight-metrics,flight-aggregates',
        help='Comma-separated list of Kafka topics to consume (default: flight-metrics,flight-aggregates)'
    )
    
    parser.add_argument(
        '--influx-url', 
        type=str, 
        default=os.environ.get('INFLUXDB_URL', 'http://localhost:8086'),
        help='InfluxDB URL (default: http://localhost:8086)'
    )
    
    parser.add_argument(
        '--influx-token', 
        type=str, 
        default=os.environ.get('INFLUXDB_TOKEN', ''),
        help='InfluxDB token (default: empty string for no auth)'
    )
    
    parser.add_argument(
        '--influx-org', 
        type=str, 
        default=os.environ.get('INFLUXDB_ORG', 'airspace'),
        help='InfluxDB organization (default: airspace)'
    )
    
    parser.add_argument(
        '--influx-bucket', 
        type=str, 
        default=os.environ.get('INFLUXDB_BUCKET', 'flight_metrics'),
        help='InfluxDB bucket (default: flight_metrics)'
    )
    
    parser.add_argument(
        '--batch-size', '-s', 
        type=int, 
        default=100,
        help='Number of points to batch before writing to InfluxDB (default: 100)'
    )
    
    parser.add_argument(
        '--time-window', '-w', 
        type=int, 
        default=5,
        help='Max time window in seconds before writing to InfluxDB (default: 5)'
    )
    
    return parser.parse_args()

def convert_to_influx_point(record, topic, timestamp_field='fetch_time'):
    """Convert a Kafka record to an InfluxDB point"""
    try:
        if topic == 'flight-metrics':
            return flight_metrics_to_point(record, topic)
        elif topic == 'flight-aggregates':
            return grid_aggregates_to_point(record, topic)
        else:
            # Generic conversion
            return generic_to_point(record, topic, timestamp_field)
    except Exception as e:
        logging.error(f"Error converting record to InfluxDB point: {e}")
        return None

def generic_to_point(record, topic, timestamp_field='fetch_time'):
    """Generic conversion from JSON to InfluxDB point"""
    # Extract timestamp, default to current time if not found
    timestamp = record.get(timestamp_field, int(time.time() * 1000))
    
    # Create the point
    point = Point(topic)
    
    # Set timestamp
    point.time(timestamp, WritePrecision.MS)
    
    # Extract measurements and tags
    tags = {}
    fields = {}
    
    for key, value in record.items():
        # Skip timestamp field to avoid duplication
        if key == timestamp_field:
            continue
        
        # String values become tags, numeric values become fields
        if isinstance(value, str):
            tags[key] = value[:255]  # InfluxDB has tag length limits
        elif isinstance(value, (int, float, bool)) and value is not None:
            fields[key] = value
        
    # Add all tags
    for tag_key, tag_value in tags.items():
        point.tag(tag_key, tag_value)
    
    # Add all fields (must have at least one)
    if not fields:
        fields['_exists'] = 1  # Default field if none found
    
    for field_key, field_value in fields.items():
        point.field(field_key, field_value)
    
    return point

def flight_metrics_to_point(record, topic):
    """Convert flight metrics to InfluxDB point format"""
    icao24 = record.get('icao24', 'unknown')
    timestamp = record.get('fetch_time', int(time.time() * 1000))
    
    point = Point("flight_metrics")
    point.time(timestamp, WritePrecision.MS)
    
    # Set tags (dimensions for querying)
    point.tag("icao24", icao24)
    
    # Set fields (actual measurements)
    for key, value in record.items():
        if key in ('icao24', 'fetch_time'):
            continue  # Skip tags/timestamp
        
        if isinstance(value, (int, float)) and value is not None:
            point.field(key, value)
    
    return point

def grid_aggregates_to_point(record, topic):
    """Convert spatial grid aggregates to InfluxDB point format"""
    # Parse timestamp from window if available
    timestamp = None
    window_start = record.get('window_start')
    if window_start:
        # Parse ISO format or timestamp
        if isinstance(window_start, str):
            try:
                timestamp = int(datetime.fromisoformat(window_start.replace('Z', '+00:00')).timestamp() * 1000)
            except ValueError:
                pass
        elif isinstance(window_start, (int, float)):
            timestamp = int(window_start)
    
    # Default to current time if parsing failed
    if not timestamp:
        timestamp = int(time.time() * 1000)
    
    # Create grid cell ID from lat/lon bins
    lat_bin = record.get('lat_bin')
    lon_bin = record.get('lon_bin')
    grid_id = f"{lat_bin}_{lon_bin}" if lat_bin is not None and lon_bin is not None else "unknown"
    
    point = Point("grid_metrics")
    point.time(timestamp, WritePrecision.MS)
    
    # Set tags
    point.tag("grid_id", grid_id)
    if lat_bin is not None:
        point.tag("lat_bin", str(lat_bin))
    if lon_bin is not None:
        point.tag("lon_bin", str(lon_bin))
    
    # Set fields
    for key, value in record.items():
        if key in ('lat_bin', 'lon_bin', 'window_start', 'window_end'):
            continue  # Skip tags/timestamp
        
        if isinstance(value, (int, float)) and value is not None:
            point.field(key, value)
    
    return point

def main():
    """Main function"""
    args = parse_args()
    logger = setup_logging()
    
    # Parse topics
    topics = [t.strip() for t in args.topics.split(',')]
    
    logger.info(f"Starting InfluxDB writer")
    logger.info(f"Kafka bootstrap servers: {args.bootstrap_servers}")
    logger.info(f"Consuming topics: {topics}")
    logger.info(f"InfluxDB URL: {args.influx_url}")
    logger.info(f"InfluxDB organization: {args.influx_org}")
    logger.info(f"InfluxDB bucket: {args.influx_bucket}")
    logger.info(f"Batch size: {args.batch_size} points")
    logger.info(f"Time window: {args.time_window} seconds")
    
    # Try multiple token formats
    token_variations = [
        args.influx_token,  # Original token from args or .env
        args.influx_token.strip('"'),  # Remove quotes if present
        args.influx_token.split()[0] if ' ' in args.influx_token else args.influx_token,  # First token if space-separated
    ]
    
    # Create InfluxDB client - try each token format
    influx_client = None
    last_error = None
    
    for token in token_variations:
        try:
            logger.info(f"Trying to connect to InfluxDB with token format...")
            influx_client = InfluxDBClient(
                url=args.influx_url,
                token=token,
                org=args.influx_org
            )
            
            # Test the connection
            health = influx_client.health()
            logger.info(f"InfluxDB connection successful! Status: {health.status}")
            break  # Found working token format
            
        except Exception as e:
            last_error = e
            logger.warning(f"Token format failed: {e}")
            continue  # Try next token format
    
    if not influx_client:
        logger.error(f"All token formats failed. Last error: {last_error}")
        # Create mock writer for development/testing
        logger.warning("Starting in MOCK MODE - data will be logged but not saved to InfluxDB")
        USE_MOCK_WRITER = True
    else:
        USE_MOCK_WRITER = False
        try:
            # Create write API
            write_api = influx_client.write_api(write_options=ASYNCHRONOUS)
            
            # Check if bucket exists, create if not
            try:
                buckets_api = influx_client.buckets_api()
                bucket = buckets_api.find_bucket_by_name(args.influx_bucket)
                
                if not bucket:
                    logger.info(f"Bucket '{args.influx_bucket}' not found, creating it")
                    org_id = influx_client.organizations_api().find_organizations()[0].id
                    buckets_api.create_bucket(bucket_name=args.influx_bucket, org_id=org_id)
            except Exception as bucket_error:
                logger.warning(f"Error checking/creating bucket: {bucket_error}")
                # Continue anyway
            
            logger.info(f"Connected to InfluxDB")
        except Exception as e:
            logger.error(f"Failed to initialize InfluxDB client: {e}")
            logger.warning("Starting in MOCK MODE - data will be logged but not saved to InfluxDB")
            USE_MOCK_WRITER = True
    
    # Connect to Kafka
    try:
        consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=args.bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='influx-writer',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=1000  # 1s timeout for empty poll
        )
        
        logger.info(f"Connected to Kafka")
        
        # Initialize point buffer
        points_buffer = []
        
        # Track last write time
        last_write_time = time.time()
        
        # Main consumption loop
        while True:
            try:
                # Poll for messages
                message_count = 0
                for msg in consumer:
                    message_count += 1
                    try:
                        # Convert message to InfluxDB point
                        point = convert_to_influx_point(msg.value, msg.topic)
                        
                        if point:
                            # Add to buffer
                            points_buffer.append(point)
                            
                            # Check if we should write
                            if len(points_buffer) >= args.batch_size:
                                if not USE_MOCK_WRITER:
                                    write_api.write(bucket=args.influx_bucket, record=points_buffer)
                                    logger.info(f"Wrote {len(points_buffer)} points to InfluxDB")
                                else:
                                    logger.info(f"MOCK: Would write {len(points_buffer)} points to InfluxDB")
                                
                                points_buffer = []
                                last_write_time = time.time()
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
                
                if message_count == 0:
                    logger.debug("No messages received in this poll interval")
                
                # Check if we've exceeded time window
                current_time = time.time()
                if current_time - last_write_time > args.time_window and points_buffer:
                    # Write buffer to InfluxDB
                    if not USE_MOCK_WRITER:
                        write_api.write(bucket=args.influx_bucket, record=points_buffer)
                        logger.info(f"Wrote {len(points_buffer)} points to InfluxDB (time window)")
                    else:
                        logger.info(f"MOCK: Would write {len(points_buffer)} points to InfluxDB (time window)")
                    
                    points_buffer = []
                    last_write_time = current_time
                
                # Small sleep to avoid spinning
                time.sleep(0.1)
                
            except KeyboardInterrupt:
                logger.info("Interrupted by user")
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                time.sleep(1)  # Avoid rapid retries
        
        # Write any remaining points
        if points_buffer:
            if not USE_MOCK_WRITER:
                write_api.write(bucket=args.influx_bucket, record=points_buffer)
                logger.info(f"Wrote final {len(points_buffer)} points to InfluxDB")
            else:
                logger.info(f"MOCK: Would write final {len(points_buffer)} points to InfluxDB")
    
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        # Clean up
        if 'consumer' in locals():
            consumer.close()
            logger.info("Kafka consumer closed")
        
        if 'write_api' in locals() and not USE_MOCK_WRITER:
            write_api.close()
            logger.info("InfluxDB write API closed")
        
        if influx_client and not USE_MOCK_WRITER:
            influx_client.close()
            logger.info("InfluxDB client closed")

if __name__ == "__main__":
    main()
