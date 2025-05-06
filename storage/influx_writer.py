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
    
    # Create InfluxDB client
    try:
        influx_client = InfluxDBClient(
            url=args.influx_url,
            token=args.influx_token,
            org=args.influx_org
        )
        
        # Create write API
        write_api = influx_client.write_api(write_options=ASYNCHRONOUS)
        
        # Check if bucket exists, create if not
        buckets_api = influx_client.buckets_api()
        bucket = buckets_api.find_bucket_by_name(args.influx_bucket)
        
        if not bucket:
            logger.info(f"Bucket '{args.influx_bucket}' not found, creating it")
            org_id = influx_client.organizations_api().find_organizations()[0].id
            buckets_api.create_bucket(bucket_name=args.influx_bucket, org_id=org_id)
        
        logger.info(f"Connected to InfluxDB")
    except Exception as e:
        logger.error(f"Failed to connect to InfluxDB: {e}")
        sys.exit(1)
    
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
                for msg in consumer:
                    try:
                        # Convert message to InfluxDB point
                        point = convert_to_influx_point(msg.value, msg.topic)
                        
                        if point:
                            # Add to buffer
                            points_buffer.append(point)
                            
                            # Check if we should write
                            if len(points_buffer) >= args.batch_size:
                                write_api.write(bucket=args.influx_bucket, record=points_buffer)
                                logger.info(f"Wrote {len(points_buffer)} points to InfluxDB")
                                points_buffer = []
                                last_write_time = time.time()
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
                
                # Check if we've exceeded time window
                current_time = time.time()
                if current_time - last_write_time > args.time_window and points_buffer:
                    # Write buffer to InfluxDB
                    write_api.write(bucket=args.influx_bucket, record=points_buffer)
                    logger.info(f"Wrote {len(points_buffer)} points to InfluxDB (time window)")
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
            write_api.write(bucket=args.influx_bucket, record=points_buffer)
            logger.info(f"Wrote final {len(points_buffer)} points to InfluxDB")
    
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        # Clean up
        if 'consumer' in locals():
            consumer.close()
            logger.info("Kafka consumer closed")
        
        if 'write_api' in locals():
            write_api.close()
            logger.info("InfluxDB write API closed")
        
        if 'influx_client' in locals():
            influx_client.close()
            logger.info("InfluxDB client closed")

if __name__ == "__main__":
    main()
