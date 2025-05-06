#!/usr/bin/env python3
"""
hdfs_writer.py

Consumes data from Kafka topics and writes it to HDFS.
Supports both raw data preservation and processed metrics.
"""

import os
import sys
import json
import time
import logging
import argparse
from pathlib import Path
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from kafka import KafkaConsumer

def setup_logging():
    """Configure logging"""
    logging.basicConfig(
        format='%(asctime)s %(levelname)s: %(message)s',
        level=logging.INFO
    )
    return logging.getLogger("hdfs-writer")

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Write Kafka data to HDFS')
    
    parser.add_argument(
        '--bootstrap-servers', '-b', 
        type=str, 
        default=os.environ.get('KAFKA_BOOTSTRAP', 'localhost:9092'),
        help='Kafka bootstrap servers (default: localhost:9092)'
    )
    
    parser.add_argument(
        '--topics', '-t', 
        type=str, 
        default='flight-stream,flight-metrics,flight-aggregates',
        help='Comma-separated list of Kafka topics to consume (default: flight-stream,flight-metrics,flight-aggregates)'
    )
    
    parser.add_argument(
        '--output-dir', '-o', 
        type=str, 
        default='data/hdfs',
        help='Output directory for HDFS files (default: data/hdfs)'
    )
    
    parser.add_argument(
        '--batch-size', '-s', 
        type=int, 
        default=1000,
        help='Number of messages to batch before writing to HDFS (default: 1000)'
    )
    
    parser.add_argument(
        '--time-window', '-w', 
        type=int, 
        default=60,
        help='Max time window in seconds before writing to HDFS (default: 60)'
    )
    
    parser.add_argument(
        '--partitioning', '-p', 
        type=str, 
        default='hourly',
        choices=['hourly', 'daily'],
        help='Partitioning scheme for HDFS files (default: hourly)'
    )
    
    return parser.parse_args()

def get_partition_path(base_dir, topic, timestamp, scheme='hourly'):
    """Generate a partition path based on timestamp"""
    dt = datetime.fromtimestamp(timestamp / 1000.0)
    
    if scheme == 'hourly':
        partition = f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/hour={dt.hour:02d}"
    else:  # daily
        partition = f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
    
    return str(Path(base_dir) / topic / partition)

def ensure_dir(directory):
    """Ensure directory exists"""
    Path(directory).mkdir(parents=True, exist_ok=True)

def write_to_parquet(data, path, schema=None):
    """Write data to Parquet file"""
    if not data:
        return False
    
    try:
        df = pd.DataFrame(data)
        table = pa.Table.from_pandas(df, schema=schema)
        ensure_dir(str(Path(path).parent))
        pq.write_table(table, path)
        return True
    except Exception as e:
        logging.error(f"Error writing to Parquet: {e}")
        return False

def infer_schema(data):
    """Infer PyArrow schema from a list of records"""
    # Convert to DataFrame first to get column types
    df = pd.DataFrame(data)
    return pa.Schema.from_pandas(df)

def flatten_nested_json(record, prefix=''):
    """Flatten nested JSON for easier storage in columnar format"""
    items = []
    for key, value in record.items():
        new_key = f"{prefix}{key}"
        if isinstance(value, dict):
            items.extend(flatten_nested_json(value, f"{new_key}.").items())
        elif isinstance(value, list):
            # Convert lists to strings for simplicity
            items.append((new_key, json.dumps(value)))
        else:
            items.append((new_key, value))
    return dict(items)

def process_message(msg, topic_buffers, schemas, args):
    """Process a single Kafka message"""
    try:
        # Get topic and timestamp
        topic = msg.topic
        timestamp = msg.timestamp
        
        # Decode message value
        if msg.value:
            record = json.loads(msg.value.decode('utf-8'))
            
            # Flatten nested JSON for easier storage
            flat_record = flatten_nested_json(record)
            
            # Add message metadata
            flat_record['_kafka_offset'] = msg.offset
            flat_record['_kafka_timestamp'] = timestamp
            
            # Add record to buffer
            if topic not in topic_buffers:
                topic_buffers[topic] = []
            
            topic_buffers[topic].append(flat_record)
            
            # If we have a schema, validate it, otherwise infer it
            if topic not in schemas and len(topic_buffers[topic]) >= 10:
                schemas[topic] = infer_schema(topic_buffers[topic])
            
            # Check if it's time to write to HDFS
            if len(topic_buffers[topic]) >= args.batch_size:
                return topic
        
        return None
    except Exception as e:
        logging.error(f"Error processing message: {e}")
        return None

def write_topic_buffer(topic, buffer, schema, args):
    """Write a topic buffer to HDFS"""
    if not buffer:
        return

    try:
        # Get current timestamp for partition
        now = int(time.time() * 1000)
        
        # Get partition path
        partition_path = get_partition_path(args.output_dir, topic, now, args.partitioning)
        
        # Generate filename with timestamp
        filename = f"{topic}_{now}.parquet"
        path = str(Path(partition_path) / filename)
        
        # Write to HDFS
        success = write_to_parquet(buffer, path, schema)
        
        if success:
            logging.info(f"Wrote {len(buffer)} records to {path}")
        else:
            logging.error(f"Failed to write {len(buffer)} records to {path}")
    except Exception as e:
        logging.error(f"Error writing buffer for topic {topic}: {e}")

def main():
    """Main function"""
    args = parse_args()
    logger = setup_logging()
    
    # Parse topics
    topics = [t.strip() for t in args.topics.split(',')]
    
    # Validate output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Starting HDFS writer")
    logger.info(f"Kafka bootstrap servers: {args.bootstrap_servers}")
    logger.info(f"Consuming topics: {topics}")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Batch size: {args.batch_size} messages")
    logger.info(f"Time window: {args.time_window} seconds")
    logger.info(f"Partitioning scheme: {args.partitioning}")
    
    try:
        # Connect to Kafka
        consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=args.bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='hdfs-writer',
            value_deserializer=lambda x: x,  # Don't deserialize, we'll do it manually
            consumer_timeout_ms=1000,  # 1s timeout for empty poll
        )
        
        logger.info(f"Connected to Kafka")
        
        # Initialize buffers and schemas
        topic_buffers = {}
        schemas = {}
        
        # Track last write time
        last_write_time = time.time()
        
        # Main consumption loop
        while True:
            try:
                # Poll for messages
                for msg in consumer:
                    # Process message and check if we should write
                    topic_to_write = process_message(msg, topic_buffers, schemas, args)
                    
                    if topic_to_write:
                        # Write buffer to HDFS
                        write_topic_buffer(
                            topic_to_write, 
                            topic_buffers[topic_to_write],
                            schemas.get(topic_to_write),
                            args
                        )
                        # Clear buffer
                        topic_buffers[topic_to_write] = []
                        # Reset write time
                        last_write_time = time.time()
                
                # Check if we've exceeded time window for any topics
                current_time = time.time()
                if current_time - last_write_time > args.time_window:
                    # Write all non-empty buffers
                    for topic, buffer in topic_buffers.items():
                        if buffer:
                            write_topic_buffer(topic, buffer, schemas.get(topic), args)
                            topic_buffers[topic] = []
                    # Reset write time
                    last_write_time = current_time
                
                # Small sleep to avoid spinning
                time.sleep(0.1)
                
            except KeyboardInterrupt:
                logger.info("Interrupted by user")
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                time.sleep(1)  # Avoid rapid retries
        
        # Write any remaining buffers
        for topic, buffer in topic_buffers.items():
            if buffer:
                write_topic_buffer(topic, buffer, schemas.get(topic), args)
    
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)
    finally:
        if 'consumer' in locals():
            consumer.close()
            logger.info("Consumer closed")

if __name__ == "__main__":
    main()
