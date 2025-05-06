#!/usr/bin/env python3
"""
replay_producer.py

Replays saved flight data from a JSONL file into a Kafka topic,
allowing testing of the streaming pipeline without needing live API access.
"""
import time
import json
import argparse
import logging
import os
import sys
from kafka import KafkaProducer
from kafka.errors import KafkaError
from pathlib import Path

def setup_logging():
    """Configure logging"""
    logging.basicConfig(
        format='%(asctime)s %(levelname)s: %(message)s',
        level=logging.INFO
    )
    return logging.getLogger("replay-producer")

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Replay flight data from a JSONL file into Kafka')
    
    parser.add_argument(
        '--file', '-f', 
        type=str, 
        default=str(Path(__file__).parent.parent / "data" / "test_samples.jsonl"),
        help='Path to JSONL file containing flight data (default: data/test_samples.jsonl)'
    )
    
    parser.add_argument(
        '--bootstrap-servers', '-b', 
        type=str, 
        default=os.environ.get('KAFKA_BOOTSTRAP', 'localhost:9092'),
        help='Kafka bootstrap servers (default: localhost:9092)'
    )
    
    parser.add_argument(
        '--topic', '-t', 
        type=str, 
        default=os.environ.get('KAFKA_TOPIC', 'flight-stream'),
        help='Kafka topic to publish messages to (default: flight-stream)'
    )
    
    parser.add_argument(
        '--speed', '-s', 
        type=float, 
        default=0.1,
        help='Time in seconds to wait between messages (default: 0.1)'
    )
    
    parser.add_argument(
        '--batch-size', 
        type=int, 
        default=1,
        help='Number of messages to send in a batch (default: 1)'
    )
    
    return parser.parse_args()

def main():
    """Main function"""
    args = parse_args()
    logger = setup_logging()
    
    # Validate input file
    data_file = Path(args.file)
    if not data_file.exists():
        logger.error(f"Data file not found: {data_file}")
        sys.exit(1)
    
    logger.info(f"Reading flight data from: {data_file}")
    logger.info(f"Connecting to Kafka at: {args.bootstrap_servers}")
    logger.info(f"Publishing to topic: {args.topic}")
    logger.info(f"Message delay: {args.speed}s")
    logger.info(f"Batch size: {args.batch_size}")
    
    try:
        # Connect to Kafka
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_servers,
            key_serializer=lambda k: k.encode('utf-8'),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            batch_size=16384,  # 16KB batches
            linger_ms=int(args.speed * 500),  # Half the message delay in ms
            compression_type='gzip'  # Compress messages
        )
        
        # Count total records
        with open(data_file) as f:
            total_records = sum(1 for _ in f)
        
        logger.info(f"Found {total_records} records to replay")
        
        # Read and send records
        sent_count = 0
        batch_count = 0
        start_time = time.time()
        last_progress_time = start_time
        
        with open(data_file) as f:
            for line in f:
                try:
                    rec = json.loads(line)
                    
                    # Handle different formats (compatibility with different export formats)
                    if 'key' in rec and 'value' in rec:
                        # Format: {"key": "abc123", "value": {...flight data...}}
                        key = rec.get("key", "unknown")
                        value = rec.get("value")
                    elif 'icao24' in rec:
                        # Format: {...flight data with icao24 field...}
                        key = rec.get("icao24", "unknown")
                        value = rec
                    else:
                        logger.warning(f"Skipping record with unknown format: {line[:50]}...")
                        continue
                    
                    if key == "unknown" or not value:
                        continue
                    
                    # Send message to Kafka
                    producer.send(args.topic, key=key, value=value)
                    sent_count += 1
                    batch_count += 1
                    
                    # Flush after batch_size messages
                    if batch_count >= args.batch_size:
                        producer.flush()
                        batch_count = 0
                        
                        # Sleep between batches
                        time.sleep(args.speed)
                    
                    # Print progress every 100 messages or every 5 seconds
                    current_time = time.time()
                    if sent_count % 100 == 0 or (current_time - last_progress_time) >= 5:
                        elapsed = current_time - start_time
                        progress = sent_count / total_records * 100
                        rate = sent_count / elapsed if elapsed > 0 else 0
                        eta = (total_records - sent_count) / rate if rate > 0 else 0
                        
                        logger.info(
                            f"Sent {sent_count}/{total_records} messages ({progress:.1f}%) "
                            f"at {rate:.1f} msg/s, ETA: {eta:.1f}s"
                        )
                        last_progress_time = current_time
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Invalid JSON: {e}")
                except Exception as e:
                    logger.error(f"Error processing record: {e}")
        
        # Ensure all messages are sent
        producer.flush()
        
        # Final statistics
        elapsed = time.time() - start_time
        logger.info(
            f"Completed sending {sent_count} messages in {elapsed:.2f}s "
            f"({sent_count/elapsed:.1f} msg/s)"
        )
    
    except KafkaError as e:
        logger.error(f"Kafka error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)
    finally:
        if 'producer' in locals():
            producer.close()
            logger.info("Producer closed")

if __name__ == "__main__":
    main()
