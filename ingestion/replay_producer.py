#!/usr/bin/env python3
import time, json
from kafka import KafkaProducer
from pathlib import Path

# 1) Point this at your saved file:
DATA_FILE = Path(__file__).parent.parent / "data" / "test_samples.jsonl"

# 2) Connect to Kafka exactly like your live producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    key_serializer=lambda k: k.encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)

# 3) Read & send each line, pausing so Spark has time to process:
with open(DATA_FILE) as f:
    for line in f:
        rec = json.loads(line)
        key = rec.get("key", "unknown")
        if(key == "unknown"):
            continue
        producer.send("flight-stream", key=key, value=rec.get("value", "None"))
        producer.flush()
        time.sleep(0.1)   # send ~10 messages/sec; you can speed this up if you like

producer.close()
