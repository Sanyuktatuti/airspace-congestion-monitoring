#!/usr/bin/env python3
from kafka import KafkaConsumer
import json

def safe_deserialize(b):
    if not b:
        return None
    text = b.decode(errors="ignore")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # you could log this if you like
        return text

c = KafkaConsumer(
    'flight-stream',  # change to flight-scores or flight-aggregates as needed
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    key_deserializer=lambda b: b.decode() if b else None,
    value_deserializer=safe_deserialize,
)

for msg in c:
    print(f"key={msg.key!r}  value={msg.value!r}")
