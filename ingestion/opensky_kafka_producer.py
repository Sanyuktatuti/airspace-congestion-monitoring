# ingestion/debug_consumer.py
from kafka import KafkaConsumer
import json
def safe_deserialize(b):
    if not b:
        return None
    s = b.decode(errors="ignore").strip()
    if not s:
        return None
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        # skip malformed
        return None

c = KafkaConsumer(
    'flight-stream',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    consumer_timeout_ms=5000,
    key_deserializer=lambda b: b.decode() if b else None,
    value_deserializer=safe_deserialize
)

for msg in c:
    # only print if there's an actual JSON payload
    if msg.value is not None:
        print(f"{msg.key} → {msg.value}")
