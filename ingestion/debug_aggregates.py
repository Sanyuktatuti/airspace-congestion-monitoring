# ingestion/debug_aggregates.py
import json
from kafka import KafkaConsumer

c = KafkaConsumer(
    'flight-aggregates',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    value_deserializer=lambda b: json.loads(b.decode()) if b else None,
)

print("Listening for aggregates…")
for msg in c:
    print(json.dumps(msg.value, indent=2))
