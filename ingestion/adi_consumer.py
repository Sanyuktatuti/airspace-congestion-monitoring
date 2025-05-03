from kafka import KafkaConsumer
import json
import os

def safe_deserialize(b):
    if not b:
        return None
    text = b.decode(errors="ignore")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text

# Create KafkaConsumer
c = KafkaConsumer(
    'flight-stream',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    key_deserializer=lambda b: b.decode() if b else None,
    value_deserializer=safe_deserialize,
)

# Output file
output_file = "data/test_samples.jsonl"
os.makedirs(os.path.dirname(output_file), exist_ok=True)

# Consume messages and write to file
with open(output_file, "a", encoding="utf-8") as f:
    for msg in c:
        record = {
            "key": msg.key,
            "value": msg.value,
        }
        f.write(json.dumps(record) + "\n")
        print("Written:", record)