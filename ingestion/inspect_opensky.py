#!/usr/bin/env python3
import sys, os, json, logging
from dotenv import load_dotenv
from opensky_kafka_producer import fetch_states, setup_logging

load_dotenv()           # ← loads .env
setup_logging()

OS_USER = os.getenv("OPENSKY_USER")
OS_PASS = os.getenv("OPENSKY_PASS")
if not (OS_USER and OS_PASS):
    logging.error("Missing OPENSKY_USER / OPENSKY_PASS environment variables.")
    sys.exit(1)

states, ts = fetch_states(username=OS_USER, password=OS_PASS)
if not states:
    logging.error("⚠️ No state vectors returned. Check your credentials, rate limits, or try a bbox.")
    sys.exit(1)

print("timestamp:", ts)
print("one sample state:", json.dumps(states[0], indent=2))
