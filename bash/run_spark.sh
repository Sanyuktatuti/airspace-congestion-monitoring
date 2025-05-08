#!/bin/bash
source .venv/bin/activate
source .env
rm -rf /tmp/spark-checkpoints
python -m spark.spark_stream_processor