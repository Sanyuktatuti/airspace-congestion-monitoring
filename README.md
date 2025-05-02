# Airspace Congestion Monitoring System

## Project Overview

The **Airspace Congestion and Safety Monitoring System** is a Big Data pipeline designed to provide real-time and historical insights into aircraft density, trajectories, and safety risks within busy flight corridors. It ingests live data from the OpenSky Network API, enriches with static aircraft metadata, processes streams for analytics and risk scoring, and visualizes results on an interactive Mapbox dashboard.

Key Features:

* **Real-Time Ingestion:** Fetch live state vectors every 10 seconds using a Kafka producer.
* **Metadata Enrichment:** Join each flight record with static aircraft details.
* **Stream Processing:** Use Spark Structured Streaming for both risk scoring and geospatial density analytics.
* **Storage Layers:** Persist real-time metrics in InfluxDB and historical data in HDFS.
* **Visualization:** Display live aircraft positions, congestion heatmaps, and risk alerts via Mapbox GL JS.

## Repository Structure

```
airspace-monitoring/
├── data/
│   ├── aircraft_metadata.csv    # Static aircraft metadata
│   └── test_samples.jsonl       # Recorded OpenSky messages for replay mode
├── ingestion/
│   ├── opensky_kafka_producer.py    # Publishes enriched flight data to Kafka
│   ├── replay_producer.py           # Replays saved data for offline testing
├── kafka/
│   └── docker-compose.yml           # Kafka & Zookeeper setup
├── spark/
│   ├── spark_stream_processor.py    # Consumes & enriches stream
│   ├── geo_density.py               # Computes density via Sedona
│   └── risk_model.py                # Applies risk-scoring logic
├── storage/
│   ├── influx_writer.py             # Writes real-time metrics
│   └── hdfs_writer.py               # Archives enriched data
├── dashboard/
│   └── mapbox_dashboard.html        # Web UI for visualization
├── output/
│   ├── alerts.csv                   # Generated risk alerts
│   └── density_heatmap.csv          # Density stats CSV
├── requirements.txt                 # Python dependencies
└── README.md                        # This document
```

## Getting Started

### Prerequisites

* **Python 3.8+**
* **Docker & Docker Compose** (for Kafka)
* **Java 8+** (for Spark)
* **Kafka** running locally on `localhost:9092`
* **InfluxDB** (optional for MVP but recommended)
* **HDFS** (optional for batch history)
* **jq** (for JSON filtering when recording messages)

### Setup

1. **Clone the repository**:

   ```bash
   git clone https://github.com/your-org/airspace-congestion-monitoring.git
   cd airspace-congestion-monitoring
   ```

2. **Install Python dependencies**:

   ```bash
   pip install -r requirements.txt
   ```

3. **Start Kafka**:

   ```bash
   docker-compose -f kafka/docker-compose.yml up -d
   ```

4. **Prepare Metadata**:

   * Place your `aircraft_metadata.csv` in `data/`.

## Running the Pipeline

### 1. Live Ingestion (Kafka Producer)

```bash
python ingestion/opensky_kafka_producer.py
```

* Connects to the OpenSky API every 10 seconds, enriches with static metadata, and publishes to the `flight-stream` topic.

### 2. Offline Replay (Replay Producer)

When API rate limits prevent live ingestion—or for offline testing—you can replay previously recorded messages:

1. **Record live messages** (run once when not rate-limited):

   ```bash
   python ingestion/opensky_kafka_producer.py \
     | jq -c '.value' \
     > data/test_samples.jsonl
   ```

   This saves each JSON record (one per line) into `ingestion/data/test_samples.jsonl`.

2. **Replay to Kafka**:

   ```bash
   python ingestion/replay_producer.py
   ```

   This script reads `test_samples.jsonl` and republishes each line into the same `flight-stream` topic (with a short pause between messages), allowing Spark and downstream components to process without hitting API limits.

### 3. Stream Processing (Spark)

```bash
spark-submit spark/spark_stream_processor.py
```

* Consumes from Kafka, computes risk scores and spatial aggregates, then writes outputs to InfluxDB (real-time) and HDFS (historical).

### 4. Geospatial Analytics

```bash
spark-submit spark/geo_density.py
```

* Calculates aircraft density and exports heatmap data.

### 5. Risk Scoring

```bash
spark-submit spark/risk_model.py
```

* Generates risk scores and writes alerts to `output/alerts.csv`.

### 6. Dashboard

* Open `dashboard/mapbox_dashboard.html` in a browser to view the live map, congestion heatmap, and risk alerts.

## Architecture Diagram

```plaintext
[OpenSky API] -> [Kafka Producer] -> [Kafka Topic: flight-stream]
       |             |                     |
       v             v                     v
    [Metadata]   [Spark Streaming: Parse & Risk]   [Spark Streaming: Geo Analytics]
       |             |                     |
       v             v                     v
    [Enriched Data]    [InfluxDB (real-time)]   [HDFS (historical)]
                             |
                             v
                      [Mapbox GL JS Dashboard]
```

## Contributing

Feel free to open issues and submit pull requests to improve functionality, add monitoring, or extend the ML models.

## License

This project is licensed under the MIT License.
