# Airspace Congestion Monitoring System

## Project Overview

The **Airspace Congestion and Safety Monitoring System** is a Big Data pipeline designed to provide real-time and historical insights into aircraft density, trajectories, and safety risks within busy flight corridors. It ingests live data from the OpenSky Network API, enriches with static aircraft metadata, processes streams for analytics and risk scoring, and visualizes results on an interactive Mapbox dashboard.

Key Features:
- **Real-Time Ingestion:** Fetch live state vectors every 10 seconds using a Kafka producer.
- **Metadata Enrichment:** Join each flight record with static aircraft details.
- **Stream Processing:** Use Spark Structured Streaming for both risk scoring and geospatial density analytics.
- **Storage Layers:** Persist real-time metrics in InfluxDB and historical data in HDFS.
- **Visualization:** Display live aircraft positions, congestion heatmaps, and risk alerts via Mapbox GL JS.

## Repository Structure

```
airspace-monitoring/
├── ingestion/
│   └── opensky_kafka_producer.py   # Publishes enriched flight data to Kafka
│   └── data/
│       └── aircraft_metadata.csv   # Static aircraft metadata
├── kafka/
│   └── docker-compose.yml         # Kafka & Zookeeper setup
├── spark/
│   ├── spark_stream_processor.py  # Consumes & enriches stream
│   ├── geo_density.py             # Computes density via Sedona
│   └── risk_model.py              # Applies risk-scoring logic
├── storage/
│   ├── influx_writer.py           # Writes real-time metrics
│   └── hdfs_writer.py             # Archives enriched data
├── dashboard/
│   └── mapbox_dashboard.html      # Web UI for visualization
├── output/
│   ├── alerts.csv                 # Generated risk alerts
│   └── density_heatmap.csv        # Density stats CSV
├── requirements.txt               # Python dependencies
└── README.md                      # This document
```

## Getting Started

### Prerequisites

- **Python 3.8+**
- **Docker & Docker Compose** (for Kafka)
- **Java 8+** (for Spark)
- **Kafka** running locally on `localhost:9092`
- **InfluxDB** (optional for MVP but recommended)
- **HDFS** (optional for batch history)

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
   - Place your `aircraft_metadata.csv` in `ingestion/data/`.

### Running the Pipeline

#### 1. Ingestion (Kafka Producer)
```bash
python ingestion/opensky_kafka_producer.py
```
- Publishes enriched flight records to the `flight-stream` topic every 10 seconds.

#### 2. Stream Processing (Spark)
```bash
spark-submit spark/spark_stream_processor.py
```
- Consumes from Kafka, enriches, and writes to InfluxDB and HDFS.

#### 3. Geospatial Analytics
```bash
spark-submit spark/geo_density.py
```
- Calculates aircraft density and exports heatmap data.

#### 4. Risk Scoring
```bash
spark-submit spark/risk_model.py
```
- Generates risk scores and writes alerts to `output/alerts.csv`.

#### 5. Dashboard
- Open `dashboard/mapbox_dashboard.html` in a browser to view live map and metrics.

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

