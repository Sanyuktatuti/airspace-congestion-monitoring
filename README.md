# Airspace Congestion Monitoring

A real-time flight tracking and airspace congestion monitoring system built with Apache Kafka, Spark Structured Streaming, InfluxDB, and Streamlit.

## Why Monitor Airspace Congestion?

Air traffic continues to grow globally, making efficient airspace monitoring critical for:

- **Safety:** Identify potential collision risks and congested corridors
- **Efficiency:** Optimize flight routes and reduce delays
- **Planning:** Better airspace management and resource allocation
- **Environment:** Reduce unnecessary fuel consumption and emissions

This system provides a scalable, real-time solution for monitoring airspace congestion patterns, calculating risk metrics, and visualizing flight data - all using modern big data technologies.

## Architecture

- **Data Ingestion**: Kafka/Redpanda streams flight data
- **Processing**: Spark Structured Streaming processes real-time flight metrics
- **Storage**: InfluxDB (time-series) and HDFS (batch storage)
- **Visualization**: Streamlit dashboard for real-time monitoring

```
┌────────────┐     ┌────────────┐     ┌────────────┐     ┌────────────┐
│ Data       │     │ Kafka      │     │ Spark      │     │ InfluxDB   │
│ Producer   │────>│ (Redpanda) │────>│ Streaming  │────>│ Writer     │
└────────────┘     └────────────┘     └────────────┘     └────────────┘
                         │                  │                   │
                         │                  │                   │
                         │                  │                   v
                         │                  │            ┌────────────┐
                         │                  │            │ InfluxDB   │
                         │                  │            │ Database   │
                         │                  │            └────────────┘
                         │                  │                   ▲
                         │                  v                   │
                         │           ┌────────────┐             │
                         │           │ HDFS       │             │
                         │           │ Writer     │             │
                         │           └────────────┘             │
                         │                                      │
                         v                                      │
                  ┌────────────┐                               │
                  │ Streamlit  │                               │
                  │ Dashboard  │────────────────────────────────
                  └────────────┘
```

## System Components

### 1. Kafka (Message Broker)

Kafka serves as the central message broker, allowing different components to communicate asynchronously. It receives flight data from producers and makes it available to consumers.

- **Container**: Uses Redpanda, a Kafka-compatible streaming platform
- **Topics**:
  - `flight-stream`: Raw flight position data
  - `flight-metrics`: Processed flight metrics
  - `flight-aggregates`: Spatial grid aggregations

### 2. Data Producers

These components fetch flight data and publish it to Kafka:

- **replay_producer.py**: Reads sample flight data from files and publishes to Kafka
- **opensky_kafka_producer.py**: Connects to live OpenSky API to fetch real flight data

### 3. Spark Streaming Processor

Processes the raw flight data in real-time, calculating metrics and performing analytics:

- **spark_stream_processor.py**: The main Spark Structured Streaming application
- **risk_model.py**: Calculates risk scores based on flight parameters
- **Features**: Windowed aggregations, spatial binning, and derived metrics calculation

### 4. Data Storage

Two storage solutions for different use cases:

- **InfluxDB**: Time-series database for real-time metrics and dashboarding
- **HDFS**: Distributed file system for batch storage and historical analysis

### 5. Dashboard

Interactive visualization of flight data:

- **opensky_dashboard.py**: Streamlit-based dashboard showing maps and metrics
- **Features**: Real-time map, grid heatmap, and flight metrics visualization

## Prerequisites

- Python 3.9+ with pip
- Docker for Kafka and InfluxDB
- Sufficient memory (~8GB+ recommended)

## Setup Instructions

### 1. Environment Setup

```bash
# Clone the repository
git clone https://github.com/yourusername/airspace-congestion-monitoring.git
cd airspace-congestion-monitoring

# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set up environment variables (optional but recommended)
cp .env.example .env
# Edit .env file to set your tokens and configuration
```

### 2. Start Infrastructure (Kafka & InfluxDB)

```bash
# Start Kafka (Redpanda)
docker-compose -f kafka/docker-compose.yml up -d

# Start InfluxDB
docker run -d --name influxdb -p 8086:8086 influxdb:2.7
```

### 3. Configure InfluxDB

```bash
# Create an organization
docker exec influxdb influx setup --username admin --password adminpassword --org airspace --bucket flight_metrics --force

# Generate an access token
TOKEN=$(docker exec influxdb influx auth create --org airspace --all-access | grep Token | awk '{print $2}')

# Export the token for client use
export INFLUXDB_TOKEN="$TOKEN"
```

### 4. Run the Pipeline

Start each component in separate terminals:

```bash
# 1. Data Producer
source .env  # Load environment variables (if you created the .env file)
python -m ingestion.replay_producer

# 2. Spark Stream Processor
source .env  # Load environment variables (if you created the .env file)
rm -rf /tmp/spark-checkpoints  # Clear any corrupted checkpoints
python -m spark.spark_stream_processor

# 3. InfluxDB Writer
source .env  # Load environment variables (if you created the .env file)
python -m storage.influx_writer

# 4. Dashboard
source .env  # Load environment variables (if you created the .env file)
streamlit run dashboard/opensky_dashboard.py
```

Alternatively, if you're not using a .env file, you can set the environment variables directly:

```bash
# Example of setting environment variables directly
export INFLUXDB_TOKEN="your_token_here"
export MAPBOX_TOKEN="pk.eyJ1IjoiZGVtby1tYXBib3giLCJhIjoiY2xvMGJuMDZ3MHI3ZjJpbnMwNHJ2ZnM1bCJ9.NQiBw0-YjBCzv_pI8kGsLw"
streamlit run dashboard/opensky_dashboard.py
```

## Simple Step-by-Step Instructions

Here's a simplified workflow to get the system running:

1. **Setup Environment**:

   ```bash
   # Clone and setup
   git clone https://github.com/yourusername/airspace-congestion-monitoring.git
   cd airspace-congestion-monitoring
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Start Docker Containers**:

   ```bash
   # Start Kafka and InfluxDB
   docker-compose -f kafka/docker-compose.yml up -d
   docker run -d --name influxdb -p 8086:8086 influxdb:2.7
   ```

3. **Configure InfluxDB**:

   ```bash
   # Set up InfluxDB with organization and bucket
   docker exec influxdb influx setup --username admin --password adminpassword --org airspace --bucket flight_metrics --force

   # Create and copy the token
   TOKEN=$(docker exec influxdb influx auth create --org airspace --all-access | grep Token | awk '{print $2}')
   echo "Your InfluxDB token: $TOKEN"
   ```

4. **Create .env File**:

   ```bash
   # Copy example file and edit it
   cp .env.example .env
   # Edit .env and paste your InfluxDB token
   ```

5. **Run All Components** (in separate terminal windows):

   ```bash
   # Terminal 1: Data Producer
   source .venv/bin/activate
   source .env
   python -m ingestion.replay_producer

   # Terminal 2: Spark Processor
   source .venv/bin/activate
   source .env
   rm -rf /tmp/spark-checkpoints
   python -m spark.spark_stream_processor

   # Terminal 3: InfluxDB Writer
   source .venv/bin/activate
   source .env
   python -m storage.influx_writer

   # Terminal 4: Dashboard
   source .venv/bin/activate
   source .env
   streamlit run dashboard/opensky_dashboard.py
   ```

6. **View Dashboard**:
   Open your browser to http://localhost:8501

## Troubleshooting

### Authentication Issues

If you see 401 Unauthorized errors with InfluxDB, ensure:

- The token has been generated properly
- The token is correctly exported as an environment variable
- The organization name matches what was configured

### Map Visualization Issues

If the map doesn't display in the dashboard:

- Make sure the MAPBOX_TOKEN environment variable is set
- Try a different map_style (options: "light", "dark", "streets", "satellite")
- Check the browser console for any errors related to Mapbox

### Spark Errors

- Clear the checkpoint directory if you see checkpoint-related errors: `rm -rf /tmp/spark-checkpoints`
- Increase memory if you see OOM errors by adjusting settings in `spark_stream_processor.py`

### Data Flow Issues

If no data shows in the dashboard:

1. Check if Kafka topics have data: `docker exec redpanda rpk topic consume flight-stream -n 1`
2. Verify Spark is processing data by monitoring its output logs
3. Ensure InfluxDB writes are successful by checking the influx_writer logs
4. Make sure the dashboard has the correct InfluxDB token

## Project Structure

- `ingestion/`: Data ingestion components
- `spark/`: Spark streaming processing logic
- `storage/`: Storage adapters for InfluxDB and HDFS
- `dashboard/`: Streamlit visualization dashboard
- `kafka/`: Kafka configuration files
- `data/`: Sample data files

## Data Flow

The system processes flight data through several stages:

1. **Data Ingestion:**

   - Flight data is either replayed from sample files or fetched from OpenSky API
   - Data includes aircraft positions, velocities, altitudes, and metadata
   - Formatted as JSON messages and published to Kafka topic `flight-stream`

2. **Stream Processing:**

   - Spark Structured Streaming consumes messages from Kafka
   - Performs windowed aggregations (30-second windows with 10-second sliding intervals)
   - Calculates risk scores, acceleration, turning rates, and other flight metrics
   - Creates spatial aggregations by binning flights into geographic grid cells
   - Outputs processed data to Kafka topics `flight-metrics` and `flight-aggregates`

3. **Data Storage:**

   - InfluxDB Writer consumes from Kafka and stores time-series data
   - HDFS Writer archives data for historical analysis
   - Each storage system is optimized for different query patterns

4. **Visualization:**
   - Streamlit dashboard connects to both Kafka and InfluxDB
   - Real-time map shows current flight positions
   - Grid heatmap displays congestion patterns
   - Metrics view shows detailed flight analytics
   - Supports both real-time and historical data viewing

The entire pipeline is designed to operate in real-time, with minimal latency between data ingestion and visualization.
