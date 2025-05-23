# Airspace Congestion Monitoring

A real-time flight tracking and airspace congestion monitoring system built with Apache Kafka, Spark Structured Streaming, InfluxDB, MongoDB, and Streamlit.

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
- **Storage**: InfluxDB (time-series), MongoDB (historical analytics), and HDFS (batch storage)
- **Visualization**: Streamlit dashboard for real-time monitoring and historical analysis

![System Architecture](Architecture.png)

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

Three storage solutions for different use cases:

- **InfluxDB**: Time-series database for real-time metrics and dashboarding
- **MongoDB**: Document database for historical flight data analytics
- **HDFS**: Distributed file system for batch storage and historical analysis

### 5. Dashboard

Interactive visualization of flight data:

- **opensky_dashboard.py**: Streamlit-based dashboard showing maps and metrics
- **Features**: Real-time map, grid heatmap, flight metrics visualization, and historical analytics

## Prerequisites

- Python 3.9+ with pip
- Docker for Kafka and InfluxDB
- MongoDB (optional, for historical analytics)
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

## Quick Start with Presentation Scripts

For a streamlined setup and execution, we provide two utility scripts:

### Start the System

```bash
# Make the script executable
chmod +x scripts/start_presentation.sh

# Run the script
./scripts/start_presentation.sh
```

**What the start script does:**

1. **Environment Setup**:

   - Sets Java home for Spark
   - Checks for port conflicts (9092, 8086, 8501) and offers to free them
   - Creates a default `.env` file if none exists

2. **Infrastructure Setup**:

   - Stops and removes existing Docker containers if present
   - Starts Redpanda (Kafka-compatible) container
   - Creates required Kafka topics (flight-stream, flight-metrics, flight-aggregates)
   - Starts InfluxDB container
   - Configures InfluxDB with organization and bucket
   - Creates and validates an access token
   - Updates the `.env` file with the valid token

3. **Application Components**:

   - Starts the data producer (replay_producer.py)
   - Starts the Spark streaming processor
   - Starts the InfluxDB writer
   - Launches the Streamlit dashboard

4. **Health Checks**:
   - Verifies all components are running
   - Provides real-time feedback on the startup process

### Stop the System

```bash
# Make the script executable
chmod +x scripts/stop_presentation.sh

# Run the script
./scripts/stop_presentation.sh
```

**What the stop script does:**

1. **Process Management**:

   - Identifies and stops all running Python processes related to the system
   - Gracefully terminates processes with timeout before force killing
   - Cleans up process IDs stored during startup

2. **Cleanup**:

   - Removes temporary Spark checkpoint files
   - Verifies all processes have been terminated

3. **Docker Cleanup**:
   - Offers to stop the Docker containers (InfluxDB and Redpanda)
   - Optionally removes the containers if requested

### 2. Alternatively, launching the application step-by-step:

## 1. Start Infrastructure (Kafka & InfluxDB)

```bash
# Start Kafka (Redpanda)
docker-compose -f kafka/docker-compose.yml up -d

# Start InfluxDB
docker run -d --name influxdb -p 8086:8086 influxdb:2.7
```

## 2. Configure InfluxDB

```bash
# Create an organization
docker exec influxdb influx setup --username admin --password adminpassword --org airspace --bucket flight_metrics --force

# Generate an access token
TOKEN=$(docker exec influxdb influx auth create --org airspace --all-access | grep Token | awk '{print $2}')

# Export the token for client use
export INFLUXDB_TOKEN="$TOKEN"
```

## 3. Run the Pipeline

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

### 4. Historical Data Analytics with MongoDB (Optional)

The system includes support for historical flight data analytics using MongoDB. This allows you to analyze patterns over time, identify congestion hotspots, and detect risk anomalies.

#### Setup MongoDB

```bash
# Run the MongoDB setup script
chmod +x scripts/setup_mongodb.sh
./scripts/setup_mongodb.sh
```

#### Generate and Import Historical Data

```bash
# Import the collected jsonl data into MongoDB
python data/import_to_mongodb.py --drop
```

#### Using Historical Analytics in the Dashboard

1. In the sidebar, select "Flight Metrics" view mode
2. Navigate to the "Historical Analytics" tab
3. Explore the following analytics:
   - **Flight Density Timeline**: Visualize how flight traffic changes over time
   - **Hourly Flight Patterns**: Analyze peak hours and traffic distribution
   - **Daily Flight Patterns**: Compare weekday vs. weekend patterns
   - **Congestion Hotspots**: Identify areas with high flight density
   - **Risk Anomalies**: Detect periods with abnormal risk levels

The historical data spans from April 30 to May 6, 2025, with approximately 475,000 flight records. (For testing purposes)

### MongoDB Integration Architecture

The MongoDB integration consists of the following components:

1. **Data Generation**: `augment_historical_data.py` generates realistic historical flight data
2. **Data Import**: `import_to_mongodb.py` imports the data into MongoDB with proper indexing
3. **Query Utilities**: `mongodb_utils.py` provides functions for querying and aggregating flight data
4. **Dashboard Integration**: `opensky_dashboard.py` uses MongoDB for historical analytics

The system uses MongoDB's aggregation framework to efficiently analyze large volumes of flight data, including:

- Time-based aggregations for hourly and daily patterns
- Geospatial queries for hotspot detection
- Risk anomaly detection through statistical analysis

MongoDB was chosen for historical analytics because of its:

- Flexible document model for flight data
- Powerful aggregation framework
- Geospatial indexing capabilities
- Ability to handle large volumes of data efficiently

The MongoDB integration is optional - the dashboard will still work without it, falling back to file-based analytics when MongoDB is unavailable.

## Simple Step-by-Step Instructions

Here's a simplified workflow to get the system running:

1. **Setup Environment**:

   ```bash
   # Clone and setup
   git clone https://github.com/yourusername/airspace-congestion-monitoring.git
   cd airspace-congestion-monitoring
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. **Start Infrastructure** (2 Options):

3. **Start and Stop Scripts:**
   Make the script executable:
   chmod +x scripts/stop_presentation.sh

   Run the script:
   ./scripts/stop_presentation.sh

   Make the script executable:
   chmod +x scripts/start_presentation.sh

   Run the script:
   ./scripts/start_presentation.sh

   ##OR

4. **Launch each services individually:**

**Start Infrastructure**:

```bash
# Start Kafka and InfluxDB with Docker
docker-compose -f kafka/docker-compose.yml up -d
docker run -d --name influxdb -p 8086:8086 influxdb:2.7
```

**Run Components**:

```bash
# In separate terminals:
python -m ingestion.replay_producer
python -m spark.spark_stream_processor
python -m storage.influx_writer
streamlit run dashboard/opensky_dashboard.py
```

**Access the Dashboard**:

- Open your browser to http://localhost:8501

## Historical Data Analytics with MongoDB

The system includes support for historical flight data analytics using MongoDB. This allows you to analyze patterns over time, identify congestion hotspots, and detect risk anomalies.

### Setup MongoDB

```bash
# Run the MongoDB setup script
chmod +x scripts/setup_mongodb.sh
./scripts/setup_mongodb.sh
```

This script will:

- Install MongoDB if not already installed
- Configure MongoDB for the application
- Install the required Python packages
- Set up environment variables

### Generate and Import Historical Data

```bash
# Generate a week of synthetic historical flight data
python data/augment_historical_data.py

# Import the data into MongoDB
python data/import_to_mongodb.py --drop
```

The historical data generator creates realistic flight patterns based on:

- Time of day variations (peak hours vs. night hours)
- Day of week patterns (weekday vs. weekend)
- Geographic distribution with realistic flight paths
- Risk score variations based on congestion levels

### Using Historical Analytics in the Dashboard

1. Start the dashboard: `streamlit run dashboard/opensky_dashboard.py`
2. In the sidebar, select "Flight Metrics" view mode
3. Navigate to the "Historical Analytics" tab
4. Explore the following analytics:
   - **Flight Density Timeline**: Visualize how flight traffic changes over time
   - **Hourly Flight Patterns**: Analyze peak hours and traffic distribution
   - **Daily Flight Patterns**: Compare weekday vs. weekend patterns
   - **Congestion Hotspots**: Identify areas with high flight density
   - **Risk Anomalies**: Detect periods with abnormal risk levels

The historical data spans from April 30 to May 6, 2025, with approximately 475,000 flight records.

The MongoDB integration is optional - the dashboard will still work without it, falling back to file-based analytics when MongoDB is unavailable.

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
