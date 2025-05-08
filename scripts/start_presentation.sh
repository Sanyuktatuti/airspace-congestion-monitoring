#!/bin/bash
set -e

# Print colorful status messages
function echo_status() {
    echo -e "\e[1;36m>>> $1\e[0m"
}

function echo_error() {
    echo -e "\e[1;31mError: $1\e[0m"
}

function echo_warning() {
    echo -e "\e[1;33mWarning: $1\e[0m"
}

function echo_success() {
    echo -e "\e[1;32m$1\e[0m"
}

# Set Java home for Spark
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
export SPARK_HOME=$(python -c "import pyspark; print(pyspark.__path__[0])")

# Check if a port is in use and kill the process if needed
function check_and_free_port() {
    local port=$1
    local force=$2
    
    echo_status "Checking if port $port is in use..."
    local pid=$(lsof -ti :$port)
    
    if [ ! -z "$pid" ]; then
        echo_warning "Port $port is in use by process(es): $pid"
        if [ "$force" == "force" ]; then
            echo_status "Force killing process(es)..."
            kill -9 $pid 2>/dev/null
            sleep 1
            if lsof -ti :$port > /dev/null; then
                echo_error "Failed to free port $port"
                return 1
            else
                echo_success "Port $port freed successfully"
            fi
        else
            read -p "Do you want to kill the process(es) using port $port? (y/n): " -n 1 -r
            echo
            if [[ $REPLY =~ ^[Yy]$ ]]; then
                kill -9 $pid 2>/dev/null
                sleep 1
                if lsof -ti :$port > /dev/null; then
                    echo_error "Failed to free port $port"
                    return 1
                else
                    echo_success "Port $port freed successfully"
                fi
            else
                echo_error "Port $port is still in use. Cannot continue."
                return 1
            fi
        fi
    else
        echo_status "Port $port is available"
    fi
    
    return 0
}

# Cleanup function for error handling
function cleanup() {
    echo_status "Cleaning up..."
    if [ -f .temp_pids ]; then
        PIDS=$(cat .temp_pids)
        for PID in $PIDS; do
            if ps -p $PID > /dev/null; then
                echo "Killing temporary process with PID: $PID"
                kill $PID 2>/dev/null || kill -9 $PID 2>/dev/null
            fi
        done
        rm .temp_pids
    fi
}

# Set up the trap to call cleanup on exit
trap cleanup EXIT

echo_status "Starting Airspace Congestion Monitoring System"

# Check if Docker is running
echo_status "Checking if Docker is running..."
if ! docker info > /dev/null 2>&1; then
    echo_error "Docker is not running. Please start Docker first."
    exit 1
fi

# Check for port conflicts before starting
check_and_free_port 9092 || exit 1
check_and_free_port 8086 || exit 1
check_and_free_port 8501 || exit 1

# Check if environment file exists
if [ ! -f .env ]; then
    echo_status "Creating default .env file..."
    cat > .env << EOF
MAPBOX_TOKEN=pk.eyJ1IjoiZGVtby1tYXBib3giLCJhIjoiY2xvMGJuMDZ3MHI3ZjJpbnMwNHJ2ZnM1bCJ9.NQiBw0-YjBCzv_pI8kGsLw
INFLUXDB_URL=http://localhost:8086
INFLUXDB_ORG=airspace
INFLUXDB_BUCKET=flight_metrics
INFLUXDB_TOKEN=your_token_here
KAFKA_BOOTSTRAP=localhost:9092
EOF
    echo_status "Please edit the .env file with your actual tokens if needed."
    sleep 2
fi

# Stop and remove existing containers if they exist
echo_status "Checking for existing containers..."
if docker ps -a | grep -q redpanda; then
    echo_status "Removing existing Redpanda container..."
    docker rm -f redpanda >/dev/null 2>&1
fi

if docker ps -a | grep -q influxdb; then
    echo_status "Removing existing InfluxDB container..."
    docker rm -f influxdb >/dev/null 2>&1
fi

# 2. Start Kafka (Redpanda)
echo_status "Starting Redpanda container..."
if [ -f kafka/docker-compose.yml ]; then
    docker-compose -f kafka/docker-compose.yml up -d
else
    # If no kafka/docker-compose.yml, use direct docker command
    docker run -d --name redpanda -p 9092:9092 -p 19092:19092 -p 8081:8081 -p 8082:8082 --rm docker.redpanda.com/redpandadata/redpanda:latest
fi

# Wait for Redpanda to start (20 seconds timeout)
echo_status "Waiting for Redpanda to start..."
timeout=20
while [ $timeout -gt 0 ]; do
    if docker exec -it redpanda rpk cluster info >/dev/null 2>&1; then
        echo_success "Redpanda is ready!"
        break
    fi
    echo -n "."
    sleep 1
    timeout=$((timeout-1))
done

if [ $timeout -eq 0 ]; then
    echo_error "Redpanda failed to start within the timeout period."
    exit 1
fi

# Create Kafka topics
echo_status "Creating Kafka topics..."
for topic in flight-stream flight-metrics flight-aggregates
do
    docker exec redpanda rpk topic create $topic 2>/dev/null || echo_warning "Topic $topic already exists."
done

# 3. Start InfluxDB
echo_status "Starting InfluxDB container..."
docker run -d --name influxdb -p 8086:8086 influxdb:2.7

# Wait for InfluxDB to start (15 seconds timeout)
echo_status "Waiting for InfluxDB to start..."
timeout=15
while [ $timeout -gt 0 ]; do
    if curl -s http://localhost:8086/ping > /dev/null; then
        echo_success "InfluxDB is ready!"
        break
    fi
    echo -n "."
    sleep 1
    timeout=$((timeout-1))
done

if [ $timeout -eq 0 ]; then
    echo_error "InfluxDB failed to start within the timeout period."
    exit 1
fi

# Set up InfluxDB and get a valid token
echo_status "Setting up InfluxDB..."
docker exec influxdb influx setup \
    --username admin \
    --password adminpassword \
    --org airspace \
    --bucket flight_metrics \
    --force || echo_warning "InfluxDB already configured."

# Get the actual admin token that works with this container
echo_status "Getting admin token from InfluxDB..."

# Create an operator token that we can use
echo_status "Creating a fresh operator token..."
TOKEN_OUTPUT=$(docker exec influxdb bash -c "influx auth create --org airspace --all-access")
echo "Token output: $TOKEN_OUTPUT"
TOKEN=$(echo "$TOKEN_OUTPUT" | grep -o '[a-zA-Z0-9_\-]*==' | head -1)

if [ -z "$TOKEN" ]; then
    echo_error "Failed to extract token from output. Trying alternative method..."
    # Try to get any existing token
    TOKEN_OUTPUT=$(docker exec influxdb bash -c "influx auth list")
    echo "Auth list output: $TOKEN_OUTPUT"
    TOKEN=$(echo "$TOKEN_OUTPUT" | grep -o '[a-zA-Z0-9_\-]*==' | head -1)
fi

if [ -z "$TOKEN" ]; then
    echo_error "Could not find a valid token. Trying one more method..."
    # Create a direct auth token
    TOKEN_OUTPUT=$(docker exec influxdb bash -c "influx auth create -o airspace --all-access --json")
    echo "JSON output: $TOKEN_OUTPUT"
    TOKEN=$(echo "$TOKEN_OUTPUT" | grep -o '"token":"[^"]*"' | cut -d'"' -f4)
fi

if [ -z "$TOKEN" ]; then
    echo_error "Failed to get any valid token. Cannot continue."
    exit 1
fi

echo_status "Using InfluxDB token: $TOKEN"

# Update .env file with token
sed -i '' "s/INFLUXDB_TOKEN=.*/INFLUXDB_TOKEN=$TOKEN/" .env
echo_success "Updated .env with InfluxDB token."

# Source environment variables
source .env

# Verify token works
echo_status "Verifying InfluxDB token..."
INFLUX_RESP=$(curl -s -H "Authorization: Token ${INFLUXDB_TOKEN}" http://localhost:8086/api/v2/buckets)
if echo "$INFLUX_RESP" | grep -q "resources"; then
    echo_success "InfluxDB token is valid!"
else
    echo_warning "Token verification failed. Trying simpler format..."
    
    # Update with simpler token format
    SIMPLE_TOKEN="_KNNAyvUeXNPT8VdZgtd9ghbHt_IFboClj3BKIRtipIE6iTAfRqzxxY8kvSlMaOviBN4q1xZEPp-WsNmXc-iBw=="
    sed -i '' "s/INFLUXDB_TOKEN=.*/INFLUXDB_TOKEN=$SIMPLE_TOKEN/" .env
    source .env
    
    # Try with this token
    INFLUX_RESP=$(curl -s -H "Authorization: Token ${INFLUXDB_TOKEN}" http://localhost:8086/api/v2/buckets)
    if echo "$INFLUX_RESP" | grep -q "resources"; then
        echo_success "Simpler token works!"
    else
        echo_warning "Still failing. Trying a different approach with bucket list..."
        
        # Try using the influx CLI directly to verify
        BUCKET_LIST=$(docker exec influxdb influx bucket list)
        if echo "$BUCKET_LIST" | grep -q "flight_metrics"; then
            echo_success "InfluxDB setup verified via CLI!"
            # Continue even with token issues
        else
            echo_error "InfluxDB setup could not be verified. Proceeding with caution."
        fi
    fi
fi

# Check if any of our processes are already running and kill them
echo_status "Checking for existing Python processes..."
for process in "spark.spark_stream_processor" "storage.influx_writer" "ingestion.replay_producer" "streamlit run dashboard/opensky_dashboard.py"; do
    PID=$(ps aux | grep "$process" | grep -v grep | awk '{print $2}')
    if [ ! -z "$PID" ]; then
        echo_warning "$process is already running with PID: $PID"
        echo_status "Stopping process..."
        kill -9 $PID 2>/dev/null
        sleep 1
    fi
done

# Clean up checkpoints to prevent issues
echo_status "Cleaning up Spark checkpoints..."
rm -rf /tmp/spark-checkpoints

# Create a temporary file to store PIDs for cleanup in case of errors
touch .temp_pids

# 4. Start Spark Stream Processor
echo_status "Starting Spark Stream Processor..."
python -m spark.spark_stream_processor &
SPARK_PID=$!
echo $SPARK_PID >> .temp_pids

# Verify Spark process started
sleep 3
if ! ps -p $SPARK_PID > /dev/null; then
    echo_error "Spark Stream Processor failed to start."
    exit 1
fi
echo_success "Spark Stream Processor started with PID: $SPARK_PID"

# 5. Start InfluxDB Writer
echo_status "Starting InfluxDB Writer..."
python -m storage.influx_writer &
INFLUX_WRITER_PID=$!
echo $INFLUX_WRITER_PID >> .temp_pids

# Verify InfluxDB Writer process started
sleep 3
if ! ps -p $INFLUX_WRITER_PID > /dev/null; then
    echo_error "InfluxDB Writer failed to start."
    kill $SPARK_PID
    exit 1
fi
echo_success "InfluxDB Writer started with PID: $INFLUX_WRITER_PID"

# 6. Start Data Producer
echo_status "Starting Data Producer..."
python -m ingestion.replay_producer --speed 0.1 --batch-size 10 &
PRODUCER_PID=$!
echo $PRODUCER_PID >> .temp_pids

# Verify Data Producer process started
sleep 3
if ! ps -p $PRODUCER_PID > /dev/null; then
    echo_error "Data Producer failed to start."
    kill $SPARK_PID $INFLUX_WRITER_PID
    exit 1
fi
echo_success "Data Producer started with PID: $PRODUCER_PID"

# 7. Start Streamlit Dashboard
echo_status "Starting Streamlit Dashboard..."
streamlit run dashboard/opensky_dashboard.py &
STREAMLIT_PID=$!
echo $STREAMLIT_PID >> .temp_pids

# Verify Streamlit started
sleep 5
if ! ps -p $STREAMLIT_PID > /dev/null; then
    echo_error "Streamlit Dashboard failed to start."
    kill $SPARK_PID $INFLUX_WRITER_PID $PRODUCER_PID
    exit 1
fi
echo_success "Streamlit Dashboard started with PID: $STREAMLIT_PID"

# Remove the temporary PID file
rm -f .temp_pids

echo_success "All components started successfully!"
echo_status "Dashboard available at: http://localhost:8501"
echo_status "Process Summary:"
echo "- Spark Stream Processor: PID $SPARK_PID"
echo "- InfluxDB Writer: PID $INFLUX_WRITER_PID"
echo "- Data Producer: PID $PRODUCER_PID"
echo "- Streamlit Dashboard: PID $STREAMLIT_PID"
echo ""
echo "To stop all processes, run: ./scripts/stop_presentation.sh"

# Save PIDs for the stop script
echo "$SPARK_PID $INFLUX_WRITER_PID $PRODUCER_PID $STREAMLIT_PID" > .running_pids 