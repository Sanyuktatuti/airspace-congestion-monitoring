#!/bin/bash

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

# Function to kill a process gracefully
function kill_process() {
    local pid=$1
    local name=$2
    
    if [ -z "$pid" ]; then
        return
    fi
    
    echo_status "Stopping $name (PID: $pid)..."
    
    # Try graceful shutdown first
    kill $pid 2>/dev/null
    
    # Wait a bit for the process to terminate
    local timeout=5
    while [ $timeout -gt 0 ] && ps -p $pid > /dev/null; do
        sleep 1
        timeout=$((timeout-1))
    done
    
    # If still running, force kill
    if ps -p $pid > /dev/null; then
        echo_warning "$name didn't terminate gracefully, force killing..."
        kill -9 $pid 2>/dev/null
        sleep 1
    fi
    
    if ! ps -p $pid > /dev/null; then
        echo_success "$name stopped successfully"
    else
        echo_error "Failed to stop $name"
    fi
}

echo_status "Stopping Airspace Congestion Monitoring System"

# Check if any ports are still bound to our applications
for port in 9092 8086 8501; do
    pid=$(lsof -ti :$port 2>/dev/null)
    if [ ! -z "$pid" ]; then
        echo_warning "Found process (PID: $pid) still using port $port"
    fi
done

# Kill any running Python processes from before
if [ -f .running_pids ]; then
    echo_status "Stopping previously launched processes..."
    PIDS=$(cat .running_pids)
    for PID in $PIDS; do
        if ps -p $PID > /dev/null; then
            component="Unknown Process"
            cmd=$(ps -p $PID -o command= 2>/dev/null)
            
            if [[ "$cmd" == *spark.spark_stream_processor* ]]; then
                component="Spark Stream Processor"
            elif [[ "$cmd" == *storage.influx_writer* ]]; then
                component="InfluxDB Writer"
            elif [[ "$cmd" == *ingestion.replay_producer* ]]; then
                component="Data Producer"
            elif [[ "$cmd" == *streamlit* ]]; then
                component="Streamlit Dashboard"
            fi
            
            kill_process $PID "$component"
        fi
    done
    rm .running_pids
fi

# Find and kill any remaining Python processes related to our components
echo_status "Checking for any remaining Python processes..."

# Kill Spark processor
SPARK_PID=$(ps aux | grep "[s]park.spark_stream_processor" | awk '{print $2}')
if [ ! -z "$SPARK_PID" ]; then
    kill_process $SPARK_PID "Spark Stream Processor"
fi

# Kill InfluxDB writer
INFLUX_PID=$(ps aux | grep "[s]torage.influx_writer" | awk '{print $2}')
if [ ! -z "$INFLUX_PID" ]; then
    kill_process $INFLUX_PID "InfluxDB Writer"
fi

# Kill Data Producer
PRODUCER_PID=$(ps aux | grep "[i]ngestion.replay_producer" | awk '{print $2}')
if [ ! -z "$PRODUCER_PID" ]; then
    kill_process $PRODUCER_PID "Data Producer"
fi

# Kill any other producers
OPENSKY_PID=$(ps aux | grep "[i]ngestion.opensky_kafka_producer" | awk '{print $2}')
if [ ! -z "$OPENSKY_PID" ]; then
    kill_process $OPENSKY_PID "OpenSky Producer"
fi

# Kill Streamlit dashboard (using a better grep pattern)
STREAMLIT_PID=$(ps aux | grep "streamlit run dashboard/opensky_dashboard.py" | grep -v grep | awk '{print $2}')
if [ ! -z "$STREAMLIT_PID" ]; then
    kill_process $STREAMLIT_PID "Streamlit Dashboard"
fi

# Clean up temporary Spark checkpoint files
echo_status "Removing Spark checkpoint files..."
rm -rf /tmp/spark-checkpoints

# Check if any python processes remain
if ps aux | grep -E 'spark.spark_stream_processor|storage.influx_writer|ingestion.replay_producer|streamlit run dashboard' | grep -v grep > /dev/null; then
    echo_warning "Some processes may still be running. You might need to kill them manually."
    ps aux | grep -E 'spark.spark_stream_processor|storage.influx_writer|ingestion.replay_producer|streamlit run dashboard' | grep -v grep
else
    echo_success "All application processes stopped successfully"
fi

# Ask if user wants to stop containers
read -p "Do you want to stop Docker containers (y/n)? " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo_status "Stopping InfluxDB container..."
    if docker stop influxdb 2>/dev/null; then
        echo_success "InfluxDB container stopped"
    else
        echo_warning "InfluxDB container not running"
    fi
    
    echo_status "Stopping Redpanda (Kafka) container..."
    if docker stop redpanda 2>/dev/null; then
        echo_success "Redpanda container stopped"
    else
        echo_warning "Redpanda container not running"
    fi
    
    # Optional: ask if user wants to remove the containers
    read -p "Do you want to remove the Docker containers (y/n)? " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        docker rm -f redpanda influxdb 2>/dev/null
        echo_success "Containers removed"
    fi
fi

echo_success "Airspace Congestion Monitoring System stopped successfully!"
echo_status "You can restart the system with: ./scripts/start_presentation.sh" 