#!/bin/bash
# Run historical analytics and dashboard

set -e

# Print colorful status messages
function echo_status() {
    echo -e "\e[1;36m>>> $1\e[0m"
}

function echo_error() {
    echo -e "\e[1;31mError: $1\e[0m"
}

function echo_success() {
    echo -e "\e[1;32m$1\e[0m"
}

# Check if Java is installed
if ! command -v java &> /dev/null; then
    echo_error "Java is required but not installed. Please install Java and try again."
    exit 1
fi

# Set Java home for Spark
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
echo_status "Using Java from: $JAVA_HOME"

# Create necessary directories
mkdir -p data/analytics

# Parse command line arguments
START_DATE="2023-01-01T00:00:00Z"
END_DATE="2023-01-07T23:59:59Z"
DASHBOARD_ONLY=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --start)
            START_DATE="$2"
            shift 2
            ;;
        --end)
            END_DATE="$2"
            shift 2
            ;;
        --dashboard-only)
            DASHBOARD_ONLY=true
            shift
            ;;
        *)
            echo_error "Unknown option: $1"
            echo "Usage: $0 [--start START_DATE] [--end END_DATE] [--dashboard-only]"
            exit 1
            ;;
    esac
done

# Run historical analytics
if [ "$DASHBOARD_ONLY" = false ]; then
    echo_status "Running historical analytics from $START_DATE to $END_DATE"
    
    # Set Spark home if needed
    if [ -z "$SPARK_HOME" ]; then
        export SPARK_HOME=$(python -c "import pyspark; print(pyspark.__path__[0])")
        echo_status "Setting SPARK_HOME to $SPARK_HOME"
    fi
    
    # Run the analytics
    python -m spark.historical_analytics --start "$START_DATE" --end "$END_DATE"
    
    if [ $? -ne 0 ]; then
        echo_error "Historical analytics failed"
        exit 1
    fi
    
    echo_success "Historical analytics completed successfully"
else
    echo_status "Skipping analytics, running dashboard only"
fi

# Run the dashboard
echo_status "Starting historical analytics dashboard"
cd dashboard && streamlit run historical_dashboard.py

exit 0 