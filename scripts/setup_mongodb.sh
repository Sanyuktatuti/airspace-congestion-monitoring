#!/bin/bash
# setup_mongodb.sh
# Script to install and configure MongoDB for the Airspace Congestion Monitoring system

set -e  # Exit immediately if a command exits with a non-zero status

echo "======================================================"
echo "Setting up MongoDB for Airspace Congestion Monitoring"
echo "======================================================"

# Detect OS
OS="$(uname)"
if [[ "$OS" == "Darwin" ]]; then
    echo "Detected macOS"
    
    # Check if Homebrew is installed
    if ! command -v brew &> /dev/null; then
        echo "Homebrew not found. Installing Homebrew..."
        /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    fi
    
    # Install MongoDB using Homebrew
    echo "Installing MongoDB Community Edition..."
    brew tap mongodb/brew
    brew install mongodb-community
    
    # Start MongoDB service
    echo "Starting MongoDB service..."
    brew services start mongodb-community
    
elif [[ "$OS" == "Linux" ]]; then
    echo "Detected Linux"
    
    # Check distribution
    if [ -f /etc/lsb-release ]; then
        # Ubuntu
        echo "Installing MongoDB on Ubuntu..."
        
        # Import MongoDB public GPG key
        curl -fsSL https://www.mongodb.org/static/pgp/server-6.0.asc | sudo apt-key add -
        
        # Create list file for MongoDB
        echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu $(lsb_release -cs)/mongodb-org/6.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-6.0.list
        
        # Update package list
        sudo apt-get update
        
        # Install MongoDB
        sudo apt-get install -y mongodb-org
        
        # Start MongoDB service
        sudo systemctl start mongod
        sudo systemctl enable mongod
        
    elif [ -f /etc/redhat-release ]; then
        # RHEL/CentOS/Fedora
        echo "Installing MongoDB on RHEL/CentOS/Fedora..."
        
        # Create repo file
        cat << EOF | sudo tee /etc/yum.repos.d/mongodb-org-6.0.repo
[mongodb-org-6.0]
name=MongoDB Repository
baseurl=https://repo.mongodb.org/yum/redhat/\$releasever/mongodb-org/6.0/x86_64/
gpgcheck=1
enabled=1
gpgkey=https://www.mongodb.org/static/pgp/server-6.0.asc
EOF
        
        # Install MongoDB
        sudo yum install -y mongodb-org
        
        # Start MongoDB service
        sudo systemctl start mongod
        sudo systemctl enable mongod
    else
        echo "Unsupported Linux distribution. Please install MongoDB manually."
        exit 1
    fi
else
    echo "Unsupported operating system: $OS. Please install MongoDB manually."
    exit 1
fi

# Install Python MongoDB driver
echo "Installing Python MongoDB driver..."
pip install pymongo dnspython

# Create data directories
echo "Creating data directories..."
mkdir -p data/mongodb

# Create .env file with MongoDB settings if it doesn't exist
if [ ! -f .env ]; then
    echo "Creating .env file with MongoDB settings..."
    cat << EOF >> .env
# MongoDB settings
MONGO_URI=mongodb://localhost:27017/
MONGO_DB=airspace_monitoring
MONGO_COLLECTION=historical_flights
EOF
else
    # Check if MongoDB settings are in .env, add if not
    if ! grep -q "MONGO_URI" .env; then
        echo "Adding MongoDB settings to .env file..."
        cat << EOF >> .env

# MongoDB settings
MONGO_URI=mongodb://localhost:27017/
MONGO_DB=airspace_monitoring
MONGO_COLLECTION=historical_flights
EOF
    fi
fi

echo "======================================================"
echo "MongoDB setup complete!"
echo ""
echo "To generate and import historical data, run:"
echo "  python data/augment_historical_data.py"
echo "  python data/import_to_mongodb.py --drop"
echo ""
echo "To start the dashboard with MongoDB support:"
echo "  cd dashboard && python opensky_dashboard.py"
echo "======================================================" 