#!/bin/bash
set -euo pipefail

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}🐍 Python gRPC Client Setup${NC}"

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}❌ Python3 not found. Please install Python 3.7+${NC}"
    exit 1
fi

# Install requirements
echo -e "${BLUE}📦 Installing Python requirements...${NC}"
pip3 install -r requirements.txt

# Generate protobuf files
echo -e "${BLUE}🔧 Generating protobuf files...${NC}"
python3 generate_protos.py

# Check if gRPC server is running
echo -e "${BLUE}🔍 Checking if gRPC server is running...${NC}"
if ! curl -s http://localhost:8083/health > /dev/null 2>&1; then
    echo -e "${RED}❌ gRPC server not running. Please start it first:${NC}"
    echo "   ./RDS.sh start"
    echo "   ./RDS.sh run-microservice"
    exit 1
fi

echo -e "${GREEN}✅ gRPC server is running${NC}"

# Run the Python client
echo -e "${BLUE}🚀 Starting Python gRPC client...${NC}"
python3 python_client.py
