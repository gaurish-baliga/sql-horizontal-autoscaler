#!/bin/bash

# SQL Horizontal Autoscaler - Initial Setup
# Sets up the initial shard and Docker network

set -e

echo "🚀 SQL Horizontal Autoscaler - Setup"
echo "===================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Read configuration from config.json
if [ ! -f "config.json" ]; then
    echo -e "${RED}❌ config.json not found${NC}"
    exit 1
fi

# Extract configuration values using jq or python
if command -v jq > /dev/null 2>&1; then
    NETWORK_NAME=$(jq -r '.docker.network_name' config.json)
    BASE_PORT=$(jq -r '.ports.base_port' config.json)
    DB_USERNAME=$(jq -r '.database.username' config.json)
    DB_PASSWORD=$(jq -r '.database.password' config.json)
    DB_ROOT_PASSWORD=$(jq -r '.database.root_password' config.json)
    DOCKER_IMAGE=$(jq -r '.docker.image' config.json)
    CONTAINER_PREFIX=$(jq -r '.docker.container_prefix' config.json)
    MAX_ATTEMPTS=$(jq -r '.limits.max_connection_attempts' config.json)
    RETRY_INTERVAL=$(jq -r '.limits.connection_retry_interval_seconds' config.json)
else
    # Fallback to python if jq is not available
    NETWORK_NAME=$(python3 -c "import json; print(json.load(open('config.json'))['docker']['network_name'])" 2>/dev/null || echo "autoscaler-network")
    BASE_PORT=$(python3 -c "import json; print(json.load(open('config.json'))['ports']['base_port'])" 2>/dev/null || echo "3306")
    DB_USERNAME=$(python3 -c "import json; print(json.load(open('config.json'))['database']['username'])" 2>/dev/null || echo "testuser")
    DB_PASSWORD=$(python3 -c "import json; print(json.load(open('config.json'))['database']['password'])" 2>/dev/null || echo "testpass")
    DB_ROOT_PASSWORD=$(python3 -c "import json; print(json.load(open('config.json'))['database']['root_password'])" 2>/dev/null || echo "rootpass")
    DOCKER_IMAGE=$(python3 -c "import json; print(json.load(open('config.json'))['docker']['image'])" 2>/dev/null || echo "mysql:8.0")
    CONTAINER_PREFIX=$(python3 -c "import json; print(json.load(open('config.json'))['docker']['container_prefix'])" 2>/dev/null || echo "mysql")
    MAX_ATTEMPTS=$(python3 -c "import json; print(json.load(open('config.json'))['limits']['max_connection_attempts'])" 2>/dev/null || echo "30")
    RETRY_INTERVAL=$(python3 -c "import json; print(json.load(open('config.json'))['limits']['connection_retry_interval_seconds'])" 2>/dev/null || echo "2")
fi

# Function to check if Docker is running
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        echo -e "${RED}❌ Docker is not running. Please start Docker first.${NC}"
        exit 1
    fi
    echo -e "${GREEN}✅ Docker is running${NC}"
}

# Function to cleanup existing containers
cleanup_existing() {
    echo -e "\n${YELLOW}🧹 Cleaning up existing containers...${NC}"
    
    # Stop and remove any existing shard containers
    docker stop $(docker ps -q --filter "name=mysql-shard") 2>/dev/null || true
    docker rm $(docker ps -aq --filter "name=mysql-shard") 2>/dev/null || true
    
    docker network rm $NETWORK_NAME 2>/dev/null || true
    echo -e "${GREEN}✅ Cleanup complete${NC}"
}

# Function to create Docker network
create_network() {
    echo -e "\n${BLUE}🌐 Creating Docker network...${NC}"
    docker network create $NETWORK_NAME 2>/dev/null || true
    echo -e "${GREEN}✅ Network '$NETWORK_NAME' ready${NC}"
}

# Function to start initial shard
start_initial_shard() {
    echo -e "\n${BLUE}🗄️  Starting initial MySQL shard on port $BASE_PORT...${NC}"

    docker run -d \
        --name ${CONTAINER_PREFIX}-shard-1 \
        --network $NETWORK_NAME \
        -p ${BASE_PORT}:3306 \
        -e MYSQL_ROOT_PASSWORD=$DB_ROOT_PASSWORD \
        -e MYSQL_DATABASE=shard1_db \
        -e MYSQL_USER=$DB_USERNAME \
        -e MYSQL_PASSWORD=$DB_PASSWORD \
        $DOCKER_IMAGE > /dev/null

    echo -e "${GREEN}✅ Initial shard started on port $BASE_PORT${NC}"
}

# Function to wait for MySQL to be ready
wait_for_mysql() {
    local max_attempts=$MAX_ATTEMPTS
    local attempt=1

    echo -e "${YELLOW}⏳ Waiting for MySQL to be ready...${NC}"

    while [ $attempt -le $max_attempts ]; do
        if docker exec ${CONTAINER_PREFIX}-shard-1 mysqladmin ping -h localhost -u $DB_USERNAME -p$DB_PASSWORD > /dev/null 2>&1; then
            echo -e "${GREEN}✅ MySQL is ready!${NC}"
            return 0
        fi
        echo "   Attempt $attempt/$max_attempts - waiting..."
        sleep $RETRY_INTERVAL
        attempt=$((attempt + 1))
    done

    echo -e "${RED}❌ MySQL failed to start within expected time${NC}"
    return 1
}

# Function to setup initial data
setup_initial_data() {
    echo -e "${BLUE}📊 Setting up initial database schema and data...${NC}"
    
    # Create tables
    docker exec -i ${CONTAINER_PREFIX}-shard-1 mysql -u $DB_USERNAME -p$DB_PASSWORD shard1_db << 'EOF'
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    shard_info VARCHAR(50) DEFAULT 'shard-1'
);

CREATE TABLE IF NOT EXISTS orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    product_name VARCHAR(100),
    amount DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    shard_info VARCHAR(50) DEFAULT 'shard-1'
);

CREATE TABLE IF NOT EXISTS products (
    product_id INT PRIMARY KEY,
    name VARCHAR(100),
    price DECIMAL(10,2),
    category VARCHAR(50),
    shard_info VARCHAR(50) DEFAULT 'shard-1'
);
EOF

    # Insert initial data (enough to approach but not exceed threshold)
    for i in {1..20}; do
        user_id=$((1000 + i))
        docker exec ${CONTAINER_PREFIX}-shard-1 mysql -u $DB_USERNAME -p$DB_PASSWORD shard1_db \
            -e "INSERT IGNORE INTO users (user_id, name, email) VALUES ($user_id, 'User $user_id', 'user$user_id@shard1.com');" 2>/dev/null
    done
    
    for i in {1..30}; do
        order_id=$((1000 + i))
        customer_id=$((1000 + (i % 20) + 1))
        docker exec ${CONTAINER_PREFIX}-shard-1 mysql -u $DB_USERNAME -p$DB_PASSWORD shard1_db \
            -e "INSERT IGNORE INTO orders (order_id, customer_id, product_name, amount) VALUES ($order_id, $customer_id, 'Product $i', $((i * 10 + 50)));" 2>/dev/null
    done
    
    for i in {1..15}; do
        product_id=$((1000 + i))
        docker exec ${CONTAINER_PREFIX}-shard-1 mysql -u $DB_USERNAME -p$DB_PASSWORD shard1_db \
            -e "INSERT IGNORE INTO products (product_id, name, price, category) VALUES ($product_id, 'Product $product_id', $((i * 25 + 100)), 'Category $((i % 3 + 1))');" 2>/dev/null
    done
    
    echo -e "${GREEN}✅ Initial data setup complete${NC}"
}

# Main execution
main() {
    echo -e "\n${YELLOW}Starting setup process...${NC}"
    
    check_docker
    cleanup_existing
    create_network
    start_initial_shard
    wait_for_mysql
    setup_initial_data
    
    echo -e "\n${GREEN}🎉 Setup Complete!${NC}"
    echo -e "\n${BLUE}📋 Summary:${NC}"
    echo "   • Initial shard: ${CONTAINER_PREFIX}-shard-1 (port $BASE_PORT)"
    echo "   • Database: shard1_db"
    echo "   • Network: $NETWORK_NAME"
    echo "   • Initial data: ~65 entries (users: 20, orders: 30, products: 15)"
    
    echo -e "\n${YELLOW}🚀 Next Steps:${NC}"
    echo "   1. Build the application: go build -o sql-autoscaler ."
    echo "   2. Start the autoscaler: ./sql-autoscaler"
    echo "   3. Test dynamic scaling: ./test.sh"

    echo -e "\n${GREEN}✅ Ready for dynamic scaling tests!${NC}"
}

# Run main function
main "$@"
